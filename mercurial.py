import sys
import os
from threading import Thread, Lock
from functools import partial

import sublime
import sublime_plugin

sys.path.append(os.path.dirname(__file__))

import hglib


servers = {}


class HgServer(object):

    def __init__(self, folder):
        super(HgServer, self).__init__()
        self.server = hglib.open(folder)
        self._summary = None
        self.commit_history = []

    def close(self):
        self.server.close()

    @property
    def summary(self):
        return self._summary

    @summary.setter
    def summary(self, value):
        self._summary = value

    def add_commit_message(self, message):
        if message in self.commit_history:
            self.commit_history.remove(message)
        self.commit_history.append(message)
        while len(self.commit_history) > 20:
            self.commit_history.pop(0)


def stop_all_servers():
    global servers
    for v in servers.values():
        if v is not None:
            v.close()


def _get_server(folder):
    global servers
    if folder not in servers:
        servers[folder] = HgServer(folder)
    return servers[folder]
    if folder in servers:
        return servers[folder]


def is_hg_root(d):
    return os.path.exists(os.path.join(d, '.hg'))


def hg_root(d):
    while d:
        if is_hg_root(d):
            return d
        p = os.path.realpath(os.path.join(d, os.path.pardir))
        if p == d:
            return None
        d = p
    return None


def plugin_loaded():
    pass


def plugin_unloaded():
    stop_all_servers()


def main_thread(callback, *args, **kwargs):
    sublime.set_timeout(partial(callback, *args, **kwargs), 0)


class HgCommandThread(Thread):
    command_lock = Lock()
    prompt_lock = Lock()

    def __init__(self, srv, func, on_done, on_output, on_prompt, on_command, on_ret, *args, **kwargs):
        super(HgCommandThread, self).__init__()
        self.srv = srv
        self.func = func
        self.on_done = on_done
        self.on_output = on_output
        self.on_prompt = on_prompt
        self.on_command = on_command
        self.on_ret = on_ret
        self.args = args
        self.kwargs = kwargs

    def _prompt(self, p):
        if not self.on_prompt:
            return b''
        self.answer = b''
        self.prompt_lock.acquire()
        main_thread(self.on_prompt, p)
        self.prompt_lock.acquire()
        try:
            return self.answer
        finally:
            self.prompt_lock.release()

    def provide_answer(self, answer):
        self.answer = answer
        self.prompt_lock.release()

    def _done(self, output=None, err=None):
        if self.on_done:
            main_thread(self.on_done, output, err)

    def _output(self, output):
        if self.on_output:
            main_thread(self.on_output, output)

    def _error(self, output):
        self._output(output)
        self._cbret(2)

    def _cbret(self, ret):
        if ret and self.on_ret:
            main_thread(self.on_ret, ret)

    def run(self):
        if not self.srv:
            self._done()
            return
        output = None
        err = None
        self.command_lock.acquire()
        try:
            if self.on_command:
                main_thread(self.on_command, self.func)
            srv = self.srv.server
            srv.setcbout(self._output)
            srv.setcberr(self._error)
            srv.setcbret(self._cbret)
            srv.setcbprompt(lambda size, x: self._prompt(x) + b'\n')
            try:
                output = getattr(srv, self.func)(*self.args, **self.kwargs)
            except hglib.error.CommandError as ex:
                encoding = srv.encoding.decode()
                err = '\n'.join(filter(bool, [
                    str(ex.out.rstrip(), encoding),
                    str(ex.err.rstrip(), encoding)
                ]))
            except Exception as e:
                err = str(e)
        finally:
            self.command_lock.release()
        self._done(output, err)


class HgCommand(object):

    def _cbout(self, data):
        o = str(data, self.encoding)
        self.panel(o)
        v = self.get_view()
        if v:
            v.set_status('HgCommandOutput', o.strip())

    def _return_code(self, ret):
        self.return_code = ret

    def _on_input_done(self, answer):
        self.panel('{}\n'.format(answer))
        self.active_hg_command.provide_answer(answer.encode())

    def _on_input_cancel(self):
        self.panel('\n')
        self.active_hg_command.provide_answer(b'')

    def _cbprompt(self, p):
        prompt = ' '.join(str(p, self.encoding).split('\n')[-2:])
        self.get_window().show_input_panel(prompt, '', self._on_input_done, None, self._on_input_cancel)

    def _done(self, output, err):
        pass

    def _command_done(self, output, err):
        v = self.get_view()
        if v:
            v.erase_status('HgCommand')
            v.erase_status('HgCommandOutput')
        if err or self.return_code:
            self.show_panel()
        self.on_done(output, err)

    def _command(self, func):
        if self.log_output:
            self.panel('', clear=True)
        v = self.get_view()
        if v:
            v.set_status('HgCommand', 'Hg: ' + func)

    def run_hg_function(self, func, on_done=None, log_output=True, on_ret=None, *args, **kwargs):
        self.srv = self.get_server()
        if not self.srv:
            self._done(None, None)
            return
        self.encoding = self.srv.server.encoding.decode()
        self.log_output = log_output
        self.on_done = on_done or self._done
        self.return_code = 0
        self.active_hg_command = HgCommandThread(
            self.srv,
            func,
            self._command_done,
            self._cbout if log_output else None,
            self._cbprompt,
            self._command,
            on_ret or self._return_code,
            *args,
            **kwargs
        )
        self.active_hg_command.start()

    def _output_to_view(self, view, output, clear=False, syntax=None, **kwargs):
        if syntax:
            view.set_syntax_file(syntax)
        else:
            view.set_syntax_file('Packages/MercurialCommands/syntax/Hg output.sublime-syntax')
        args = {
            'output': output,
            'clear': clear
        }
        view.run_command('hg_scratch_output', args)

    def panel(self, output, clear=False, **kwargs):
        if not hasattr(self, 'output_view') or not self.output_view:
            self.output_view = self.get_window().get_output_panel('hg')
        self.output_view.set_read_only(False)
        self._output_to_view(self.output_view, output, clear=clear, **kwargs)
        self.output_view.set_read_only(True)

    def show_panel(self):
        self.get_window().run_command('show_panel', {'panel': 'output.hg'})

    def scratch(self, output, title=None, **kwargs):
        v = None
        if title:
            views = self.get_window().views()
            views = list(filter(lambda x: x.name() == title, views))
            if views:
                v = views[0]
        if not v:
            v = self.get_window().new_file()
            if title:
                v.set_name(title)
            v.set_scratch(True)
        v.set_read_only(False)
        self._output_to_view(v, output, clear=True, **kwargs)
        v.set_read_only(True)
        v.run_command('goto_line', {'line': 1})
        self.get_window().focus_view(v)
        return v

    def reset_summary(self):
        self.srv.summary = None
        v = self.get_view()
        if v:
            v.run_command('hg_branch_status')


class HgScratchOutputCommand(sublime_plugin.TextCommand):

    def run(self, edit, output='', clear=False):
        if clear:
            region = sublime.Region(0, self.view.size())
            self.view.erase(edit, region)
        self.view.insert(edit, self.view.size(), output)


class HgTextCommand(HgCommand, sublime_plugin.TextCommand):

    def get_window(self):
        return self.view.window() or sublime.active_window()

    def get_view(self):
        return self.view

    def get_server(self):
        if self.view.settings().get('is_widget'):
            return None
        fn = self.view.file_name()
        if fn:
            d = hg_root(os.path.realpath(os.path.dirname(fn)))
        else:
            d = self.get_window().extract_variables().get('folder')
            if not d or not is_hg_root(d):
                return None
        if d:
            return _get_server(d)
        return None


class HgWindowCommand(HgCommand, sublime_plugin.WindowCommand):

    def get_window(self):
        return self.window

    def get_view(self):
        return self.window.active_view()

    def get_server(self):
        d = self.window.extract_variables().get('folder')
        if d and is_hg_root(d):
            return _get_server(d)
        return None


class HgBranchStatusCommand(HgTextCommand):

    def _done(self, output, err):
        if not output:
            if err:
                self.panel(err)
                self.show_panel()
            self.view.erase_status('HgState')
            return
        self.srv.summary = {
            'branch': str(output[b'branch'], self.encoding),
            'commit': output[b'commit'],
            'update': output[b'update']
        }
        self._set_status(self.srv)

    def _set_status(self, srv):
        s = srv.summary['branch']
        if not srv.summary['commit']:
            s += ' ‼'
        if srv.summary['update']:
            s += ' ^'
        self.view.set_status('HgState', str(s))

    def run(self, edit, force=False):
        srv = self.get_server()
        if not srv:
            self.view.erase_status('HgState')
            return
        if force or not srv.summary:
            self.run_hg_function('summary', log_output=False)
        else:
            self._set_status(srv)


class HgBranchStatusListener(sublime_plugin.EventListener):

    def on_activated_async(self, view):
        view.run_command('hg_branch_status')

    def on_post_save_async(self, view):
        view.run_command('hg_branch_status', {'force': True})


class HgIncomingCommand(HgWindowCommand):

    hg_command = 'incoming'
    output_view_title = 'Hg: Incoming'

    def _done(self, data, err):
        if data:
            output = []
            for r in data:
                r = list(map(lambda x: str(x, self.encoding) if type(x) == bytes else x, r))
                output.append('{}\t{}:{}\t{}'.format(r[3], r[0], r[1][:12], r[-1]))
                output.append(r[4])
                output.append(r[5])
                output.append('')
            self.scratch('\n'.join(output), title=self.output_view_title)
            self.window.run_command('hide_panel', {'panel': 'output.hg'})

    def run(self):
        self.run_hg_function(self.hg_command)


class HgOutgoingCommand(HgIncomingCommand):

    hg_command = 'outgoing'
    output_view_title = 'Hg: Outgoing'


class HgPullCommand(HgWindowCommand):

    def _done(self, data, err):
        self.reset_summary()

    def run(self, update=False, rebase=False):
        if rebase:
            update = False
        if update:
            tool = None
        else:
            tool = 'internal:merge'
        self.run_hg_function('pull', update=update, rebase=rebase, tool=tool)


class HgPushCommand(HgWindowCommand):

    def run(self, newbranch=False):
        self.run_hg_function('push', newbranch=newbranch)


class HgUpdateCommand(HgWindowCommand):

    def _done(self, data, err):
        self.reset_summary()

    def run(self, clean=False, rev=None):
        if clean:
            if not sublime.ok_cancel_dialog('Discard uncommited changes?'):
                return
        self.run_hg_function('update', clean=clean, rev=rev)


class HgUpdateBranchCommand(HgWindowCommand):

    def _done(self, data, err):
        if data:
            self.branches = list(map(lambda x: str(x[0], self.encoding), data))
            try:
                idx = self.branches.index(self.current_branch)
            except ValueError:
                idx = -1
            self.get_window().show_quick_panel(
                self.branches,
                self.select_done,
                sublime.KEEP_OPEN_ON_FOCUS_LOST,
                idx,
                None
            )
        else:
            self.panel(err if err else 'No branches')
            self.show_panel()

    def select_done(self, idx):
        if idx > -1:
            self.get_window().run_command('hg_update', {'rev': self.branches[idx]})

    def on_branch_done(self, data, err):
        if data:
            self.current_branch = str(data, self.encoding)
        else:
            self.panel(err if err else 'No current branch')
            self.show_panel()
            self.current_branch = None
        self.run_hg_function('branches', log_output=False, closed=self.closed)

    def run(self, closed=False):
        srv = self.get_server()
        if not srv:
            return
        self.closed = closed
        if not srv.summary:
            self.run_hg_function('branch', log_output=False, on_done=self.on_branch_done)
        else:
            self.current_branch = srv.summary['branch']
            self.run_hg_function('branches', log_output=False, closed=closed)


class HgMergeCommand(HgWindowCommand):

    def _done(self, data, err):
        self.reset_summary()
        if self.return_code:
            self.panel('RETURN CODE: %s' % self.return_code)
            return
        message = 'merged'
        if self.rev:
            message += ' ' + self.rev
        self.get_window().run_command('hg_commit', {'message': message})

    def run(self, rev=None):
        self.rev = rev
        self.run_hg_function('merge', rev=rev)


class HgMergeBranchCommand(HgWindowCommand):

    def _done(self, data, err):
        if data:
            self.branches = list(map(lambda x: str(x[0], self.encoding), data))
            self.get_window().show_quick_panel(
                self.branches,
                self.select_done,
                sublime.KEEP_OPEN_ON_FOCUS_LOST,
                -1,
                None
            )
        else:
            self.panel(err if err else 'No branches')
            self.show_panel()

    def select_done(self, idx):
        if idx > -1:
            self.get_window().run_command('hg_merge', {'rev': self.branches[idx]})

    def run(self):
        self.run_hg_function('branches', log_output=False)


class HgStatusCommand(HgWindowCommand):

    def _done(self, data, err):
        if data:
            output = []
            for r in data:
                r = list(map(lambda x: str(x, self.encoding) if type(x) == bytes else x, r))
                output.append('{}\t{}'.format(r[0], r[1]))
            self.scratch('\n'.join(output), title='Hg: Status')
        else:
            self.panel(err if err else 'No changes')

    def run(self):
        self.run_hg_function('status', log_output=False)


class HgDiffCommand(HgWindowCommand):

    def _done(self, data, err):
        if data:
            self.scratch(str(data, self.encoding), title='Hg: Diff', syntax='Packages/Diff/Diff.tmLanguage')
        else:
            self.panel(err if err else 'No changes')
            self.show_panel()

    def run(self):
        self.run_hg_function('diff', log_output=False)


class HgAddremoveCommand(HgWindowCommand):

    def _done(self, data, err):
        self.reset_summary()

    def run(self):
        self.run_hg_function('addremove')


class HgBranchCommand(HgWindowCommand):

    def _on_branch_done(self, output, err):
        if output:
            self.ask_branch_name(str(output, self.encoding))
        else:
            self.panel(err if err else 'No current branch')

    def _on_input_cancel(self):
        self.reset_summary()

    def _on_input_done(self, answer):
        self.run_hg_function('branch', name=answer.encode())

    def _done(self, output, err):
        self.reset_summary()

    def ask_branch_name(self, current_branch):
        self.get_window().show_input_panel(
            'Branch name',
            current_branch,
            self._on_input_done,
            None,
            self._on_input_cancel)

    def run(self):
        srv = self.get_server()
        if not srv:
            return
        if not srv.summary:
            self.run_hg_function('branch', log_output=False, on_done=self._on_branch_done)
        else:
            self.ask_branch_name(srv.summary['branch'])


class HgBranchCleanCommand(HgWindowCommand):

    def _done(self, data, err):
        self.reset_summary()

    def run(self):
        self.run_hg_function('branch', clean=True)


class HgCommitCommand(HgWindowCommand):

    def _done(self, data, err):
        self.reset_summary()

    def _on_status_done(self, data, err):
        if not data and not self.close_branch:
            self.panel(err if err else 'No changes')
            self.show_panel()
            return

        output = [self.message or '']
        output.extend([
            '# ----------',
            '# Enter the commit message. Everything below this paragraph is ignored.',
            '# Empty message aborts the commit.',
            '# Close this window to accept your message.'
        ])
        if data:
            output.append('')
            output.append('Files to commit:')
            for r in data:
                r = list(map(lambda x: str(x, self.encoding) if type(x) == bytes else x, r))
                output.append('\t{}\t{}'.format(r[0], r[1]))

        commit_history = self.srv.commit_history
        if commit_history:
            output.append('')
            output.append('Commit messages history:')
            output.append('')
            for c in commit_history:
                output.append('{}'.format(c))
        v = self.scratch(
            '\n'.join(output),
            title='Hg: Commit close branch' if self.close_branch else 'Hg: Commit',
            syntax='Packages/MercurialCommands/syntax/Hg Commit Message.sublime-syntax'
        )
        v.set_read_only(False)
        HgCommitCommand.active_message = self

    def run(self, message=None, close_branch=False):
        self.close_branch = close_branch
        self.message = message
        self.run_hg_function('status', log_output=False, on_done=self._on_status_done)

    def on_message_done(self, message):
        message = message.split('\n# ----------')[0].strip()
        if not message:
            self.panel('No commit message', clear=True)
            self.show_panel()
            return
        self.srv.add_commit_message(message)
        self.run_hg_function('commit', message=message.encode(self.encoding), closebranch=self.close_branch)


class HgCommitMessageListener(sublime_plugin.EventListener):

    def on_close(self, view):
        if view.name() not in ['Hg: Commit close branch', 'Hg: Commit']:
            return
        command = HgCommitCommand.active_message
        if not command:
            return
        region = sublime.Region(0, view.size())
        message = view.substr(region)
        command.on_message_done(message)


class HgResolveAllCommand(HgWindowCommand):

    def _done(self, data, err):
        self.reset_summary()

    def run(self):
        self.run_hg_function('resolve', all=True)


class HgRebaseCommand(HgWindowCommand):

    def run(self, continue_rebase=None, abort_rebase=None):
        self.run_hg_function('rebase', continue_rebase=continue_rebase, abort_rebase=abort_rebase)
