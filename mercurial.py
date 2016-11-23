import sys
import os

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

    def close(self):
        self.server.close()

    @property
    def summary(self):
        return self._summary

    @summary.setter
    def summary(self, value):
        self._summary = value


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


class HgCommand(object):

    def _cbout(self, data):
        self.panel(str(data, self.encoding))

    def _cbprompt(self, p):
        res = sublime.yes_no_cancel_dialog(str(p, self.encoding), 'y', 'n')
        if res == sublime.DIALOG_YES:
            return b'y'
        if res == sublime.DIALOG_NO:
            return b'n'
        return b''

    def run_hg_function(self, srv, func, log_output=True, *args, **kwargs):
        err = None
        self.srv = srv.server
        self.srv.setcbout(self._cbout if log_output else None)
        self.srv.setcberr(self._cbout if log_output else None)
        self.srv.setcbprompt(lambda size, data: self._cbprompt(data) + b'\n')
        if log_output:
            self.panel('', clear=True)
        try:
            return getattr(self.srv, func)(*args, **kwargs), None
        except hglib.error.CommandError as ex:
            err = '\n'.join(filter(bool, [
                str(ex.out.rstrip(), self.encoding),
                str(ex.err.rstrip(), self.encoding)
            ]))
        except Exception as e:
            err = str(e)
        return None, err

    def _output_to_view(self, view, output, clear=False, syntax=None, **kwargs):
        if syntax:
            view.set_syntax_file(syntax)
        args = {
            'output': output,
            'clear': clear
        }
        view.run_command('hg_scratch_output', args)

    def panel(self, output, clear=False, **kwargs):
        if not hasattr(self, 'output_view'):
            self.output_view = self.get_window().get_output_panel('hg')
        self.output_view.set_read_only(False)
        self._output_to_view(self.output_view, output, clear=clear, **kwargs)
        self.output_view.set_read_only(True)
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
        if not fn:
            return None
        d = hg_root(os.path.realpath(os.path.dirname(fn)))

        if d:
            result = _get_server(d)
            if result:
                self.encoding = result.server.encoding.decode()
            return result
        return None


class HgWindowCommand(HgCommand, sublime_plugin.WindowCommand):

    def get_window(self):
        return self.window

    def get_view(self):
        return self.window.active_view()

    def get_server(self):
        d = self.window.extract_variables().get('folder')
        if d and is_hg_root(d):
            result = _get_server(d)
            if result:
                self.encoding = result.server.encoding.decode()
            return result
        return None


class HgBranchStatusCommand(HgTextCommand):

    def run(self, edit, force=False):
        srv = self.get_server()
        if not srv:
            self.view.erase_status('Hgstate')
            return
        if not srv.summary or force:
            summary, err = self.run_hg_function(srv, 'summary', log_output=False)
            if not summary:
                if err:
                    self.panel(err)
                self.view.erase_status('Hgstate')
                return
            srv.summary = summary
        summary = srv.summary
        s = str(summary[b'branch'], self.encoding)
        if not summary[b'commit']:
            s += ' ‼'
        if summary[b'update']:
            s += ' ^'
        self.view.set_status('Hgstate', str(s))


class HgBranchStatusListener(sublime_plugin.EventListener):

    def on_activated_async(self, view):
        view.run_command('hg_branch_status')

    def on_post_save_async(self, view):
        view.run_command('hg_branch_status', {'force': True})


class HgIncomingCommand(HgWindowCommand):

    hg_command = 'incoming'

    def run(self):
        srv = self.get_server()
        if not srv:
            return
        res, err = self.run_hg_function(srv, self.hg_command, log_output=False)
        output = []
        for r in res:
            r = list(map(lambda x: str(x, self.encoding) if type(x) == bytes else x, r))
            output.append('{}\t{}:{}\t{}'.format(r[3], r[0], r[1][:12], r[-1]))
            output.append(r[4])
            output.append(r[5])
            output.append('')
        if output:
            self.scratch('\n'.join(output), title='Hg: Incoming')
        else:
            if err:
                self.panel(err)
            else:
                self.panel('no ' + self.hg_command)


class HgOutgoingCommand(HgIncomingCommand):

    hg_command = 'outgoing'


class HgPullCommand(HgWindowCommand):

    def run(self, update=False, rebase=False):
        srv = self.get_server()
        if not srv:
            return
        if rebase:
            update = False
        if update:
            tool = None
        else:
            tool = 'internal:merge'
        self.run_hg_function(srv, 'pull', update=update, rebase=rebase, tool=tool)
        srv.summary = None


class HgPushCommand(HgWindowCommand):

    def run(self, newbranch=False):
        srv = self.get_server()
        if not srv:
            return
        self.run_hg_function(srv, 'push', newbranch=newbranch)


class HgUpdateCommand(HgWindowCommand):

    def run(self):
        srv = self.get_server()
        if not srv:
            return
        self.run_hg_function(srv, 'update')
        srv.summary = None


class HgMergeCommand(HgWindowCommand):

    def run(self):
        srv = self.get_server()
        if not srv:
            return
        self.run_hg_function(srv, 'merge', cb=self._cbprompt)
        srv.summary = None


class HgStatusCommand(HgWindowCommand):

    def run(self):
        srv = self.get_server()
        if not srv:
            return
        res, err = self.run_hg_function(srv, 'status', log_output=False)
        if res:
            output = []
            for r in res:
                r = list(map(lambda x: str(x, self.encoding) if type(x) == bytes else x, r))
                output.append('{}\t{}'.format(r[0], r[1]))
            if output:
                self.scratch('\n'.join(output), title='Hg: Status')
        else:
            if err:
                self.panel(err)
            else:
                self.panel('no')


class HgDiffCommand(HgWindowCommand):

    def run(self):
        srv = self.get_server()
        if not srv:
            return
        res, err = self.run_hg_function(srv, 'diff', log_output=False)
        if res:
            self.scratch(str(res, self.encoding), title='Hg: Diff', syntax='Packages/Diff/Diff.tmLanguage')
        else:
            if err:
                self.panel(err)
            else:
                self.panel('no')


class HgAddremoveCommand(HgWindowCommand):

    def run(self):
        srv = self.get_server()
        if not srv:
            return
        self.run_hg_function(srv, 'addremove')
        srv.summary = None


commit_history = []


class HgCommitCommand(HgWindowCommand):

    def run(self, close_branch=False):
        srv = self.get_server()
        if not srv:
            return
        res, err = self.run_hg_function(srv, 'status', log_output=False)
        if err:
            self.panel(err)
            return
        global commit_history
        output = ['closed' if close_branch else '']
        output.extend([
            '# ----------',
            '# Enter the commit message. Everything below this paragraph is ignored.',
            '# Empty message aborts the commit.',
            '# Close this window to accept your message.'
        ])
        for r in res:
            r = list(map(lambda x: str(x, self.encoding) if type(x) == bytes else x, r))
            output.append('#\t{}\t{}'.format(r[0], r[1]))
        if commit_history:
            output.append('# commit messages history:')
            for c in commit_history:
                output.append('# {}'.format(c))
        v = self.scratch('\n'.join(output), title='Hg: Commit close branch' if close_branch else 'Hg: Commit')
        v.set_read_only(False)
        HgCommitCommand.active_message = self

    def on_message_done(self, message, close_branch):
        srv = self.get_server()
        if not srv:
            return
        message = message.split('\n# ----------')[0].strip()
        if not message:
            self.panel('No commit message')
            return
        global commit_history
        commit_history.insert(0, message)
        commit_history = commit_history[:20]
        self.run_hg_function(srv, 'commit', message=message.encode(self.encoding), closebranch=close_branch)
        srv.summary = None


class HgCommitMessageListener(sublime_plugin.EventListener):

    def on_close(self, view):
        if view.name() not in ['Hg: Commit close branch', 'Hg: Commit']:
            return
        command = HgCommitCommand.active_message
        if not command:
            return
        close_branch = view.name() == 'Hg: Commit close branch'
        region = sublime.Region(0, view.size())
        message = view.substr(region)
        command.on_message_done(message, close_branch)


class HgResolveAllCommand(HgWindowCommand):

    def run(self):
        srv = self.get_server()
        if not srv:
            return
        self.run_hg_function(srv, 'resolve', all=True)
        srv.summary = None
