#!/usr/bin/env python3

from typing import Tuple, Optional, List, Dict, Any
import argparse
import binascii
import datetime
import time
import io
import collections
import sys
import logging
import traceback
import pdb
import json
import os
import tempfile
import webbrowser

import tornado.ioloop
import tornado.web
import tornado.httpserver
import tornado.netutil
import tornado.websocket

from beancount.core.data import Transaction, Posting
from beancount.core.number import MISSING, Decimal
import beancount.parser.printer

from . import reconcile

from . import training
from . import matching
from .source import Source, InvalidSourceReference


def json_encode_beancount_entry(x):
    if x is None:
        return None
    if isinstance(x, Transaction):
        x = x._replace(postings=[y._asdict() for y in x.postings])
    result = x._asdict()
    if result['meta'] is not None:
        result['meta'] = result['meta'].copy()
        result['meta'].pop('__tolerances__', None)
    return result


def format_transaction(transaction: Transaction) -> str:
    printer = beancount.parser.printer.EntryPrinter()
    return printer(transaction)


def format_posting(posting: Posting, indent: str = '  ') -> str:
    printer = beancount.parser.printer.EntryPrinter()
    flag_account, position_str, weight_str = printer.render_posting_strings(
        posting)
    oss = io.StringIO()
    oss.write(('%s%s  %s' % (indent, flag_account, position_str)).rstrip() +
              '\n')
    if posting.meta:
        printer.write_metadata(posting.meta, oss, '  ' + indent)
    return oss.getvalue()


def convert_uncleared(p: Tuple[Transaction, Posting]):
    return {
        'transaction': json_encode_beancount_entry(p[0]),
        'posting': json_encode_beancount_entry(p[1]),
        'transaction_formatted': format_transaction(p[0]),
    }


def convert_uncleared_list(
        uncleared_entries: List[Tuple[Transaction, Posting]]) -> List[Any]:
    return [convert_uncleared(p) for p in uncleared_entries]


def convert_invalid_reference(ref: Tuple[Source, InvalidSourceReference]):
    def convert_transaction_posting_pair(
            pair: Tuple[Transaction, Optional[Posting]]) -> dict:
        transaction, posting = pair
        result = {
            'transaction': json_encode_beancount_entry(transaction),
            'posting': json_encode_beancount_entry(posting)
        }
        result['transaction_formatted'] = format_transaction(transaction)
        if posting is not None:
            result['posting_formatted'] = format_posting(posting, indent='')
        return result

    return {
        'num_extras':
        ref[1].num_extras,
        'source':
        ref[0].name,
        'transaction_posting_pairs': [
            convert_transaction_posting_pair(p)
            for p in ref[1].transaction_posting_pairs
        ],
    }


def convert_invalid_references(entries):
    return [convert_invalid_reference(ref) for ref in entries]


def json_encode_candidates(candidates: reconcile.Candidates):
    result = {}  # type: Dict[str, Any]

    def encode_used_transaction(transaction: Transaction,
                                index: Optional[int]) -> dict:
        if index is None:
            pending = None
            info = None
            source = None
        else:
            pending = candidates.pending_data[index]
            info = pending.info
            source = None if pending.source is None else pending.source.name
        return {
            'formatted': format_transaction(transaction),
            'entry': json_encode_beancount_entry(transaction),
            'pending_index': index,
            'info': info,
            'source': source,
        }

    result['used_transactions'] = [
        encode_used_transaction(transaction, index)
        for transaction, index in candidates.used_transactions
    ]
    result['candidates'] = candidates.candidates
    result['date'] = candidates.date
    result['number'] = candidates.number
    return result


def json_encode_candidate(obj: reconcile.Candidate):
    change_sets, _, _ = obj.staged_changes_with_unique_account_names.get_diff()
    _, _, new_entries = obj.staged_changes.get_diff()
    return dict(
        change_sets=change_sets,
        used_transaction_ids=obj.used_transaction_ids,
        substituted_accounts=obj.substituted_accounts or [],
        original_transaction_properties=obj.original_transaction_properties,
        new_entries=[json_encode_beancount_entry(x) for x in new_entries],
        associated_data=[x.__dict__ for x in obj.associated_data],
    )


def json_encode_pending_candidate(pending: reconcile.PendingEntry):
    return {
        'date': pending.date,
        'formatted': pending.formatted,
        'entries': [json_encode_beancount_entry(x) for x in pending.entries],
        'info': pending.info,
        'source': None if pending.source is None else pending.source.name,
        'id': pending.id
    }


def json_convert_pending_list(pending_data: List[reconcile.PendingEntry]):
    return [json_encode_pending_candidate(x) for x in pending_data]


def convert_errors(x):
    return x


def json_encode_state(obj):
    if isinstance(obj, Decimal):
        return str(obj)
    if isinstance(obj, (frozenset, set)):
        return list(obj)
    if isinstance(obj, datetime.date):
        return obj.strftime('%Y-%m-%d')
    if isinstance(obj, reconcile.Candidate):
        return json_encode_candidate(obj)
    if isinstance(obj, reconcile.Candidates):
        return json_encode_candidates(obj)


class IndexHandler(tornado.web.RequestHandler):
    def get(self):
        frontend_dist = self.application.frontend_dist
        self.set_header('Content-Type', 'text/html')
        import pkg_resources
        contents = pkg_resources.resource_string(
            __name__, 'frontend_dist/%s/index.html' % frontend_dist)
        contents = contents.replace(
            self.application.secret_key_pattern.encode(),
            self.application.secret_key.encode())
        self.write(contents)


data_convert_functions = {
    'errors': convert_errors,
    'uncleared': convert_uncleared_list,
    'invalid': convert_invalid_references,
    'pending': json_convert_pending_list,
}


class GetDataHandler(tornado.web.RequestHandler):
    def get(self, data_type, generation, begin_index, end_index):
        begin_index = int(begin_index)
        end_index = int(end_index)
        info = self.application.current_state.get(data_type)
        if info is None or str(info[0]) != generation:
            self.set_status(404)
            return self.finish('Current generation not specified.')
        if begin_index < 0 or begin_index > end_index or end_index > info[1]:
            self.set_status(400)
            return self.finish('Invalid index specified.')
        try:
            value = getattr(self.application,
                            'current_%s' % data_type)[begin_index:end_index]
            converted_value = data_convert_functions[data_type](value)
            json_encoding = json.dumps(
                converted_value, default=json_encode_state)
            self.set_header('Content-Type', 'application/json')
            self.write(json_encoding.encode())
        except:
            self.set_status(500)
            import traceback
            traceback.print_exc()
            return self.finish('Error writing data')


class ChangeCandidateHandler(tornado.web.RequestHandler):
    def post(self):
        msg = json.loads(self.request.body)
        self.application.handle_change_candidate(msg)
        self.set_header('Content-Type', 'application/json')
        self.write(json.dumps(None).encode())


class SelectCandidateHandler(tornado.web.RequestHandler):
    def post(self):
        msg = json.loads(self.request.body)
        new_entries = self.application.handle_select_candidate(msg) or []
        self.set_header('Content-Type', 'application/json')
        self.write(
            json.dumps(
                [json_encode_beancount_entry(x) for x in new_entries],
                default=json_encode_state).encode())


class SkipHandler(tornado.web.RequestHandler):
    def post(self):
        msg = json.loads(self.request.body)
        self.application.handle_skip(msg)
        self.set_header('Content-Type', 'application/json')
        self.write(json.dumps(None).encode())


class RetrainHandler(tornado.web.RequestHandler):
    def post(self):
        self.application.retrain()
        self.set_header('Content-Type', 'application/json')
        self.write(json.dumps(None).encode())


class WebSocketHandler(tornado.websocket.WebSocketHandler):
    def open(self, *args):
        self.application.socket_clients.add(self)
        self.set_nodelay(True)
        self.prev_state = dict()
        self.prev_state_generation = dict()
        self.watched_files = set()
        try:
            self.send_state_update()
        except:
            traceback.print_exc()

    def on_message(self, message):
        try:
            message = json.loads(message)
            f = getattr(self, 'on_message_%s' % message['type'], None)
            if f is None:
                raise TypeError('Invalid message type: %r' % message)
            self.application.ioloop.add_callback(f, message['value'])
        except:
            traceback.print_exc()
            pdb.pm()

    def on_close(self):
        print('closed, code = %r, reason = %r' % (self.close_code,
                                                  self.close_reason))
        self.application.socket_clients.remove(self)

    def send_state_update(self):
        try:
            update = dict()
            new_state = self.application.current_state
            new_state_generation = self.application.current_state_generation
            prev_state = self.prev_state
            prev_state_generation = self.prev_state_generation
            for k, v in new_state.items():
                generation = new_state_generation[k]
                if prev_state_generation.get(k) != generation:
                    prev_state_generation[k] = generation
                    update[k] = v
            for k, v in prev_state.items():
                if k not in new_state:
                    update[k] = None
            if len(update) > 0:
                self.write_message(
                    json.dumps(
                        dict(type='state_update', state=update),
                        default=json_encode_state))
                prev_state.update(update)
        except:
            traceback.print_exc()
            pdb.post_mortem()

    def send_file_update(self, filename, contents):
        try:
            self.write_message(
                json.dumps(
                    dict(
                        type='file_contents', path=filename,
                        contents=contents)))
        except:
            traceback.print_exc()

    def on_message_watch_file(self, filename):
        try:
            with open(filename, 'r') as f:
                contents = f.read()
            self.send_file_update(filename, contents)
            if filename in self.watched_files:
                return
            self.application.watched_files.setdefault(filename, set()).add(self)
        except:
            traceback.print_exc()

    def on_message_unwatch_file(self, filename):
        try:
            if filename not in self.watched_files:
                return
            filename_watchers = self.application.watched_files[filename]
            del filename_watchers[self]
            if not filename_watchers:
                del self.application.watched_files[filename]
        except:
            traceback.print_exc()

    def on_message_get_file_contents(self, filename):
        try:
            with open(filename, 'r') as f:
                contents = f.read()
            self.write_message(
                json.dumps(
                    dict(
                        type='file_contents', path=filename,
                        contents=contents)))
        except:
            traceback.print_exc()

    def on_message_set_file_contents(self, msg):
        try:
            filename = msg['filename']
            contents = msg['contents']
            with tempfile.NamedTemporaryFile(
                    mode='w',
                    dir=os.path.dirname(filename),
                    prefix=os.path.basename(filename) + '.tmp',
                    delete=False) as f:
                f.write(contents)
                f.flush()
                os.rename(f.name, filename)
        except:
            traceback.print_exc()


class GetFileHandler(tornado.web.RequestHandler):
    def get(self):
        path = self.get_argument('path')
        content_type = self.get_argument('content_type')
        try:
            with open(path, 'rb') as f:
                contents = f.read()
            self.set_header('Content-Type', content_type)
            self.write(contents)
        except:
            self.set_status(404)
            self.finish('File not found')


class Application(tornado.web.Application):
    def __init__(self, args, ioloop, **kwargs):

        # Secret key that prevents cross-origin access to the websocket.
        # The key is contained in the html response.
        secret_key = 'BEANCOUNT_IMPORT_SECRET_KEY_%s' % binascii.hexlify(
            os.urandom(20)).decode()
        self.secret_key_pattern = 'BEANCOUNT_IMPORT_SECRET_KEY_XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX'
        self.secret_key = secret_key
        self.ioloop = ioloop
        super(Application, self).__init__([
            ('/', IndexHandler),
            (r'/%s/(errors|pending|invalid|uncleared)/([^/]*)/(\d+)-(\d+)' %
             secret_key, GetDataHandler),
            (r'/%s/websocket' % secret_key, WebSocketHandler),
            (r'/%s/get_file' % secret_key, GetFileHandler),
            (r'/%s/change_candidate' % secret_key, ChangeCandidateHandler),
            (r'/%s/select_candidate' % secret_key, SelectCandidateHandler),
            (r'/%s/skip' % secret_key, SkipHandler),
            (r'/%s/retrain' % secret_key, RetrainHandler),
        ], **kwargs)
        self.frontend_dist = args.frontend_dist
        self.socket_clients = set()
        self.watched_files = dict()
        self.current_state = dict()
        self.current_state_generation = dict()
        self.generation = 0
        self.skip_ids = None

        self.log_status('Initializing')

        self.reconciler = reconcile.Reconciler(
            journal_path=args.journal_input,
            ignore_path=args.ignored_journal,
            log_status=self.log_status,
            options=vars(args))
        self.reset()

        self.check_modification_timer = tornado.ioloop.PeriodicCallback(
            self._check_modification, 100)
        self.check_modification_timer.start()

    def next_generation(self):
        generation = self.generation
        self.generation += 1
        return generation

    def _notify_modified_files(self, modified_filenames: List[str]):
        for filename in modified_filenames:
            watchers = self.watched_files.get(filename, None)
            if watchers:
                try:
                    with open(filename, 'r') as f:
                        contents = f.read()
                    for watcher in watchers:
                        watcher.send_file_update(filename, contents)
                except:
                    traceback.print_exc()

    def _check_modification(self):
        if self.reconciler.loaded_future.done():
            loaded_reconciler = self.reconciler.loaded_future.result()
            modified_filenames = loaded_reconciler.editor.check_any_journal_modification(
            )
            if modified_filenames:
                self._notify_modified_files(list(modified_filenames))
                self.reconciler.reload_journal()
                self.reset()

    def reset(self):
        self.next_candidates = None
        self.current_errors = None
        self.current_invalid = None
        self.current_uncleared = None
        self.current_pending = None
        self.set_state(
            pending=None,
            errors=None,
            invalid=None,
            uncleared=None,
            main_journal_path=os.path.realpath(self.reconciler.journal_path),
            candidates=None)
        self.ioloop.add_future(self.reconciler.loaded_future,
                               self._handle_reconciler_loaded)

    def retrain(self):
        if self.reconciler.loaded_future.done():
            self.reconciler.retrain()
            self.reset()

    def _handle_reconciler_loaded(self, loaded_future):
        try:
            loaded_reconciler = loaded_future.result()
            generation = self.next_generation()
            self.set_state(
                errors=(generation, len(loaded_reconciler.errors)),
                invalid=(generation, len(loaded_reconciler.invalid_references)),
                accounts=sorted(loaded_reconciler.editor.accounts.keys()),
                journal_filenames=sorted(
                    list(loaded_reconciler.editor.journal_filenames)))
            self.current_errors = loaded_reconciler.errors
            self.current_invalid = loaded_reconciler.invalid_references
            self.get_next_candidates(new_pending=True)
        except:
            traceback.print_exc()
            pdb.post_mortem()

    def get_next_candidates(self, new_pending):
        loaded_reconciler = self.reconciler.loaded_future.result()
        start_time = time.time()
        self.next_candidates, index, self.skip_ids = loaded_reconciler.get_next_candidates(
            self.skip_ids)
        end_time = time.time()
        print('Got next candidates in %.4f seconds' % (end_time - start_time))
        generation = self.next_generation()
        kwargs = dict()
        if new_pending:
            kwargs.update(
                pending=(generation, len(loaded_reconciler.pending_data)),
                uncleared=(generation,
                           len(loaded_reconciler.uncleared_postings)),
            )

        accounts = sorted(loaded_reconciler.editor.accounts.keys())
        if accounts != self.current_state['accounts']:
            kwargs.update(accounts=accounts)

        self.current_pending = loaded_reconciler.pending_data
        self.current_uncleared = loaded_reconciler.uncleared_postings
        if self.next_candidates is None:
            self.set_state(candidates=None, pending_index=None, **kwargs)
        else:
            self.set_state(
                candidates=self.next_candidates,
                candidates_generation=generation,
                pending_index=index,
                **kwargs)

    def _broadcast_state_changed(self):
        try:
            for client in self.socket_clients:
                client.send_state_update()
        except:
            traceback.print_exc()

    # Forces a state update message to be sent to clients even for state objects that have not
    # changed.
    def set_state_force(self, **kwargs):
        for k, v in kwargs.items():
            self.current_state[k] = v
            self.current_state_generation[k] = self.next_generation()
        self.set_state()

    def set_state(self, **kwargs):
        for k, v in kwargs.items():
            if v is not self.current_state.get(k):
                self.current_state[k] = v
                self.current_state_generation[k] = self.next_generation()
        self.ioloop.add_callback(self._broadcast_state_changed)

    def log_status(self, message):
        logging.info(message)
        self.set_state(message=message)

    def handle_change_candidate(self, msg):
        try:
            if (self.next_candidates is not None and
                    self.current_state['candidates_generation'] ==
                    msg['generation']):
                self.next_candidates.change_transaction(msg['candidate_index'],
                                                        msg['changes'])
                self.set_state_force(candidates=self.next_candidates)
        except:
            traceback.print_exc()

    def handle_select_candidate(self, msg):
        try:
            if (self.next_candidates is not None and msg['generation'] ==
                    self.current_state['candidates_generation']):
                index = msg['index']
                if index >= 0 and index < len(self.next_candidates.candidates):
                    candidate = self.next_candidates.candidates[index]
                    ignore = msg.get('ignore', None) is True
                    result = self.reconciler.loaded_future.result(
                    ).accept_candidate(
                        candidate,
                        ignore=ignore,
                    )
                    self._notify_modified_files(result.modified_filenames)
                    self.get_next_candidates(new_pending=True)
                    return result.new_entries
        except:
            traceback.print_exc()
            print('got error')
            pdb.post_mortem()

    def handle_skip(self, msg):
        pending_generation = int(msg['generation'])
        pending_index = int(msg['index'])
        pending_state = self.current_state['pending']
        if pending_state is None:
            return
        if pending_state[0] != pending_generation:
            return
        loaded_reconciler = self.reconciler.loaded_future.result()
        if pending_index < 0:
            pending_index = 0
        if pending_index >= pending_state[1]:
            pending_index = pending_state[1] - 1
        self.skip_ids = loaded_reconciler.get_skip_ids_by_index(pending_index)
        self.get_next_candidates(new_pending=False)

    def handle_retrain(self, _):
        self.retrain()


def main(argv, **kwargs):
    argparser = argparse.ArgumentParser(
        parents=[reconcile.get_entry_file_selector_argparser(kwargs)])
    argparser.add_argument(
        '--journal_input',
        help='Top-level Beancount input file',
        required=kwargs.get('journal_input') is None)
    argparser.add_argument(
        '--ignored_journal',
        help='Beancount input file containing ignored entries',
        required=kwargs.get('ignored_journal') is None)
    argparser.add_argument(
        '--data_sources',
        help='Data sources JSON specification',
        type=json.loads,
        default=[])
    argparser.add_argument(
        '--ignore_account_for_classification_pattern',
        help=
        'Regular expression matching account names that should be ignored for the purpose of automatic classification.  Only transactions with exactly two non-ignored postings are used.',
        default=training.DEFAULT_IGNORE_ACCOUNT_FOR_CLASSIFICATION_PATTERN)
    argparser.add_argument(
        '--log-output',
        type=str,
        help='Filename to which log output will be written.')
    argparser.add_argument(
        '--account_pattern',
        type=str,
        help='Regular expression for limiting accounts to reconcile.')
    argparser.add_argument(
        '-p',
        '--port',
        type=int,
        default=8101,
        help='Port on which webserver listens.')
    argparser.add_argument(
        '-a',
        '--address',
        type=str,
        default='127.0.0.1',
        help='Address on which webserver listens.')
    argparser.add_argument(
        '--browser',
        action='store_true',
        help='Open a web browser automatically.')
    argparser.add_argument(
        '-d',
        '--debug',
        help='Set log verbosity to DEBUG.',
        action='store_const',
        dest='loglevel',
        const=logging.DEBUG,
        default=logging.WARNING)
    argparser.add_argument(
        '--dev',
        help='Use development version of frontend.',
        action='store_const',
        dest='frontend_dist',
        const='dev',
        default='prod',
    )
    argparser.add_argument(
        '-v',
        '--verbose',
        help='Set log verbosity to INFO.',
        action='store_const',
        dest='loglevel',
        const=logging.DEBUG)
    argparser.add_argument(
        '--fuzzy_match_days',
        type=int,
        default=5,
        help=
        'Maximum number of days by which the dates of two matching entries may differ.'
    )
    argparser.add_argument(
        '--classifier_cache',
        type=str,
        help=
        'Cache file for automatic account prediction classifier.  This speeds up loading.'
    )
    argparser.set_defaults(**kwargs)
    args = argparser.parse_args(argv)
    logging_args = dict(level=args.loglevel)
    if args.log_output is not None:
        logging_args['filename'] = args.log_output
    logging.basicConfig(**logging_args)

    ioloop = tornado.ioloop.IOLoop.instance()
    app = Application(args=args, ioloop=ioloop, debug=True)

    http_server = tornado.httpserver.HTTPServer(app)
    sockets = tornado.netutil.bind_sockets(
        port=args.port or None, address=args.address)
    http_server.add_sockets(sockets)
    server_url = 'http://%s:%s' % sockets[0].getsockname()[0:2]
    print('Listening at %s' % server_url)
    if args.browser:
        webbrowser.open(server_url, new=1)
    ioloop.start()


if __name__ == '__main__':
    main(sys.argv[1:])
