from typing import List
import functools
import io
import os
import shutil

import py
import tornado.ioloop

from . import reconcile
from . import test_util
from . import webserver

testdata_root = os.path.realpath(
    os.path.join(os.path.dirname(__file__), '..', 'testdata'))


def _encode_transaction_entries(
                                transactions_list: List[reconcile.PendingEntry],
                                additional: bool) -> str:
    out = io.StringIO()
    for transactions in transactions_list:
        for entry in transactions.entries:
            if ('additional' in entry.tags) == additional:
                out.write(test_util.format_entries([entry]).strip() + '\n\n')
    return out.getvalue().strip() + '\n'


class WebserverGoldenTester:
    def __init__(self,
                 golden_directory: str,
                 temp_dir: str):
        self.golden_directory = golden_directory
        self.temp_dir = temp_dir
        self.ioloop = tornado.ioloop.IOLoop.instance()
        self.application = webserver.Application(
            webserver.parse_arguments(
                argv=[],
                journal_input=os.path.join(temp_dir, 'journal.beancount'),
                ignored_journal=os.path.join(temp_dir, 'ignore.beancount'),
                default_output=os.path.join(temp_dir, 'transactions.beancount')),
            ioloop=self.ioloop)

    def _check_state(self, snapshot_name: str):
        loaded_reconciler = self.application.reconciler.loaded_future.result()
        golden = os.path.join(self.golden_directory, snapshot_name)

        test_util.check_golden_contents(
            path=os.path.join(self.temp_dir, 'transactions.beancount'),
            expected_contents=_encode_transaction_entries(
                loaded_reconciler.pending_data, additional=False),
        )
        if os.path.exists(os.path.join(self.temp_dir, 'additional', 'transactions.beancount')):
            test_util.check_golden_contents(
                path=os.path.join(self.temp_dir, 'additional', 'transactions.beancount'),
                expected_contents=_encode_transaction_entries(
                    loaded_reconciler.pending_data, additional=True),
            )

    def snapshot(self, snapshot_number):
        snapshot_name = str(snapshot_number)
        golden = os.path.join(self.golden_directory, snapshot_name)
        for name in os.listdir(golden):
            if name == 'additional':
                continue
            shutil.copyfile(
                os.path.join(golden, name), os.path.join(self.temp_dir, name))

        additional_golden_path = os.path.join(golden, 'additional')
        additional_temp_path = os.path.join(self.temp_dir, 'additional')
        os.makedirs(additional_temp_path, exist_ok=True)
        if os.path.exists(additional_golden_path):
            for name in os.listdir(additional_golden_path):
                shutil.copyfile(
                    os.path.join(additional_golden_path, name),
                    os.path.join(additional_temp_path, name))

        self.ioloop.add_future(self.application.reconciler.loaded_future,
                               functools.partial(self._check_state, snapshot_name))
        self.ioloop.run_sync(lambda: self.application.reconciler.loaded_future)


def test_check_modification(tmpdir: py.path.local):
    tester = WebserverGoldenTester(
        golden_directory=os.path.join(testdata_root, 'webserver', 'test_check_modification'),
        temp_dir=str(tmpdir),
    )
    snapshot_count = 5
    for snapshot_number in range(snapshot_count):
        tester.snapshot(snapshot_number)
