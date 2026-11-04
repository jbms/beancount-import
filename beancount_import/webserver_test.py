from typing import Dict, List
import asyncio
import io
import os
import time

import py
import tornado.gen
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
    """Drives a webserver through a sequence of journal snapshots.

    Each snapshot directory under `golden_directory` holds the journal files
    to copy into `temp_dir`; `transactions.beancount` (and, optionally,
    `additional/transactions.beancount`) also list the FIXME transactions the
    reconciler is expected to report as pending once it has reloaded.
    """

    # Upper bound on how long the file watcher may take to notice a change
    # and finish reloading the journal.
    reload_timeout_seconds = 10.0
    poll_interval_seconds = 0.05

    def __init__(self,
                 golden_directory: str,
                 temp_dir: str,
                 initial_snapshot: int = 0):
        self.golden_directory = golden_directory
        self.temp_dir = temp_dir
        self._written_contents = {}  # type: Dict[str, bytes]
        # Copy the initial journal before the application starts loading it,
        # so the first load cannot race with the copy.
        self._copy_snapshot(str(initial_snapshot))
        self.ioloop = tornado.ioloop.IOLoop.instance()
        self.application = webserver.Application(
            webserver.parse_arguments(
                argv=[],
                journal_input=os.path.join(temp_dir, 'journal.beancount'),
                ignored_journal=os.path.join(temp_dir, 'ignore.beancount'),
                default_output=os.path.join(temp_dir, 'transactions.beancount')),
            ioloop=self.ioloop)

    def _copy_snapshot(self, snapshot_name: str):
        """Copies the snapshot's files into `temp_dir`.

        Only files whose contents changed are written, so that each step
        exercises the watch on the directory that actually changed. The
        comparison uses the contents last written rather than re-reading
        `temp_dir`, since reading a watched file also produces watcher events.
        """
        golden = os.path.join(self.golden_directory, snapshot_name)
        for subdir in ('', 'additional'):
            golden_dir = os.path.join(golden, subdir)
            os.makedirs(os.path.join(self.temp_dir, subdir), exist_ok=True)
            if not os.path.isdir(golden_dir):
                continue
            for name in os.listdir(golden_dir):
                source = os.path.join(golden_dir, name)
                if os.path.isdir(source):
                    continue
                with open(source, 'rb') as f:
                    contents = f.read()
                target = os.path.join(self.temp_dir, subdir, name)
                if self._written_contents.get(target) == contents:
                    continue
                with open(target, 'wb') as f:
                    f.write(contents)
                self._written_contents[target] = contents

    def _expected_pending(self, snapshot_name: str) -> Dict[str, str]:
        golden = os.path.join(self.golden_directory, snapshot_name)
        expected = {}
        for key, path in (('main', 'transactions.beancount'),
                          ('additional',
                           os.path.join('additional', 'transactions.beancount'))):
            full_path = os.path.join(golden, path)
            contents = ''
            if os.path.exists(full_path):
                with open(full_path, 'r', encoding='utf-8') as f:
                    contents = f.read()
            expected[key] = contents.strip()
        return expected

    @staticmethod
    def _actual_pending(
            loaded_reconciler: reconcile.LoadedReconciler) -> Dict[str, str]:
        return {
            'main': _encode_transaction_entries(
                loaded_reconciler.pending_data, additional=False).strip(),
            'additional': _encode_transaction_entries(
                loaded_reconciler.pending_data, additional=True).strip(),
        }

    def _wait_for_state(self, snapshot_name: str):
        """Runs the IOLoop until the reconciler reports the snapshot's pending
        transactions, failing if that does not happen within the timeout."""
        expected = self._expected_pending(snapshot_name)
        reconciler = self.application.reconciler

        async def wait():
            deadline = time.monotonic() + self.reload_timeout_seconds
            while True:
                future = reconciler.loaded_future
                loaded_reconciler = await asyncio.wrap_future(future)
                # Only compare once no newer reload has been started.
                if future is reconciler.loaded_future:
                    actual = self._actual_pending(loaded_reconciler)
                    if actual == expected:
                        return
                    if time.monotonic() > deadline:
                        assert actual == expected, (
                            'snapshot %s not reloaded within %ss' %
                            (snapshot_name, self.reload_timeout_seconds))
                await asyncio.sleep(self.poll_interval_seconds)

        self.ioloop.run_sync(wait)

    def snapshot(self, snapshot_number: int):
        snapshot_name = str(snapshot_number)
        self._copy_snapshot(snapshot_name)
        self._wait_for_state(snapshot_name)


def test_check_modification(tmpdir: py.path.local):
    tester = WebserverGoldenTester(
        golden_directory=os.path.join(testdata_root, 'webserver', 'test_check_modification'),
        temp_dir=str(tmpdir),
    )
    snapshot_count = 5
    try:
        for snapshot_number in range(snapshot_count):
            tester.snapshot(snapshot_number)
    finally:
        tester.application.stop_check_modification_observer()


def test_modification_during_reload_is_not_lost(tmpdir: py.path.local,
                                                monkeypatch):
    """A journal edit saved while the journal is (re)loading must still be
    picked up once loading finishes."""
    temp_dir = str(tmpdir)
    journal_path = os.path.join(temp_dir, 'journal.beancount')

    def write_journal(*narrations: str, mtime_offset: float):
        with open(journal_path, 'w') as f:
            f.write('2020-01-01 open Assets:Cash\n'
                    '2020-01-01 open Expenses:Misc\n')
            for narration in narrations:
                f.write('\n2020-01-02 * "%s"\n'
                        '  Assets:Cash  -1 USD\n'
                        '  Expenses:Misc  1 USD\n' % narration)
        # Ensure each version is seen as modified, whatever the file system's
        # timestamp resolution.
        os.utime(journal_path, (os.stat(journal_path).st_mtime + mtime_offset,
                                os.stat(journal_path).st_mtime + mtime_offset))

    write_journal(mtime_offset=-100)
    for name in ('ignore.beancount', 'transactions.beancount'):
        open(os.path.join(temp_dir, name), 'w').close()

    ioloop = tornado.ioloop.IOLoop.instance()
    application = webserver.Application(
        webserver.parse_arguments(
            argv=[],
            journal_input=journal_path,
            ignored_journal=os.path.join(temp_dir, 'ignore.beancount'),
            default_output=os.path.join(temp_dir, 'transactions.beancount')),
        ioloop=ioloop)
    # File system events are simulated by calling check_modification.
    monkeypatch.setattr(application, 'start_check_modification_observer',
                        lambda loaded_reconciler: None)

    def wait_until_loaded():
        while True:
            future = application.reconciler.loaded_future
            ioloop.run_sync(lambda: future)
            # Let _handle_reconciler_loaded run; it may start another load.
            ioloop.run_sync(lambda: tornado.gen.sleep(0.01))
            if application.reconciler.loaded_future is future:
                return future.result()

    wait_until_loaded()

    original_init = reconcile.LoadedReconciler.__init__

    def init_then_edit(self, *args, **kwargs):
        original_init(self, *args, **kwargs)
        if not getattr(init_then_edit, 'edited', False):
            init_then_edit.edited = True
            # Saved after this load read the journal, but before it finished.
            write_journal('first', 'second', mtime_offset=-50)
            application.check_modification()

    monkeypatch.setattr(reconcile.LoadedReconciler, '__init__', init_then_edit)
    write_journal('first', mtime_offset=-80)
    application.check_modification()
    loaded = wait_until_loaded()
    assert loaded.editor.check_any_journal_modification() == set()
    assert sorted(e.narration for e in loaded.editor.entries
                  if hasattr(e, 'narration')) == ['first', 'second']
