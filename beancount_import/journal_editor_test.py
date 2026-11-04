import datetime

import beancount.parser.printer
from beancount.core.data import Transaction, Posting, EMPTY_SET
from beancount.core.number import MISSING, Decimal
from beancount.core.amount import Amount
import os
import pytest
import py
import stat

from . import journal_editor
from . import test_util

META_IGNORE = set({'__tolerances__', '__automatic__'})


def clean_meta(meta):
    if meta is None:
        return {}
    return {k: v for k, v in meta.items() if k not in META_IGNORE}


def clean_posting(posting):
    return posting._replace(meta=clean_meta(posting.meta))


def clean_entry(entry):
    entry = entry._replace(meta=clean_meta(entry.meta))
    if isinstance(entry, Transaction):
        entry = entry._replace(
            postings=list(map(clean_posting, entry.postings)))
    return entry


def clean_entries(entries):
    return list(map(clean_entry, entries))


def check_file_contents(path, expected_contents):
    with open(path, 'r') as f:
        contents = f.read()
    assert contents == expected_contents


def check_journal_entries(editor: journal_editor.JournalEditor):
    new_editor = journal_editor.JournalEditor(editor.journal_path,
                                              editor.ignored_path)
    assert clean_entries(editor.entries) == clean_entries(new_editor.entries)
    assert clean_entries(editor.ignored_entries) == clean_entries(
        new_editor.ignored_entries)


def create_journal(tmpdir: py.path.local,
                   contents: str,
                   name: str = 'journal.beancount'):
    path = tmpdir.join(name)
    path.write_binary(contents.encode())
    return str(path)


def test_load_file_simple(tmpdir):
    """Tests that partial booking resolves the missing units."""
    journal_path = create_journal(
        tmpdir, """
2015-02-01 * "Test transaction"
  Assets:Account-A  100 USD
  Assets:Account-B
""")
    editor = journal_editor.JournalEditor(journal_path)
    expected_entries = test_util.parse("""
        2015-02-01 * "Test transaction"
          Assets:Account-A  100 USD
          Assets:Account-B  -100 USD
        """)
    assert test_util.format_entries(
        editor.entries) == test_util.format_entries(expected_entries)


def test_load_file_reduction(tmpdir):
    """Tests that partial booking leaves the reduction alone."""
    journal_path = create_journal(
        tmpdir, """
2015-02-01 * "Test transaction"
  Assets:Account-A  -5 STOCK {}
  Assets:Account-B
""")
    editor = journal_editor.JournalEditor(journal_path)
    expected_entries = test_util.parse("""
        2015-02-01 * "Test transaction"
          Assets:Account-A  -5 STOCK {}
          Assets:Account-B
        """)
    assert test_util.format_entries(
        editor.entries) == test_util.format_entries(expected_entries)


def test_load_file_buy(tmpdir):
    """Tests that partial booking handles a posting with a cost."""
    journal_path = create_journal(
        tmpdir, """
2015-02-01 * "Test transaction"
  Assets:Account-A  5 STOCK {3.00 USD}
  Assets:Account-B
""")
    editor = journal_editor.JournalEditor(journal_path)
    expected_entries = test_util.parse("""
        2015-02-01 * "Test transaction"
          Assets:Account-A  5 STOCK {3.00 USD}
          Assets:Account-B -15.00 USD
        """)
    assert test_util.format_entries(
        editor.entries) == test_util.format_entries(expected_entries)


def test_simple(tmpdir):
    journal_path = create_journal(
        tmpdir, """
2015-02-01 * "Test transaction"
  Assets:Account-A  100 USD
  Assets:Account-B
""")
    editor = journal_editor.JournalEditor(journal_path)
    stage = editor.stage_changes()
    old_entry = editor.entries[0]
    new_entry = old_entry._replace(postings=[
        old_entry.postings[0]._replace(meta=dict(note="Hello")),
        old_entry.postings[1],
    ])
    stage.change_entry(old_entry, new_entry)
    result = stage.apply()
    assert result.old_entries == [old_entry]
    assert result.old_ignored_entries == []
    assert result.new_ignored_entries == []
    check_file_contents(
        journal_path, """
2015-02-01 * "Test transaction"
  Assets:Account-A  100 USD
    note: "Hello"
  Assets:Account-B
""")
    check_journal_entries(editor)

def test_permissions(tmpdir):
    journal_path = create_journal(
        tmpdir, """
2015-02-01 * "Test transaction"
  Assets:Account-A  100 USD
  Assets:Account-B
""")
    # Set unusual permissions. We'll check that they are preserved.
    os.chmod(journal_path, mode = stat.S_IRUSR | stat.S_IWUSR | stat.S_IXGRP)
    # In case the OS doesn't support arbitrary inputs to os.chmod,
    # check what we were actually able to set the mode to.
    original_mode = os.stat(journal_path).st_mode
    editor = journal_editor.JournalEditor(journal_path)
    stage = editor.stage_changes()
    old_entry = editor.entries[0]
    new_entry = old_entry._replace(postings=[
        old_entry.postings[0]._replace(meta=dict(note="Hello")),
        old_entry.postings[1],
    ])
    stage.change_entry(old_entry, new_entry)
    result = stage.apply()
    assert os.stat(journal_path).st_mode == original_mode


def test_existing_empty_meta_value(tmpdir):
    journal_path = create_journal(
        tmpdir, """
2015-02-01 * "Test transaction"
  Assets:Account-A  100 USD
    other:
  Assets:Account-B
""")
    editor = journal_editor.JournalEditor(journal_path)
    stage = editor.stage_changes()
    old_entry = editor.entries[0]
    assert old_entry.postings[0].meta['other'] is None
    new_entry = old_entry._replace(postings=[
        old_entry.postings[0]._replace(
            meta=dict(old_entry.postings[0].meta, note="Hello")),
        old_entry.postings[1],
    ])
    stage.change_entry(old_entry, new_entry)
    result = stage.apply()
    assert result.old_entries == [old_entry]
    check_file_contents(
        journal_path, """
2015-02-01 * "Test transaction"
  Assets:Account-A  100 USD
    other:
    note: "Hello"
  Assets:Account-B
""")
    check_journal_entries(editor)


def test_transaction_add_meta(tmpdir):
    journal_path = create_journal(
        tmpdir, """
2015-02-01 * "Test transaction"
  transaction_note: "Hello"
  Assets:Account-A  100 USD
  Assets:Account-B
""")
    editor = journal_editor.JournalEditor(journal_path)
    stage = editor.stage_changes()
    old_entry = editor.entries[0]
    new_entry = old_entry._replace(postings=[
        old_entry.postings[0]._replace(meta=dict(note="Hello")),
        old_entry.postings[1],
    ])
    stage.change_entry(old_entry, new_entry)
    result = stage.apply()
    assert result.old_ignored_entries == []
    assert result.new_ignored_entries == []
    assert result.old_entries == [old_entry]
    check_file_contents(
        journal_path, """
2015-02-01 * "Test transaction"
  transaction_note: "Hello"
  Assets:Account-A  100 USD
    note: "Hello"
  Assets:Account-B
""")
    check_journal_entries(editor)


def test_transaction_modify_narration(tmpdir):
    journal_path = create_journal(
        tmpdir, """
2015-02-01 * "Test transaction"
  Assets:Account-A  100 USD
  Assets:Account-B
""")
    editor = journal_editor.JournalEditor(journal_path)
    stage = editor.stage_changes()
    old_entry = editor.entries[0]
    new_entry = old_entry._replace(narration='Modified transaction')
    stage.change_entry(old_entry, new_entry)
    result = stage.apply()
    assert result.old_entries == [old_entry]
    assert result.old_ignored_entries == []
    assert result.new_ignored_entries == []
    check_file_contents(
        journal_path, """
2015-02-01 * "Modified transaction"
  Assets:Account-A  100 USD
  Assets:Account-B
""")
    check_journal_entries(editor)


def test_two(tmpdir):
    journal_path = create_journal(
        tmpdir, """
2015-01-01 * "Test transaction 1"
  Assets:Account-A  100 USD
  Assets:Account-B

2015-02-01 * "Test transaction"
  Assets:Account-A  100 USD
  Assets:Account-B

2015-03-01 * "Test transaction 2"
  Assets:Account-A  100 USD
  Assets:Account-B

""")
    editor = journal_editor.JournalEditor(journal_path)
    stage = editor.stage_changes()
    old_entry = editor.entries[1]
    new_entry = old_entry._replace(postings=[
        old_entry.postings[0]._replace(meta=dict(note="Hello")),
        old_entry.postings[1],
    ])
    stage.change_entry(old_entry, new_entry)
    result = stage.apply()
    assert result.old_entries == [old_entry]
    assert result.old_ignored_entries == []
    assert result.new_ignored_entries == []
    check_file_contents(
        journal_path, """
2015-01-01 * "Test transaction 1"
  Assets:Account-A  100 USD
  Assets:Account-B

2015-02-01 * "Test transaction"
  Assets:Account-A  100 USD
    note: "Hello"
  Assets:Account-B

2015-03-01 * "Test transaction 2"
  Assets:Account-A  100 USD
  Assets:Account-B

""")
    check_journal_entries(editor)


def test_two2(tmpdir):
    journal_path = create_journal(
        tmpdir, """
2015-01-01 * "Test transaction 1"
  Assets:Account-A  100 USD
  Assets:Account-B

2015-02-01 * "Test transaction"
  Assets:Account-A  100 USD
  Assets:Account-B

2015-03-01 * "Test transaction 2"
  Assets:Account-A  100 USD
  Assets:Account-B

""")
    editor = journal_editor.JournalEditor(journal_path)
    stage = editor.stage_changes()
    old_entry = editor.entries[1]
    new_entry = old_entry._replace(postings=[
        old_entry.postings[0]._replace(meta=dict(note="Hello")),
        old_entry.postings[1],
    ])
    stage.change_entry(old_entry, new_entry)

    old_entry2 = editor.entries[2]
    new_entry2 = old_entry2._replace(postings=[
        old_entry2.postings[0],
        old_entry2.postings[1]._replace(meta=dict(note="Foo")),
    ])
    stage.change_entry(old_entry2, new_entry2)

    result = stage.apply()
    assert result.old_entries == [old_entry, old_entry2]
    assert result.old_ignored_entries == []
    assert result.new_ignored_entries == []
    check_file_contents(
        journal_path, """
2015-01-01 * "Test transaction 1"
  Assets:Account-A  100 USD
  Assets:Account-B

2015-02-01 * "Test transaction"
  Assets:Account-A  100 USD
    note: "Hello"
  Assets:Account-B

2015-03-01 * "Test transaction 2"
  Assets:Account-A  100 USD
  Assets:Account-B
    note: "Foo"

""")
    check_journal_entries(editor)


def test_remove(tmpdir):
    journal_path = create_journal(
        tmpdir, """
2015-01-01 * "Test transaction 1"
  Assets:Account-A  100 USD
  Assets:Account-B

2015-02-01 * "Test transaction"
  Assets:Account-A  100 USD
  Assets:Account-B

2015-03-01 * "Test transaction 2"
  Assets:Account-A  100 USD
  Assets:Account-B
""")
    editor = journal_editor.JournalEditor(journal_path)
    stage = editor.stage_changes()
    old_entry = editor.entries[1]
    stage.remove_entry(old_entry)
    result = stage.apply()
    assert result.old_entries == [old_entry]
    assert result.new_entries == []
    assert result.old_ignored_entries == []
    assert result.new_ignored_entries == []
    check_file_contents(
        journal_path, """
2015-01-01 * "Test transaction 1"
  Assets:Account-A  100 USD
  Assets:Account-B

2015-03-01 * "Test transaction 2"
  Assets:Account-A  100 USD
  Assets:Account-B
""")
    check_journal_entries(editor)


def test_add(tmpdir):
    journal_path = create_journal(
        tmpdir, """
2015-01-01 * "Test transaction 1"
  Assets:Account-A  100 USD
  Assets:Account-B

2015-03-01 * "Test transaction 2"
  Assets:Account-A  100 USD
  Assets:Account-B
""")
    editor = journal_editor.JournalEditor(journal_path)
    stage = editor.stage_changes()
    new_transaction = Transaction(
        meta=None,
        date=datetime.date(2015, 4, 1),
        flag='*',
        payee=None,
        narration='New transaction',
        tags=EMPTY_SET,
        links=EMPTY_SET,
        postings=[
            Posting(
                account='Assets:Account-A',
                units=Amount(Decimal(3), 'USD'),
                cost=None,
                price=None,
                flag=None,
                meta=None),
            Posting(
                account='Assets:Account-B',
                units=MISSING,
                cost=None,
                price=None,
                flag=None,
                meta=None),
        ],
    )
    stage.add_entry(new_transaction, journal_path)
    result = stage.apply()
    check_file_contents(
        journal_path, """
2015-01-01 * "Test transaction 1"
  Assets:Account-A  100 USD
  Assets:Account-B

2015-03-01 * "Test transaction 2"
  Assets:Account-A  100 USD
  Assets:Account-B

2015-04-01 * "New transaction"
  Assets:Account-A  3 USD
  Assets:Account-B
""")
    check_journal_entries(editor)

def test_add_ignored(tmpdir):
    journal_path = create_journal(
        tmpdir, """
2015-01-01 * "Test transaction 1"
  Assets:Account-A  100 USD
  Assets:Account-B
""")
    ignored_path = create_journal(
        tmpdir, """
2015-03-01 * "Test transaction 2"
  Assets:Account-A  100 USD
  Assets:Account-B
""",
        name='ignored.beancount')
    editor = journal_editor.JournalEditor(journal_path, ignored_path)
    stage = editor.stage_changes()
    new_transaction = Transaction(
        meta=None,
        date=datetime.date(2015, 4, 1),
        flag='*',
        payee=None,
        narration='New transaction',
        tags=EMPTY_SET,
        links=EMPTY_SET,
        postings=[
            Posting(
                account='Assets:Account-A',
                units=Amount(Decimal(3), 'USD'),
                cost=None,
                price=None,
                flag=None,
                meta=None),
            Posting(
                account='Assets:Account-B',
                units=MISSING,
                cost=None,
                price=None,
                flag=None,
                meta=None),
        ],
    )
    stage.add_entry(new_transaction, ignored_path)
    result = stage.apply()
    check_file_contents(
        journal_path, """
2015-01-01 * "Test transaction 1"
  Assets:Account-A  100 USD
  Assets:Account-B
""")
    check_file_contents(
        ignored_path, """
2015-03-01 * "Test transaction 2"
  Assets:Account-A  100 USD
  Assets:Account-B

2015-04-01 * "New transaction"
  Assets:Account-A  3 USD
  Assets:Account-B
""")
    check_journal_entries(editor)

def test_remove_ignored(tmpdir):
    journal_path = create_journal(
        tmpdir, """
2015-03-01 * "Test transaction 2"
  Assets:Account-A  100 USD
  Assets:Account-B
""")
    ignored_path = create_journal(
        tmpdir,
        """
2015-01-01 * "Test transaction 1"
  Assets:Account-A  100 USD
  Assets:Account-B

2015-02-01 * "Test transaction"
  Assets:Account-A  100 USD
  Assets:Account-B
""",
        name='ignored.beancount')
    editor = journal_editor.JournalEditor(journal_path, ignored_path)
    stage = editor.stage_changes()
    old_entry = editor.ignored_entries[1]
    stage.remove_entry(old_entry)
    result = stage.apply()
    assert result.old_ignored_entries == [old_entry]
    assert result.new_entries == []
    assert result.old_entries == []
    assert result.new_ignored_entries == []
    check_file_contents(
        journal_path, """
2015-03-01 * "Test transaction 2"
  Assets:Account-A  100 USD
  Assets:Account-B
""")
    check_file_contents(
        ignored_path, """
2015-01-01 * "Test transaction 1"
  Assets:Account-A  100 USD
  Assets:Account-B
""")
    check_journal_entries(editor)


def test_journal_cache(tmpdir, monkeypatch):
    journal_path = os.path.join(str(tmpdir), 'journal.beancount')
    included_path = os.path.join(str(tmpdir), 'included.beancount')
    cache_path = os.path.join(str(tmpdir), 'cache.pickle')
    with open(journal_path, 'w') as f:
        f.write('include "included.beancount"\n'
                '2015-01-01 open Assets:Checking\n'
                '2015-01-01 open Expenses:Food\n'
                '2015-01-01 custom "budget" Expenses:Food "monthly" 10 USD\n')
    with open(included_path, 'w') as f:
        f.write('2015-03-01 * "Store"\n'
                '  Assets:Checking  -10 USD\n'
                '  Expenses:Food\n')

    def state(editor):
        return ([beancount.parser.printer.format_entry(e)
                 for e in editor.entries],
                sorted(editor.accounts), sorted(editor.journal_filenames))

    loads = []
    real_load_file = journal_editor.load_file
    monkeypatch.setattr(journal_editor, 'load_file',
                        lambda *a, **kw: loads.append(a) or real_load_file(*a, **kw))

    uncached = state(journal_editor.JournalEditor(journal_path))
    assert state(journal_editor.JournalEditor(
        journal_path, cache_path=cache_path)) == uncached
    assert len(loads) == 2

    # Unchanged files: loaded from the cache.
    cached_editor = journal_editor.JournalEditor(journal_path,
                                                 cache_path=cache_path)
    assert len(loads) == 2
    assert state(cached_editor) == uncached
    assert cached_editor.check_any_journal_modification() == set()

    # A changed included file is loaded again.
    with open(included_path, 'w') as f:
        f.write('2015-03-01 * "Store"\n'
                '  Assets:Checking  -12 USD\n'
                '  Expenses:Food\n')
    st = os.stat(included_path)
    os.utime(included_path, (st.st_atime, st.st_mtime + 10))
    reloaded = journal_editor.JournalEditor(journal_path,
                                            cache_path=cache_path)
    assert len(loads) == 3
    assert '-12 USD' in state(reloaded)[0][-1]


def test_journal_cache_plugin_change(tmpdir, monkeypatch):
    # The cache is invalidated when a plugin module's source changes, even if
    # the plugin is only importable once the journal has been loaded.
    plugin_dir = tmpdir.mkdir('plugins')
    plugin_path = os.path.join(str(plugin_dir), 'my_test_plugin.py')
    with open(plugin_path, 'w') as f:
        f.write('__plugins__ = ["plugin"]\n'
                'def plugin(entries, options_map):\n'
                '    return entries, []\n')
    monkeypatch.syspath_prepend(str(plugin_dir))
    journal_path = os.path.join(str(tmpdir), 'journal.beancount')
    cache_path = os.path.join(str(tmpdir), 'cache.pickle')
    with open(journal_path, 'w') as f:
        f.write('plugin "my_test_plugin"\n'
                '2015-01-01 open Assets:Checking\n')

    loads = []
    real_load_file = journal_editor.load_file
    monkeypatch.setattr(journal_editor, 'load_file',
                        lambda *a, **kw: loads.append(a) or real_load_file(*a, **kw))
    journal_editor.JournalEditor(journal_path, cache_path=cache_path)
    journal_editor.JournalEditor(journal_path, cache_path=cache_path)
    assert len(loads) == 1

    st = os.stat(plugin_path)
    os.utime(plugin_path, (st.st_atime, st.st_mtime + 10))
    journal_editor.JournalEditor(journal_path, cache_path=cache_path)
    assert len(loads) == 2


def test_journal_cache_glob_include(tmpdir, monkeypatch):
    # A new file matched by a glob include changes no file the cache
    # recorded.
    statements = tmpdir.mkdir('statements')
    journal_path = os.path.join(str(tmpdir), 'journal.beancount')
    cache_path = os.path.join(str(tmpdir), 'cache.pickle')
    with open(journal_path, 'w') as f:
        f.write('include "statements/*.beancount"\n'
                '2015-01-01 open Assets:Checking\n'
                '2015-01-01 open Expenses:Food\n')
    statement = ('2015-03-0%d * "Store"\n'
                 '  Assets:Checking  -10 USD\n'
                 '  Expenses:Food\n')
    statements.join('a.beancount').write(statement % 1)
    first = journal_editor.JournalEditor(journal_path, cache_path=cache_path)
    assert len(first.entries) == 3

    statements.join('b.beancount').write(statement % 2)
    second = journal_editor.JournalEditor(journal_path, cache_path=cache_path)
    assert len(second.entries) == 4


def test_journal_cache_restores_all_attributes(tmpdir):
    # Everything a load sets must be cached (see _CACHED_ATTRS), or a cached
    # editor silently lacks it.
    journal_path = os.path.join(str(tmpdir), 'journal.beancount')
    ignored_path = os.path.join(str(tmpdir), 'ignored.beancount')
    cache_path = os.path.join(str(tmpdir), 'cache.pickle')
    with open(journal_path, 'w') as f:
        f.write('2015-01-01 open Assets:Checking\n')
    with open(ignored_path, 'w') as f:
        f.write('')
    for ignored in (None, ignored_path):
        loaded = journal_editor.JournalEditor(journal_path, ignored,
                                              cache_path=cache_path)
        cached = journal_editor.JournalEditor(journal_path, ignored,
                                              cache_path=cache_path)
        assert cached.journal_load_time == loaded.journal_load_time
        assert sorted(vars(cached)) == sorted(vars(loaded))
