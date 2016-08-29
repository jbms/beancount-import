import pytest
import py
import datetime

from . import journal_editor
import beancount.parser.printer
from beancount.core.data import Transaction, Posting
from beancount.core.number import MISSING
from beancount.core.amount import Amount
from decimal import Decimal

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
            postings=list(map(clean_posting, entry.postings)),
            tags=entry.tags or None,
            links=entry.links or None)
    return entry


def clean_entries(entries):
    return list(map(clean_entry, entries))


def check_file_contents(path, expected_contents):
    with open(path, 'r') as f:
        contents = f.read()
    assert contents == expected_contents


def check_journal_entries(editor):
    assert clean_entries(editor.entries) == clean_entries(
        journal_editor.JournalEditor(editor.journal_path).entries)


def create_journal(tmpdir: py.path.local, contents: str, name: str = 'journal.beancount'):
    path = tmpdir.join(name)
    path.write(contents)
    return str(path)


def test_simple(tmpdir):
    journal_path = create_journal(tmpdir, """
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
    old_entries, new_entries = stage.apply()
    assert old_entries == [old_entry]
    check_file_contents(
        journal_path, """
2015-02-01 * "Test transaction"
  Assets:Account-A  100 USD
    note: "Hello"
  Assets:Account-B
""")
    check_journal_entries(editor)


def test_transaction_add_meta(tmpdir):
    journal_path = create_journal(tmpdir, """
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
    old_entries, new_entries = stage.apply()
    assert old_entries == [old_entry]
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
    journal_path = create_journal(tmpdir, """
2015-02-01 * "Test transaction"
  Assets:Account-A  100 USD
  Assets:Account-B
""")
    editor = journal_editor.JournalEditor(journal_path)
    stage = editor.stage_changes()
    old_entry = editor.entries[0]
    new_entry = old_entry._replace(narration='Modified transaction')
    stage.change_entry(old_entry, new_entry)
    old_entries, new_entries = stage.apply()
    assert old_entries == [old_entry]
    check_file_contents(
        journal_path, """
2015-02-01 * "Modified transaction"
  Assets:Account-A  100 USD
  Assets:Account-B
""")
    check_journal_entries(editor)

def test_two(tmpdir):
    journal_path = create_journal(tmpdir, """
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
    old_entries, new_entries = stage.apply()
    assert old_entries == [old_entry]
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
    journal_path = create_journal(tmpdir, """
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

    old_entries, new_entries = stage.apply()
    assert old_entries == [old_entry, old_entry2]
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
    journal_path = create_journal(tmpdir, """
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
    old_entries, new_entries = stage.apply()
    assert old_entries == [old_entry]
    assert new_entries == []
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
    journal_path = create_journal(tmpdir, """
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
        tags=None,
        links=None,
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
    old_entries, new_entries = stage.apply()
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
