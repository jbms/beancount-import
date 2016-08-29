"""Facilities for modifying a Beancount journal in-place.

An existing root Beancount journal (which may consist of multiple files due to
`include` directives) is loaded by creating an instance of the `JournalEditor`
class.

The `stage_changes()` method of `JournalEditor` returns a new, initially-empty
`StagedChanges` object which can be used to create a change set consisting of
additions of new directives, removals of existing directives, and modifications
of existing directives.

The staged changes can be converted to a unified-diff-like format, which can
then be display to the user (e.g. for confirmation) and/or applied to the
journal.

Compared to just rewriting a new journal from scratch, this method of making
changes retains the original organization, whitespace, comments, etc. of the
journal, and modifies only the lines occupied by the directives explicitly
included in the change set.
"""


from typing import Union, Dict, Tuple, List, Optional, Set
import re
import time
import io
import collections
import os
import tempfile
import datetime

from beancount.core.data import Open, Transaction, Balance, Commodity
import beancount.core.data
import beancount.loader
import beancount.parser.printer
import beancount.parser.booking
from beancount.core.number import MISSING


def ignore_errors(_):
    pass

def get_accounts_and_commodities(entries):
    """
    Preprocesses the entries of the journal.

    Returns:
      (accounts, commodities)
    """

    accounts = dict()
    commodities = dict()

    for entry in entries:
        if isinstance(entry, Open):
            if entry.account in accounts:
                raise RuntimeError('Duplicate open directive for account %s' % entry.account)
            accounts[entry.account] = entry
        elif isinstance(entry, Commodity):
            commodities[entry.currency] = entry
    return accounts, commodities

class JournalEditor(object):
    def __init__(self, journal_path, skip_booking=False):

        self.default_journal_load_time = time.time()
        self.journal_load_time = {}
        journal_path = os.path.realpath(journal_path)
        self.journal_path = journal_path

        if skip_booking:
            self.entries, self.errors, self.options_map = beancount.loader._parse_recursive(
                [(journal_path, True)], log_timings=None)
        else:
            self.entries, self.errors, self.options_map = beancount.loader.load_file(
                journal_path, log_errors=ignore_errors)

        self.cached_lines = {}
        self.accounts, self.commodities = get_accounts_and_commodities(self.entries)
        self.journal_filenames = set(os.path.realpath(x) for x in [journal_path] + self.options_map['include'])

    def get_journal_lines(self, filename):
        filename = os.path.realpath(filename)
        if filename in self.cached_lines:
            return (filename, self.cached_lines[filename])
        with open(filename, 'r') as f:
            lines = f.read().split('\n')
        self.cached_lines[filename] = lines
        return filename, lines

    def get_entry_line_range(self, entry):
        filename, lines = self.get_journal_lines(entry.meta['filename'])
        start_line = entry.meta['lineno'] - 1
        line_i = start_line + 1
        # Find last line of transaction
        # According to Beanacount grammer, each line of the entry must start with whitespace and contain a non-whitespace character.
        while line_i < len(lines):
            if not re.match(r'^\s+[^\s]', lines[line_i]):
                break
            line_i += 1
        return filename, lines, (start_line, line_i)

    def get_append_line_range(self, filename):
        filename, lines = self.get_journal_lines(filename)
        if len(lines) == 0 or lines[-1].strip():
            # If file is empty, start at 0.
            # If last line is not blank, start after last line.
            start_line = len(lines)
        else:
            # Last line is blank, so start on last line.
            start_line = len(lines) - 1
        return filename, lines, (start_line, start_line)

    def check_journal_modification(self, filename):
        mtime = os.stat(filename).st_mtime
        check_mtime = self.journal_load_time.get(filename, self.default_journal_load_time)
        return (mtime > check_mtime)

    def check_any_journal_modification(self):
        modified_filenames = set()
        for f in self.journal_filenames:
            if self.check_journal_modification(f):
                modified_filenames.add(f)
        return modified_filenames

    def apply_file_change_sets(self, filename, change_sets):
        """
        change_type < 0 means delete 1 line
        change_type == 0 means replace 1 line
        change_type > 0 means insert 1 line
        """
        _, old_lines = self.get_journal_lines(filename)
        new_lines = []
        next_old_lineno = 0
        next_new_lineno = 0
        lineno_map = dict()
        def fill_unchanged_lines(end_old_lineno):
            nonlocal next_new_lineno, next_old_lineno
            assert end_old_lineno <= len(old_lines) and end_old_lineno >= next_old_lineno
            new_lines.extend(old_lines[next_old_lineno:end_old_lineno])
            # print('Mapping old %d-%d to new %d-%d' %
            #       (next_old_lineno + 1, end_old_lineno + 1, next_new_lineno + 1,
            #        next_new_lineno + end_old_lineno - next_old_lineno + 1))
            for i in range(next_old_lineno, end_old_lineno):
                # +1 because beancount parser uses 1-based line numbers
                lineno_map[i+1] = next_new_lineno + 1
                next_new_lineno += 1
            next_old_lineno = end_old_lineno

        append_only = True

        for line_range, line_changes in change_sets:
            fill_unchanged_lines(line_range[0])

            if append_only:
                if line_range[0] < len(old_lines):
                    if line_range[0] != len(old_lines) - 1 or old_lines[-1].strip():
                        # If changes start either before the last line or on the non-empty last
                        # line, then they are not append-only.
                        append_only = False

            for change_type, line in line_changes:
                if change_type >= 0:
                    new_lines.append(line)
                if change_type < 0:
                    lineno_map[next_old_lineno + 1] = None
                    # print('Mapping %d -> None [deletion]' % (next_old_lineno + 1))
                if change_type == 0:
                    lineno_map[next_old_lineno + 1] = next_new_lineno + 1
                    # print('Mapping %d -> %d [unchanged]' % (next_old_lineno + 1, next_new_lineno + 1))
                if change_type <= 0:
                    next_old_lineno += 1
                if change_type >= 0:
                    next_new_lineno += 1
            assert next_old_lineno == line_range[1]

        fill_unchanged_lines(len(old_lines))

        new_data = '\n'.join(new_lines)


        # DEBUG
        # DEBUG
        # reverse_lineno_map = {v: k for k, v in lineno_map.items()}

        # for old_lineno, old_line in enumerate(old_lines):
        #     if lineno_map.get(old_lineno+1) is None:
        #         print('Unmapped old line %d: %s' % (old_lineno+1, old_line))
        # for new_lineno, new_line in enumerate(new_lines):
        #     if reverse_lineno_map.get(new_lineno+1) is None:
        #         print('Unmapped new line %d: %s' % (new_lineno+1, new_line))
        # with open('/tmp/old-lines.txt', 'w') as f:
        #     f.write('\n'.join(old_lines))

        # END DEBUG STUFF


        filename = os.path.realpath(filename)
        if self.check_journal_modification(filename):
            raise RuntimeError('Journal file modified concurrently: %r' % filename)

        with tempfile.NamedTemporaryFile(dir=os.path.dirname(filename),
                                         prefix='.' + os.path.basename(filename),
                                         mode='w+',
                                         suffix='.tmp',
                                         delete=False) as f:
            f.write(new_data)
            f.flush()
            self.journal_load_time[filename] = os.stat(f.name).st_mtime
            os.rename(f.name, filename)

        self.cached_lines[filename] = new_lines

        realpaths = dict()
        def get_realpath(path):
            result = realpaths.get(path, None)
            if result is None:
                result = os.path.realpath(path)
                realpaths[path] = result
            return result

        fixed_meta_ids = set()
        def fix_meta(meta):
            if meta is None:
                return
            if id(meta) in fixed_meta_ids:
                return
            # Multiple postings generated by booking to handle a reduction will share a metadata
            # dict.  We have to make sure to modify it only once.
            fixed_meta_ids.add(id(meta))
            entry_filename = meta.get('filename', None)
            if entry_filename is None:
                return
            if get_realpath(entry_filename) != filename:
                return
            lineno = meta.get('lineno', None)
            # Automatic Document entries get a lineno of 0
            if lineno is None or lineno == 0:
                return
            meta['lineno'] = lineno_map[lineno]

        # Update lines of all entries
        if not append_only:
            for entry in self.entries:
                fix_meta(entry.meta)
                if isinstance(entry, Transaction):
                    for posting in entry.postings:
                        fix_meta(posting.meta)

    def apply_change_sets(self, change_sets):
        for filename, file_change_sets in change_sets:
            self.apply_file_change_sets(filename, file_change_sets)

    def apply_staged_changes(self, staged_changes):
        change_sets, old_entries, new_entries = staged_changes.get_diff()
        self.apply_change_sets(change_sets)
        old_entries_set = set(map(id, old_entries))
        self.entries = [e for e in self.entries if id(e) not in old_entries_set
                        and e.meta.get('lineno') is not None]
        booked_new_entries, balance_errors = beancount.parser.booking.book(new_entries, self.options_map)
        self.entries.extend(booked_new_entries)
        self.entries.sort(key=beancount.core.data.entry_sortkey)
        for entry in booked_new_entries:
            if isinstance(entry, Open):
                self.accounts[entry.account] = entry
            if isinstance(entry, Commodity):
                self.commodities[entry.currency] = entry

        return old_entries, booked_new_entries

    def stage_changes(self):
        return StagedChanges(self)

class FileChangeSetsBuilder(object):
    def __init__(self, filename, lines):
        self.filename = os.path.realpath(filename)
        self.lines = lines
        self.line_delta = 0
        self.builders = []

    def add_builder(self, line_range):
        line_delta = 0
        if self.builders:
            last_builder = self.builders[-1]
            line_delta = last_builder.new_lineno - last_builder.orig_lineno
        builder = LineChangeBuilder(filename=self.filename, lines=self.lines, line_range=line_range, line_delta=line_delta)
        self.builders.append(builder)
        return builder

    @property
    def change_sets(self):
        return (self.filename, [x.change_set for x in self.builders])

class LineChangeBuilder(object):
    def __init__(self, filename, lines, line_range, line_delta):
        self.filename = filename
        self.lines = lines
        self.line_range = line_range
        self.changes = []
        self.orig_lineno = line_range[0]
        self.new_lineno = line_range[0] + line_delta

    def set_metadata(self, x, lineno=None):
        meta = dict({}, **(x.meta or {}))
        meta['filename'] = self.filename
        if lineno is None:
            lineno = self.new_lineno + 1
        meta['lineno'] = lineno
        return x._replace(meta=meta)

    def keep_line(self):
        self.replace_line(self.lines[self.orig_lineno])

    @property
    def cur_orig_line(self):
        orig_lineno = self.orig_lineno
        lines = self.lines
        if orig_lineno < len(lines):
            return lines[orig_lineno]
        return None

    def replace_line(self, new_line):
        old_line = self.lines[self.orig_lineno]
        self.orig_lineno += 1
        self.new_lineno += 1
        if old_line == new_line:
            self.changes.append((0, new_line))
        else:
            self.changes.append((-1, old_line))
            self.changes.append((1, new_line))

    def add_lines(self, new_lines):
        if isinstance(new_lines, str):
            new_lines = [new_lines]
        self.new_lineno += len(new_lines)
        self.changes.extend((1, new_line) for new_line in new_lines)

    def delete_line(self, count=1):
        new_orig_lineno = self.orig_lineno + count
        assert count >= 0
        assert new_orig_lineno <= len(self.lines)
        self.changes.extend((-1, line) for line in self.lines[self.orig_lineno:new_orig_lineno])
        self.orig_lineno = new_orig_lineno

    def delete_until(self, final_orig_lineno):
        self.delete_line(final_orig_lineno - self.orig_lineno)

    def ensure_blank_line(self):
        if self.orig_lineno > 0 and self.lines[self.orig_lineno-1].strip():
            self.add_lines('')

    def match_metadata(self, meta):
        assert os.path.realpath(meta['filename']) == self.filename
        assert meta['lineno'] == self.orig_lineno + 1

    def raise_error(self, message):
        raise RuntimeError('%s:%d: %s' % (self.filename, self.orig_lineno, message))

    @property
    def change_set(self):
        return (self.line_range, self.changes)

def get_meta_ignore():
    printer = beancount.parser.printer.EntryPrinter()
    meta_ignore = set(printer.META_IGNORE)
    meta_ignore.add('__tolerances__')
    return frozenset(meta_ignore)
META_IGNORE = get_meta_ignore()

metadata_line_re = '^ +([a-z][a-zA-Z0-9\\-_]*) *: *([^a-zA-Z].*)$'
def compute_metadata_changes(builder, old_meta, new_meta, indent):
    printer = beancount.parser.printer.EntryPrinter()
    def format_metadata_line(key):
        oss = io.StringIO()
        printer.write_metadata({key: new_meta[key]}, oss, prefix=' ' * indent)
        return oss.getvalue().rstrip()

    old_meta_not_seen = set(old_meta.keys())
    old_meta_not_seen.difference_update(META_IGNORE)
    while True:
        line = builder.cur_orig_line
        if line is None:
            break
        m = re.fullmatch(metadata_line_re, line)
        if m is None:
            break
        key = m.group(1)
        if key not in old_meta_not_seen:
            builder.raise_error('unexpected metadata key %r found' % (key,))
        old_meta_not_seen.remove(key)
        if key not in new_meta:
            builder.delete_line()
        elif new_meta[key] != old_meta[key]:
            builder.replace_line(format_metadata_line(key))
        else:
            builder.keep_line()
    if old_meta_not_seen:
        builder.raise_error('expected metadata keys %r' % (old_meta_not_seen,))
    for key in new_meta:
        if key in META_IGNORE or key in old_meta:
            continue
        builder.add_lines(format_metadata_line(key))
def get_posting_line(posting):
    printer = beancount.parser.printer.EntryPrinter()
    flag_account, position_str, _ = printer.render_posting_strings(posting)
    return ('  %s  %s' % (flag_account, position_str)).rstrip()

def compute_posting_changes(builder, old_posting, new_posting):
    builder.match_metadata(old_posting.meta)
    old_posting_line = get_posting_line(old_posting)
    new_posting_line = get_posting_line(new_posting)
    if old_posting_line == new_posting_line:
        builder.keep_line()
    else:
        builder.replace_line(new_posting_line)
    compute_metadata_changes(builder=builder,
                             old_meta=old_posting.meta,
                             new_meta=new_posting.meta,
                             indent=4)

class StagedChanges(object):
    def __init__(self, journal_editor):
        self.journal_editor = journal_editor
        self.changed_entries = collections.OrderedDict()
        self._cached_diff = None

    def add_entry(self, new_entry, output_filename):
        self.changed_entries.setdefault(os.path.realpath(output_filename), []).append((None, new_entry))
        self._cached_diff = None

    def remove_entry(self, old_entry):
        if 'filename' not in old_entry.meta or 'lineno' not in old_entry.meta:
            raise ValueError('Cannot remove entry without filename and line')
        self.changed_entries.setdefault(os.path.realpath(old_entry.meta['filename']), []).append((old_entry, None))
        self._cached_diff = None

    def change_entry(self, old_entry, new_entry):
        if not isinstance(old_entry, Transaction) or not isinstance(new_entry, Transaction):
            raise NotImplementedError('only Transaction entries supported')
        self.changed_entries.setdefault(os.path.realpath(old_entry.meta['filename']), []).append((old_entry, new_entry))
        self._cached_diff = None

    def get_all_new_entries(self):
        """Returns a sequence of the new entries WITHOUT updated line numbers."""
        return [new_entry for _, changed_entries in self.changed_entries.items() for _, new_entry in changed_entries]

    def get_all_accounts(self, account_map: Optional[Dict[str, str]] = None
                         ) -> Tuple[Dict[str, Tuple[datetime.date, Set[str]]], Set[str]]:
        """Returns an account -> (date, currencies) dict, as well as the set of accounts listed in open directives."""
        def map_account(account: str) -> str:
            if account_map is None:
                return account
            return account_map.get(account, account)
        accounts = collections.OrderedDict() # type: Dict[str, Tuple[datetime.date, Set[str]]]
        open_accounts = set()
        def add_account(account, date, currencies):
            if currencies is None:
                currencies = set()
            existing_date, existing_currencies = accounts.setdefault(account, (date, set(currencies)))
            existing_currencies.update(currencies)
            if existing_date > date:
                existing_date = date
            accounts[account] = (existing_date, existing_currencies)
        for entry in self.get_all_new_entries():
            if isinstance(entry, Open):
                account = map_account(entry.account)
                open_accounts.add(account)
                add_account(account, entry.date, entry.currencies)
            elif isinstance(entry, Balance):
                add_account(map_account(entry.account), entry.date, [entry.amount.currency])
            elif isinstance(entry, Transaction):
                other_currencies = set()
                for posting in entry.postings:
                    if posting.price is not None and posting.price is not MISSING:
                        other_currencies.add(posting.price.currency)
                    elif posting.cost is not None and posting.cost.currency is not None:
                        other_currencies.add(posting.cost.currency)
                    elif posting.units is not None and posting.units is not MISSING:
                        other_currencies.add(posting.units.currency)

                for posting in entry.postings:
                    if posting.units is None or posting.units is MISSING:
                        if len(other_currencies) == 1:
                            currencies = other_currencies
                        else:
                            currencies = []
                    else:
                        currencies = [posting.units.currency]
                    add_account(map_account(posting.account), entry.date, currencies)
        return accounts, open_accounts

    def get_missing_accounts(self, account_map: Optional[Dict[str, str]]=None):
        referenced_accounts, open_accounts = self.get_all_accounts(account_map)
        accounts = self.journal_editor.accounts
        return [
            (account, date, currencies)
            for account,
            (date, currencies) in referenced_accounts.items()
            if account not in accounts and account not in open_accounts
        ]

    def get_diff(self):
        if self._cached_diff is not None:
            return self._cached_diff

        change_sets = []
        new_entries = []
        old_entries = []

        printer = beancount.parser.printer.EntryPrinter()

        for filename, changed_entries in self.changed_entries.items():
            changed_entries.sort(key=lambda x: float('inf') if x[0] is None else x[0].meta['lineno'])
            _, lines = self.journal_editor.get_journal_lines(filename)
            change_sets_builder = FileChangeSetsBuilder(filename=filename, lines=lines)
            for old_entry, new_entry in changed_entries:
                if new_entry is None:
                    # Remove entry
                    _, _, line_range = self.journal_editor.get_entry_line_range(old_entry)
                    start_line = line_range[0]

                    # Move start line up until non-whitespace line
                    while start_line > 0 and not lines[start_line-1].strip():
                        start_line -= 1

                    builder = change_sets_builder.add_builder((start_line, line_range[1]))
                    builder.delete_until(line_range[1])
                    old_entries.append(old_entry)
                elif old_entry is None:
                    # Add new entry

                    _, _, line_range = self.journal_editor.get_append_line_range(filename)
                    builder = change_sets_builder.add_builder(line_range)
                    builder.ensure_blank_line()
                    new_entry = builder.set_metadata(new_entry)
                    if isinstance(new_entry, Transaction):
                        new_postings = []
                        cur_lineno = new_entry.meta['lineno'] + 1 + len(new_entry.meta.keys() - META_IGNORE)
                        new_postings = []
                        for posting in new_entry.postings:
                            posting = builder.set_metadata(posting, cur_lineno)
                            cur_lineno += 1 + len(posting.meta.keys() - META_IGNORE)
                            new_postings.append(posting)
                        new_entry = new_entry._replace(postings=new_postings)
                    new_entries.append(new_entry)
                    added_lines = printer(new_entry).strip('\n').split('\n')
                    added_lines = [x.rstrip() for x in added_lines]
                    builder.add_lines(added_lines)

                else:
                    # Change entry

                    _, _, line_range = self.journal_editor.get_entry_line_range(old_entry)
                    builder = change_sets_builder.add_builder(line_range)
                    old_print_result = printer(old_entry).split('\n')
                    new_print_result = printer(new_entry).split('\n')
                    new_entry = builder.set_metadata(new_entry)
                    if old_print_result[0] == new_print_result[0]:
                        # First line is the same
                        builder.keep_line()
                    else:
                        builder.replace_line(new_print_result[0].rstrip())
                    compute_metadata_changes(builder=builder,
                                             old_meta=old_entry.meta,
                                             new_meta=new_entry.meta,
                                             indent=2)
                    assert isinstance(old_entry, Transaction) == isinstance(new_entry, Transaction)
                    if isinstance(old_entry, Transaction):
                        # handle postings
                        next_old_posting_i = 0
                        new_postings = []
                        for new_posting in new_entry.postings:
                            new_posting = builder.set_metadata(new_posting)
                            new_postings.append(new_posting)
                            if next_old_posting_i < len(old_entry.postings):
                                old_posting = old_entry.postings[next_old_posting_i]
                                # posting replaces old_posting
                                compute_posting_changes(builder=builder,
                                                        old_posting=old_posting,
                                                        new_posting=new_posting)
                                next_old_posting_i += 1
                            else:
                                new_posting_lines = []
                                new_posting_lines.append(get_posting_line(new_posting))
                                oss = io.StringIO()
                                printer.write_metadata(new_posting.meta, oss, prefix = '    ')
                                metadata_text = oss.getvalue().strip('\n')
                                if metadata_text:
                                    new_posting_lines.extend(metadata_text.split('\n'))
                                builder.add_lines(new_posting_lines)
                        # Delete all remaining old postings
                        builder.delete_until(builder.line_range[1])

                        # Replace list of postings with postings containing updated filename and lineno
                        # information.
                        new_entry = new_entry._replace(postings=new_postings)
                    old_entries.append(old_entry)
                    new_entries.append(new_entry)
            change_sets.append(change_sets_builder.change_sets)
        self._cached_diff = change_sets, old_entries, new_entries
        return self._cached_diff
    def apply(self):
        return self.journal_editor.apply_staged_changes(self)

    def get_combined_changes(self):
        combined_changes = []
        for _, change_sets in self.get_diff()[0]:
            for _, line_changes in change_sets:
                combined_changes.extend(line_changes)
        return combined_changes
