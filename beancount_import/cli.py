#!/usr/bin/env python3

import pdb
import re
import sys, csv, argparse, os
import concurrent.futures
import collections
import datetime
import tempfile
import json
import nltk, sklearn.tree

import npyscreen
import npyscreen.wgwidget
import npyscreen.wgtitlefield

import curses
import curses.ascii
import threading
import time
import subprocess
import logging
from decimal import Decimal

import beancount.parser.printer
import beancount.loader
import beancount.parser.booking
from beancount.core.data import Open, Transaction

logger = logging.getLogger('beancount-import')

source_data_key = ['source_data'] + ['source_data%d' % x for x in range(1,3)]
posting_date_key = 'date'
journal_date_format = '%Y-%m-%d'

def get_posting_date(entry, posting):
    if posting_date_key in posting.meta:
        return posting.meta[posting_date_key]
    return entry.date

MintEntry = collections.namedtuple('MintEntry', ['account', 'date', 'amount', 'source_data'])

class JournalState(object):
    def __init__(self, args, log_status):
        self.args = args

        log_status('Parsing journal')
        def ignore_errors(x):
            pass

        self.default_journal_load_time = time.time()
        self.journal_load_time = {}

        self.entries, errors, self.options = beancount.loader.load_file(
            args.journal_input, log_errors = ignore_errors)

        if len(errors) > 0:
            raise RuntimeError('Errors detected, run bean-check')

        self.fuzzy_match_days = args.fuzzy_match_days

        self.training_examples = []
        self.classifier = None

        self.cached_lines = {}

        self._process_accounts()

        log_status('Processing entries')
        self._process_entries()

    def get_filename_for_account(self, account_name):
        if self.args.account_output is not None:
            for pattern, filename in self.args.account_output:
                if re.match(pattern, account_name):
                    return filename
        return self.args.journal_output

    def _process_accounts(self):
        """Populates the bidirectional mappings self.mint_id_to_account and self.account_to_mint_id
        based on the mint_id account metadata field.
        """
        self.mint_id_to_account = {}
        self.account_to_mint_id = {}
        self.all_accounts = set()

        for entry in self.entries:
            if not isinstance(entry, Open):
                continue
            self.all_accounts.add(entry.account)
            mint_id = entry.meta.get('mint_id',None)
            if mint_id is None:
                continue
            if not isinstance(mint_id, str):
                raise RuntimeError('Invalid mint_id (not string): %r' % mint_id)
            old_mint_id = self.account_to_mint_id.get(entry.account,None)
            old_account = self.mint_id_to_account.get(mint_id, None)
            if old_mint_id != None and old_mint_id != mint_id:
                raise RuntimeError('Duplicate mappings for account %r: %r'
                                   % (account, [old_mint_id, mint_id]))
            if old_account != None and old_account != entry.account:
                raise RuntimeError('Duplicate mappings for mint_id %r: %r'
                                   % (mint_id, [old_account, entry.account]))
            self.account_to_mint_id[entry.account] = mint_id
            self.mint_id_to_account[mint_id] = entry.account

    def _process_entries(self):
        """Initializes self.{matched,unmatched}_postings.
        """
        # Dict of (account, amount, date +- args.fuzzy_match_days) -> { id(posting) -> (entry, posting) }
        self.unmatched_postings = {}

        # Multiset of MintEntry

        # This contains postings that have (source_data, posting_date) and have not yet been matched against the downloaded data during this session.
        self.matched_postings = {}
        journal_filenames = set()

        for entry in self.entries:
            if not isinstance(entry, Transaction): continue

            journal_filenames.add(entry.meta['filename'])
            for posting_i, posting in enumerate(entry.postings):

                # Only consider postings into accounts tracked by Mint.
                if posting.account not in self.account_to_mint_id:
                    continue

                if posting.meta is None:
                    continue

                # If there is already source_data, add to matched_postings.
                posting_date = posting.meta.get(posting_date_key, None)
                matched = False
                # To handle duplicate downloaded entries (hopefully rare), we allow multiple source_data values per posting.
                for sd_key in source_data_key:
                    source_data = posting.meta.get(sd_key, None)
                    if source_data is not None:
                        if posting_date is None:
                            raise RuntimeError('Posting date is missing on entry: %r' % entry)
                        self.add_matched_posting(entry, posting, source_data, posting_date)
                        matched = True

                # Since there is no source_data, add to unmatched_postings.
                if not matched:
                    self.add_unmatched_posting(entry, posting)
        journal_filenames.add(self.args.journal_input)
        self.journal_filenames = set(os.path.realpath(f) for f in journal_filenames)

    def add_matched_posting(self, entry, posting, source_data, posting_date):
        self.matched_postings.setdefault(
            MintEntry(account = posting.account, date = posting_date,
                      amount = posting.position.number, source_data = source_data),
            []).append((entry, posting))

        # If there are exactly 2 postings, use this as training data.
        if len(entry.postings) == 2:
            posting_i = 0 if posting is entry.postings[0] else 1
            self.add_training_example(posting.account,
                                      source_data,
                                      posting.position.number,
                                      entry.postings[1 - posting_i].account)

    def add_training_example(self, source_account, source_data, amount, target_account):
        self.training_examples.append((self.get_features(source_account, source_data, amount), target_account))

    def get_features(self, source_account, source_data, amount):
        features = collections.defaultdict(lambda: False)
        features['source_account=%s' % source_account] = True

        # For now, skip amount.

        csv_desc = 'csv-desc:'
        if source_data.startswith(csv_desc):
            source_data = source_data[len(csv_desc):]
        else:
            raise RuntimeError('Unsupported source_data format: %r' % source_data)

        words = []
        for w in source_data.split():
            w = w.strip('-.').lower()
            if len(w) > 0:
                words.append(w)
        for start_i in range(len(words)-1):
            for end_i in range(start_i, len(words)):
                features['phrase:%s' % ' '.join(words[start_i:end_i])] = True
        return features

    def predict_target_account(self, source_account, source_data, amount):
        return self.classifier.classify(self.get_features(source_account, source_data, amount))

    def get_fuzzy_date_range(self, orig_date):
        for day_offset in range(-self.fuzzy_match_days, self.fuzzy_match_days+1):
            yield orig_date + datetime.timedelta(days = day_offset)

    def get_unmatched_posting_group_key(self, entry, posting):
        # If there is a posting date, only allow exact date matches.
        if 'date' in posting.meta:
            date_range = [posting.meta['date']]
        # Otherwise perform fuzzy matching based on the transaction date.
        else:
            date_range = self.get_fuzzy_date_range(entry.date)

        for date in date_range:
            yield (posting.account, date, posting.position.number)

    def add_unmatched_posting(self, entry, posting):
        # FIXME: Check that posting.position.lot matches the currency that we are tracking
        for group_key in self.get_unmatched_posting_group_key(entry, posting):
            group = self.unmatched_postings.setdefault(group_key, {})
            group[id(posting)] = (entry, posting)

    def remove_unmatched_posting(self, entry, posting):
        for group_key in self.get_unmatched_posting_group_key(entry, posting):
            group = self.unmatched_postings.get(group_key, None)
            if group is not None:
                del group[id(posting)]

    def get_matches(self, account, date, source_data, amount):
        matches = {} # id(posting) -> (entry, posting)
        for match_date in self.get_fuzzy_date_range(date):
            key = (account, match_date, amount)
            matches.update(self.unmatched_postings.get(key, {}))
        matches = [v for k,v in matches.items()] # list of (entry, posting)
        # Sort by date
        matches.sort(key = lambda x: get_posting_date(x[0], x[1]))
        return matches

    def get_journal_lines(self, filename):
        filename = os.path.realpath(filename)
        if filename in self.cached_lines:
            return self.cached_lines[filename]
        with open(filename, 'r') as f:
            lines = f.read().split('\n')
        self.cached_lines[filename] = lines
        return lines

    def get_transaction_line_range(self, entry):
        lines = self.get_journal_lines(entry.meta['filename'])
        start_line = entry.meta['lineno'] - 1
        line_i = start_line + 1
        # Find last line of transaction
        # According to Beanacount grammer, each line of the transaction must start with whitespace and contain a non-whitespace character.
        while line_i < len(lines):
            if not re.match('^\s+[^\s]', lines[line_i]):
                break
            line_i += 1
        return (start_line, line_i)

    def check_journal_modification(self, filename):
        mtime = os.stat(filename).st_mtime
        check_mtime = self.journal_load_time.get(filename, self.default_journal_load_time)
        return (mtime > check_mtime)

    def check_any_journal_modification(self):
        for f in self.journal_filenames:
            if self.check_journal_modification(f):
                return True
        return False

    def apply_changes(self, filename, line_range, line_changes):
        orig_lines = self.get_journal_lines(filename)
        new_lines = orig_lines[:line_range[0]]
        # +1 because beancount parser uses 1-based line numbers
        lineno_map = {i+1: i+1 for i in range(0, line_range[0])}
        next_new_lineno = line_range[0] + 1
        next_old_lineno = line_range[0] + 1
        for change_type, line in line_changes:
            new_lines.append(line)
            if change_type == 0:
                lineno_map[next_old_lineno] = next_new_lineno
            if change_type <= 0:
                next_old_lineno += 1
            if change_type >= 0:
                next_new_lineno += 1

        new_lines += orig_lines[line_range[1]:]
        while next_new_lineno <= len(new_lines):
            lineno_map[next_old_lineno] = next_new_lineno
            next_old_lineno += 1
            next_new_lineno += 1

        new_data = '\n'.join(new_lines)


        filename = os.path.realpath(filename)
        if self.check_journal_modification(filename):
            raise RuntimeError('Journal file modified concurrently: %r' % filename)

        with tempfile.NamedTemporaryFile(dir = os.path.dirname(filename),
                                         prefix = '.' + os.path.basename(filename),
                                         mode = 'w+',
                                         suffix = '.tmp',
                                         delete = False) as f:
            f.write(new_data)
            f.flush()
            self.journal_load_time[filename] = os.stat(f.name).st_mtime
            os.rename(f.name, filename)

        self.cached_lines[filename] = new_lines

        # Update lines of all entries
        for entry in self.entries:
            if entry.meta is not None and 'lineno' in entry.meta:
                entry.meta['lineno'] = lineno_map[entry.meta['lineno']]
            if isinstance(entry, Transaction):
                for posting in entry.postings:
                    if posting.meta is not None and 'lineno' in posting.meta:
                        posting.meta['lineno'] = lineno_map[posting.meta['lineno']]
    def append_lines(self, lines, output_filename=None, separate_with_blank_line=True):
        if output_filename is None:
            output_filename = self.args.journal_output
        # Avoid mutating original
        lines = list(lines)
        filename = os.path.realpath(output_filename)
        orig_lines = self.get_journal_lines(filename)
        if self.check_journal_modification(filename):
            raise RuntimeError('Journal file modified concurrently: %r' % filename)
        base_lineno = len(orig_lines)
        if orig_lines[-1] != '':
            lines.insert(0, '')
            base_lineno += 1
        if separate_with_blank_line:
            if len(orig_lines) == 1 or orig_lines[-2] != '':
                lines.insert(0, '')
                base_lineno += 1

        lines.append('')

        with open(filename, 'a') as f:
            f.write('\n'.join(lines))

        if len(orig_lines) > 0:
            orig_lines[-1] += lines[0]
            orig_lines.extend(lines[1:])
        else:
            orig_lines.extend(lines)

        # Check that orig lines matches the file
        with open(filename, 'r') as f:
            new_lines = f.read().split('\n')
        assert(orig_lines == new_lines)

        self.journal_load_time[filename] = os.stat(filename).st_mtime
        return filename, base_lineno

class MatchCandidate(object):
    def __init__(self, entry, posting, source_data, date, state):
        self.entry = entry
        self.posting = posting
        self.state = state
        self.source_data = source_data
        self.posting_date = date

        lines = state.get_journal_lines(posting.meta['filename'])
        line_range = state.get_transaction_line_range(entry)
        self.line_range = line_range
        self.line_changes = [(0, line) for line in lines[line_range[0]:line_range[1]]]

        insert_offset = posting.meta['lineno'] - entry.meta['lineno'] + 1
        indent = ' ' * 4

        # Insert the date first so that it comes after
        if posting_date_key not in posting.meta:
            self.line_changes.insert(
                insert_offset, (1, '%s%s: %s' % (indent, posting_date_key, date.strftime(journal_date_format))))

        # Json string literal representation (approximately) matches beancount syntax
        self.line_changes.insert(
            insert_offset,
            (1, '%s%s: %s' % (indent, source_data_key[0], json.dumps(source_data))))

    def apply(self):
        self.state.apply_changes(self.posting.meta['filename'], self.line_range, self.line_changes)
        self.state.remove_unmatched_posting(self.entry, self.posting)

        # We don't increment the count in matched_postings because this posting has already been matched against downloaded data during this session.
        self.state.add_matched_posting(entry = self.entry,
                                       posting = self.posting,
                                       source_data = self.source_data,
                                       posting_date = self.posting_date)
        return (self.entry, self.posting)

def get_narration_from_source_data(source_data):
    csv_desc = 'csv-desc:'
    if source_data.startswith(csv_desc):
        return source_data[len(csv_desc):]
    else:
        raise RuntimeError('Unsupported source_data format: %r' % source_data)

class NewCandidate(object):
    def __init__(self, entry, target_account, state):
        self.entry = entry
        self.state = state
        self.target_account = target_account

        self.line_changes_actual = [
            (0, '%s * %s' % (entry.date.strftime(journal_date_format),
                             json.dumps(get_narration_from_source_data(entry.source_data)))),
            (0, '  %s    %.2f USD' % (entry.account, entry.amount)),
            (0, '    %s: %s' % (source_data_key[0], json.dumps(entry.source_data))),
            (0, '    %s: %s' % (posting_date_key, entry.date.strftime(journal_date_format))),
            (1, '  %s' % (target_account))
            ]
        if self.target_account not in self.state.all_accounts:
            self.open_account_line = '1900-01-01 open %s' % self.target_account
            self.line_changes = self.line_changes_actual + [(1, self.open_account_line)]
        else:
            self.line_changes = self.line_changes_actual
            self.open_account_line = None
    def apply(self):
        lines = [line for change_type, line in self.line_changes_actual]
        filename, base_lineno = self.state.append_lines(lines)
        if self.open_account_line is not None:
            self.state.append_lines([self.open_account_line],
                                    output_filename=self.state.get_filename_for_account(self.target_account),
                                    separate_with_blank_line=False)
        entry_text = '\n'.join(lines) + '\n'
        def ignore_errors(x):
            pass
        postings = []
        entries, errors, options = beancount.parser.parser.parse_string(entry_text)
        entries, balance_errors = beancount.parser.booking.book(entries, self.state.options)
        if len(entries) != 1:
            raise RuntimeError('Error parsing new transaction text: %r\n%r' % (entry_text, errors))
        entry = entries[0]
        entry.meta['filename'] = filename
        entry.meta['lineno'] += base_lineno
        for posting in entry.postings:
            posting.meta['filename'] = filename
            posting.meta['lineno'] += base_lineno
        self.state.add_unmatched_posting(entry, entry.postings[1])

        # We don't increment the count in matched_postings because this posting has already been matched against downloaded data during this session.
        self.state.add_matched_posting(entry = entry,
                                       posting = entry.postings[0],
                                       source_data = self.entry.source_data,
                                       posting_date = self.entry.date)

        return (entry, entry.postings[0])

def load_mint_data(filename, state):
    expected_field_names = [
        'Date', 'Description', 'Original Description', 'Amount',
        'Transaction Type', 'Category', 'Account Name', 'Labels', 'Notes']
    mint_date_format = '%m/%d/%Y'

    account_map = state.mint_id_to_account

    try:
        entries = []
        with open(filename, 'r', newline = '') as csvfile:
            reader = csv.DictReader(csvfile)
            if reader.fieldnames != expected_field_names:
                raise RuntimeError('Actual field names %r != expected field names %r'
                                   % (reader.fieldnames, expected_field_names))
            for row in reader:
                account = row['Account Name']
                if account not in account_map:
                    #raise RuntimeError('Unknown account name: %r in row %r' % (account,row))
                    continue
                if row['Transaction Type'] not in ['credit', 'debit']:
                    raise('Unknown transaction type: %r in row %r' % (row['Transaction Type'], row))
                amount = Decimal(row['Amount'])
                if row['Transaction Type'] == 'debit':
                    amount = -amount

                try:
                    date = datetime.datetime.strptime(row['Date'], mint_date_format).date()
                except Exception as e:
                    raise RuntimeError('Invalid date: %r' % row['Date']) from e

                source_data = 'csv-desc:' + row['Original Description']

                source_account = account_map[account]
                entries.append(MintEntry(account = source_account, date = date,
                                         source_data = source_data, amount = amount))
        entries.reverse()
        entries.sort(key = lambda x: x[1]) # sort by date
        return entries

    except Exception as e:
        raise RuntimeError('CSV file has incorrect format',filename) from e


class ProcessState(object):
    def __init__(self, args, log_status):
        self.args = args
        self.log_status = log_status
        self.state = JournalState(self.args, log_status)

        log_status('Parsing CSV file %r' % args.mint_data)
        self.imported_data = load_mint_data(args.mint_data, self.state)

        log_status('Matching entries')

        matched_postings = collections.Counter()
        for mint_entry, postings in self.state.matched_postings.items():
            matched_postings[mint_entry] += len(postings)

        self.pending_data = []
        for e in self.imported_data:
            if matched_postings[e] > 0:
                matched_postings[e] -= 1
            elif re.fullmatch(args.limit, e.account):
                self.pending_data.append(e)

        # Find mint entries referenced in the journal that are not present in the imported date.
        has_stale = False
        stale_entries = []
        for mint_entry, postings in self.state.matched_postings.items():
            if matched_postings[mint_entry]:
                for entry, posting in postings:
                    stale_entries.append(
                        '%s:%d: Stale entry: %s %s' %
                        (posting.meta['filename'], posting.meta['lineno'],
                         mint_entry.date.strftime(journal_date_format), mint_entry.source_data))
        if stale_entries:
            raise RuntimeError('Stale entries found\n' + '\n'.join(stale_entries))
class ActionWidget(npyscreen.wgwidget.Widget):
    def __init__(self, *args, **kwargs):
        super(ActionWidget, self).__init__(*args, **kwargs)
        self._is_editable = False

    def update(self, clear=True):
        if clear: self.clear()
        if self.hidden:
            self.clear()
            return True

        for line_i, (line_type, line) in enumerate(self.value.line_changes):
            if self.highlight and line_i == 0:
                attributes = [
                    curses.color_pair(self.parent.theme_manager.get_pair_number('BLACK_WHITE')),
                    curses.color_pair(self.parent.theme_manager.get_pair_number('BLACK_WHITE')) | curses.A_BOLD,
                ]
            else:
                attributes = [
                    curses.color_pair(self.parent.theme_manager.get_pair_number('WHITE_BLACK')),
                    curses.color_pair(self.parent.theme_manager.get_pair_number('WHITE_BLACK')) | curses.A_BOLD,
                  ]

            # line_type: 0 -> existing, 1 -> insertion
            self.parent.curses_pad.addstr(self.rely+line_i, self.relx, line, attributes[line_type])

class ActionList(npyscreen.MultiLineAction):
    def __init__(self, *args, **kwargs):
        self.is_showing_completions = None
        self.update_display_type(True, refresh = False)

        super(ActionList, self).__init__(*args, allow_filtering = False, **kwargs)

        self.values = []
        self.add_handlers({
            "s": self.skip_entry,
            "t": self.retrain,
            "a": self.modify_account,
            "e": self.h_act_on_highlighted,
        })
    def update_display_type(self, is_showing_completions, refresh = None):
        if is_showing_completions:
            if self.is_showing_completions != is_showing_completions:
                self._contained_widgets = npyscreen.Textfield
                self._contained_widget_height = 1
                if refresh != False:
                    self.resize()
        else:
            app = self.parent.parentApp
            self.values = app.current_matches
            if self.values is not None and len(self.values) > 0:
                height = max(len(x.line_changes) for x in self.values) + 1
            else:
                height = 1
            self._contained_widgets = ActionWidget
            self._contained_widget_height = height
            if refresh != False:
                self.resize()

        self.is_showing_completions = is_showing_completions

    def display_value(self, vl):
        return vl

    def actionHighlighted(self, value, key):
        app = self.parent.parentApp
        if app.has_entry:
            if key == curses.ascii.LF or key == ord('e'):
                # Apply change
                entry, posting = value.apply()
                if key == ord('e'):
                    # Also open editor
                    app.open_editor(posting.meta['filename'], posting.meta['lineno'])

                del app.process_state.pending_data[0]
                app.has_entry = False

    def skip_entry(self, *args, **kwargs):
        app = self.parent.parentApp
        if app.has_entry:
            app.has_entry = False
            # Change this to be persistent across reloads
            del app.process_state.pending_data[0]

    def modify_account(self, *args, **kwargs):
        app = self.parent.parentApp
        if not app.has_entry:
            return
        wAccount = app.mainForm.wCommand
        wAccount.editable = True
        new_candidate = app.current_matches[-1]
        wAccount.entry_widget.value = new_candidate.target_account

        self.editable = False
        self.h_exit_down(*args)

    def retrain(self, *args, **kwargs):
        app = self.parent.parentApp
        app.classifier = None
        app.initialize_state()

def is_subseq(x, y):
    it = iter(y)
    return all(any(c == ch for c in it) for ch in x)

class AccountEntryWidget(npyscreen.Autocomplete):
    def __init__(self, *args, **kwargs):
        super(AccountEntryWidget, self).__init__(
            *args,
            **kwargs)
        self.editable = False
        self.add_handlers({
            '^A': self.h_beginning_of_line,
            '^B': self.h_cursor_left,
            '^F': self.h_cursor_right,
            '^E': self.h_end_of_line,
            curses.ascii.LF: self.accept,
            curses.ascii.ESC: self.cancel,
            ord(curses.ascii.alt('b')): self.h_backward_word,
            ord(curses.ascii.alt('f')): self.h_forward_word,
            ord(curses.ascii.alt(chr(curses.ascii.DEL))): self.h_backward_delete_word,
        })

    def when_value_edited(self, *args, **kwargs):
        if self.editable:
            app = self.parent.parentApp
            wMain = app.mainForm.wMain
            wMain.values = self.get_completions()
            wMain.update_display_type(True)
            wMain.update()

    def h_beginning_of_line(self, input):
        self.cursor_position = 0

    def h_end_of_line(self, input):
        self.cursor_position = len(self.value)

    def h_backward_word(self, input):
        try:
            self.cursor_position = self.value.rindex(':', 0, self.cursor_position)
        except ValueError:
            self.cursor_position = 0

    def h_backward_delete_word(self, input):
        orig_cursor_position = self.cursor_position
        self.h_backward_word(input)
        self.value = self.value[:self.cursor_position] + self.value[orig_cursor_position:]

    def h_forward_word(self, input):
        try:
            self.cursor_position = self.value.index(':', self.cursor_position+1)+1
        except ValueError:
            self.cursor_position = len(self.value)

    def get_completions(self):
        app = self.parent.parentApp
        state = app.process_state.state
        # Check for prefix matches
        lower_value = self.value.lower()
        completions = [a for a in state.all_accounts if a.lower().startswith(lower_value)]
        if len(completions) == 0:
            # Allow for subsequence matches
            completions = [a for a in state.all_accounts if is_subseq(self.value.lower(), a.lower())]
        completions.sort()
        return completions

    def auto_complete(self, x):
        app = self.parent.parentApp
        state = app.process_state.state
        completions = self.get_completions()
        if len(completions) > 0:
            prefix = os.path.commonprefix(completions)
            self.value = prefix
        self.cursor_position = len(self.value)

    def accept(self, x):
        app = self.parent.parentApp
        state = app.process_state.state
        if self.value not in state.all_accounts:
            if not npyscreen.notify_yes_no(
                    'Account %s does not exist.  Add it?' % self.value,
                    editw=2):
                return
        app.current_matches[-1] = app.make_new_candidate(self.value)
        self.cancel(x)

    def cancel(self, x = None):
        self.value = ''
        app = self.parent.parentApp
        wMain = app.mainForm.wMain
        wMain.editable = True
        wMain.values = app.current_matches
        wMain.update_display_type(False)
        wMain.update()
        self.editable = False
        self.h_exit_up(x)

class TitledAccountEntryWidget(npyscreen.TitleText):
    _entry_type = AccountEntryWidget

class MainForm(npyscreen.FormMutt):
    MAIN_WIDGET_CLASS = ActionList
    COMMAND_WIDGET_CLASS = TitledAccountEntryWidget
    COMMAND_WIDGET_NAME = 'Account: '
    def __init__(self, *args, **kwargs):
        super(MainForm, self).__init__(*args, **kwargs)

class DaemonThreadExecutor(concurrent.futures.Executor):
    """Launches each task in a separate daemon thread."""

    def submit(self, fn, *args, **kwargs):
        f = concurrent.futures.Future()
        def wrapper():
            if not f.set_running_or_notify_cancel():
                return
            try:
                f.set_result(fn(*args, **kwargs))
            except Exception as e:
                f.set_exception(e)

        t = threading.Thread(target = wrapper)
        t.daemon = True
        t.start()
        return f

class App(npyscreen.NPSAppManaged):
    # Note: this is in tenths of a second
    keypress_timeout_default = 1

    def __init__(self, args):
        super(App,self).__init__()
        self.lock = threading.Lock()
        self.status_text = ''
        self.status2_text = ''
        self.args = args
        self.process_state = None
        self.classifier = None

    def log_status(self, status):
        logging.info(status)
        with self.lock:
            self.status_text = status
            self.status2_text = ''

    def onStart(self):
        self.addForm("MAIN", MainForm)
        self.mainForm = self.getForm("MAIN")
        self.initialize_state()

    def do_initialize_state(self):
        if self.process_state is None:
            process_state = ProcessState(self.args, self.log_status)
        else:
            process_state = self.process_state
        if self.classifier is None:
            training_examples = process_state.state.training_examples
            self.log_status('Training classifier with %d examples' % len(training_examples))
            classifier = nltk.classify.scikitlearn.SklearnClassifier(
                estimator = sklearn.tree.DecisionTreeClassifier()
                )
            classifier.train(training_examples)
            logging.info('Evaluating accuracy of classifier')
            errors = 0
            for features, label in training_examples:
                if classifier.classify(features) != label:
                    errors += 1
            logging.info('Classifier accuracy: %.4f' % (1 - float(errors) / len(training_examples)))
        else:
            classifier = self.classifier
        return process_state, classifier

    def initialize_state(self):
        self.dialog_running = False
        self.has_entry = False
        self.current_matches = None
        executor = DaemonThreadExecutor()
        self.process_state_future = executor.submit(self.do_initialize_state)

    def make_new_candidate(self, target_account):
        next_entry = self.process_state.pending_data[0]
        return NewCandidate(entry = next_entry,
                            state = self.process_state.state,
                            target_account = target_account)

    def update_process(self):
        # Check if we need to reload journal state
        if not self.dialog_running and self.process_state_future.done():
            self.process_state, self.classifier = self.process_state_future.result()
            logging.info('Load finished')
            self.dialog_running = True

        need_display = False
        need_force_display = False

        if self.dialog_running:
            # Check if there has been concurrent modification
            if self.process_state.state.check_any_journal_modification():
                self.process_state = None
                self.initialize_state()
                if self.mainForm.wMain.is_showing_completions:
                    self.mainForm.wCommand.entry_widget.cancel()
                need_force_display = True
            else:

                self.status_text = '%d/%d entries pending' % (len(self.process_state.pending_data), len(self.process_state.imported_data))

                if not self.has_entry:
                    if len(self.process_state.pending_data) == 0:
                        sys.exit(0)
                    next_entry = self.process_state.pending_data[0]
                    logging.info('Finding matches')
                    matches = self.process_state.state.get_matches(
                        account = next_entry.account, date = next_entry.date,
                        source_data = next_entry.source_data, amount = next_entry.amount)
                    matches = [MatchCandidate(entry = entry,
                                              posting = posting, state = self.process_state.state,
                                              source_data = next_entry.source_data, date = next_entry.date)
                               for entry, posting in matches]

                    predicted_account = self.classifier.classify(
                        self.process_state.state.get_features(
                            source_account = next_entry.account,
                            source_data = next_entry.source_data,
                            amount = next_entry.amount))

                    matches.append(self.make_new_candidate(predicted_account))
                    self.current_matches = matches
                    if self.mainForm.wMain.is_showing_completions:
                        self.mainForm.wCommand.entry_widget.cancel()
                    else:
                        self.mainForm.wMain.update_display_type(False)

                    self.has_entry = True

                    self.status2_text = '[%s] (%s) %s %s  [%d matches]' % (
                        next_entry.account,
                        next_entry.date.strftime('%Y/%m/%d'),
                        next_entry.source_data,
                        next_entry.amount, len(matches)-1)

                    self.mainForm.wMain.cursor_line = 0
                    need_force_display = True

        with self.lock:
            old_value = self.mainForm.wStatus1.value
            if old_value != self.status_text:
                self.mainForm.wStatus1.value = self.status_text
                need_display = True

            old_value = self.mainForm.wStatus2.value
            if old_value != self.status2_text:
                self.mainForm.wStatus2.value = self.status2_text
                need_display = True

        if need_display:
            logging.debug('Updating display')
            if need_force_display:
                self.mainForm.DISPLAY()
            else:
                self.mainForm.display()

    def while_waiting(self):
        self.update_process()

    def open_editor(self, filename, lineno):
        subprocess.check_call([self.args.editor, '+%d' % lineno, filename],
                              stdout = subprocess.PIPE, stderr = subprocess.PIPE)

def main():
    argparser = argparse.ArgumentParser()
    argparser.add_argument('--journal_input', help = 'Top-level Beancount input file', required = True)
    argparser.add_argument('--journal_output', help = 'Beancount output file to which new transactions will be appended',
                           required = True)
    argparser.add_argument('--account_output', help = 'Beancount output file to which matching accounts will be appended',
                           nargs=2, action = 'append')
    argparser.add_argument('--mint_data', help = 'Mint CSV data file',
                           required = True)
    argparser.add_argument('--editor', type = str, help = 'Editor program to run, invoked as <editor> +<lineno> <filename>')
    argparser.add_argument('--log-output', type = str, help = 'Filename to which log output will be written.',
                           default = '/dev/null')
    argparser.add_argument('--limit', type=str, help='Regular expression for limiting accounts to reconcile.',
                           default='.*')
    argparser.add_argument(
        '-d', '--debug',
        help = 'Set log verbosity to DEBUG.',
        action = 'store_const', dest = 'loglevel', const = logging.DEBUG,
        default = logging.WARNING)
    argparser.add_argument(
        '-v', '--verbose',
        help = 'Set log verbosity to INFO.',
        action = 'store_const', dest = 'loglevel', const = logging.DEBUG)
    argparser.add_argument(
        '--fuzzy_match_days', type = int, default = 3,
        help = 'Maximum number of days by which the dates of two matching entries may differ.')
    args = argparser.parse_args()

    logging.basicConfig(filename = args.log_output, level = args.loglevel)

    app = App(args)
    app.run()
        

if __name__ == '__main__':
    main()
