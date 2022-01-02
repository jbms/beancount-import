"""Google employee Ultipro payroll statement source.

Data format
===========

To use, first download PDF pay statements into a directory on the filesystem
either manually or using the `finance_dl.ultipro_google` module.

You might have a directory structure like:

    financial/
      documents/
        Income/
          Google/
            %Y-%m-%d.statement-<id>.pdf

In some cases, due to a bug of some sort, the document number in the "Pay
History" list on the Ultipro website may differ from the actual document number
in the statement itself.  In this case, the `finance_dl.ultipro_google` module
will save the document with the "wrong" document number in the filename.  When
this module processes the file, it will detect the discrepancy and create a
symbolic link from the filename with the "correct" document number (contained in
the pay statement iself) to the actual file.

Specifying the source to beancount_import
=========================================

Within your Python script for invoking beancount_import, you might use an
expression like the following to specify the Google ultipro source:

    dict(
        module='beancount_import.source.ultipro_google',
        company_name='Google',
        key_prefix='google_payroll',
        currency='USD',
        directory=os.path.join(journal_dir,
                               'documents', 'Income', 'Google'),
        rules={
            'Earnings': [
                ('Regular Pay', 'Income:Google:Salary'),
                ('Annual Bonus', 'Income:Google:Annual-Bonus'),
                ('HSA ER Seed', 'Income:Google:HSA'),
            ],
            'Deductions': [
                ('Dental', 'Expenses:Health:Dental:Insurance'),
                ('Medical', 'Expenses:Health:Medical:Insurance'),
            ],
            'Taxes': [
                ('Federal Income Tax',
                 'Income:Expenses:Taxes:TY{year:04d}:Federal:Income'),
                ('Employee Medicare',
                 'Income:Expenses:Taxes:TY{year:04d}:Federal:Medicare'),
                ('Social Security Employee Tax',
                 'Income:Expenses:Taxes:TY{year:04d}:Federal:Social-Security'),
                ('CA State Income Tax',
                 'Income:Expenses:Taxes:TY{year:04d}:California:Income'),
                ('CA Private Disability Employee',
                 'Income:Expenses:Taxes:TY{year:04d}:California:Disability'),
            ],
            'Net Pay Distribution': [
                ('x+1234', 'Assets:Checking:My-Bank'),
            ],
        },
    )

The `company_name` key is optional (defaults to `"Google"`) and is used as the
payee for the generated transactions.  The `key_prefix` key specifies the prefix
used for metadata keys added to generated transactions, and defaults to
`"google_payroll"`.  The `rules` key specifies a dictionary that maps each of
the keys `"Earnings"`, `"Deductions"`, `"Taxes"`, and `"Net Pay Distribution"`
to optional lists containing `(description_pattern, acount)` tuples.  The
`description_pattern` is a regular expression (using the syntax accepted by the
Python `re` module) matched against the full line item description text.  The
first matching pattern determines the posting account: the `account` value is
passed to the `str.format` function with `year` set to the statement year.

If there is no matching pattern, the placeholder `Expenses:FIXME` account is
used, and the account can be predicted based on the description text.

In addition to the explicitly specified 'Net Pay Distribution' rules, an
additional rule of the form 'x+1234' is created for each Beancount account with
an `account_id` metadata field, where `1234` are the last 4 digits of the
`account_id` value.  For example, specifying:

    2000-01-01 open Assets:Checking:My-Bank
      account_id: "12345678901234"

in your Beancount journal generates an additional 'Net Pay Distribution' rule
`('x+1234', 'Assets:Checking:My-Bank')`.

You may wish to use this source in cojunction with the
`beancount_import.source.ofx` source (to match 401k contributions) and the
`beancount_import.source.stockplanconnect` source (to match payroll lines
related to restricted stock units).

"""

from typing import List, Optional, Tuple, Dict, Set
import datetime
import os
import collections
import functools
import re
from beancount.core.number import D, ZERO
from beancount.core.data import Open, Transaction, Posting, Amount, Entries, Directive, EMPTY_SET
from . import ImportResult, SourceResults, Source, AssociatedData, InvalidSourceReference
from ..matching import FIXME_ACCOUNT
from . import ultipro_google_statement

date_format = '%m/%d/%Y'
journal_date_format = '%Y-%m-%d'

Rules = Dict[str, List[Tuple[str, str]]]

ACCOUNT_ID_KEY = 'account_id'


class Config(object):
    def __init__(self,
                 currency,
                 key_prefix='google_payroll',
                 company_name='Google',
                 **kwargs):
        super().__init__(**kwargs)
        self.company_name = company_name
        self.key_prefix = key_prefix
        self.period_end_date_key = key_prefix + '_period_end_date'
        self.period_start_date_key = key_prefix + '_period_start_date'
        self.pay_date_key = key_prefix + '_pay_date'
        self.document_key = key_prefix + '_document'
        self.desc_key = key_prefix + '_desc'
        self.currency = currency


def make_import_result(parse_result: ultipro_google_statement.ParseResult,
                       account_pattern_for_row_name, config: Config,
                       info: dict) -> ImportResult:
    """Generate journal entries based on a payroll statement.

    :param all_values: parsed payroll statement.
    :param errors: errors from parsing payroll statement.
    :param account_pattern_for_row_name: A function that takes (row_name,
        section) and returns an account pattern. The pattern is later
        transformed by calling format with the year parameter set to the
        appropriate year.
    :param config: specifies the configuration.

    :return: list of beancount entries.
    """
    currency = config.currency
    all_values = parse_result.all_values
    general = parse_result.general
    pay_date = general['Pay Date']['date']
    start_date = general['Period Start Date']['date']
    end_date = general['Period End Date']['date']

    year = pay_date.year

    txn = Transaction(
        meta=collections.OrderedDict(),
        date=pay_date,
        flag='*',
        payee=config.company_name,
        narration='Payroll',
        tags=EMPTY_SET,
        links=EMPTY_SET,
        postings=[],
    )
    for i, error in enumerate(parse_result.errors):
        txn.meta['ultipro_parse_error%d' % i] = error
    document_number = general['Document']['number']
    txn.meta[config.document_key] = document_number
    txn.meta[config.pay_date_key] = pay_date
    txn.meta[config.period_start_date_key] = start_date
    txn.meta[config.period_end_date_key] = end_date

    def add_posting(section, row_name, value):
        account_pattern = account_pattern_for_row_name(row_name, section)
        txn.postings.append(
            Posting(
                account=account_pattern.format(year=year),
                units=Amount(currency=currency, number=value),
                cost=None,
                meta={config.desc_key: '%s: %s' % (section, row_name)},
                price=None,
                flag=None,
            ))

    for section, field_names, sign in [
            (
                'Earnings',
                [('Current', False)],
                -1, # Earnings are recorded as negative amounts
            ),
            (
                'Deductions',
                [('Current', False),
                 ('Current:Employer', True)],
                1),
            (
                'Taxes',
                [('Current', False)],
                1,
            ),
            (
                'Net Pay Distribution',
                [('Amount', False)],
                1,
            ),
            ]:
        for row_name, fields in all_values[section]:
            for field_name, employer_match in field_names:
                value = fields.get(field_name)
                if value is None or value == ZERO:
                    continue
                value *= sign
                if employer_match:
                    row_name += ' Employer Match'
                add_posting(section, row_name, value)
                if employer_match:
                    assert section == 'Deductions'
                    add_posting('Earnings', row_name, -value)

    return ImportResult(date=txn.date, entries=[txn], info=info)


def get_net_pay_rules(journal):
    net_pay_distribution = []
    for entry in journal.accounts.values():
        if entry.meta and ACCOUNT_ID_KEY in entry.meta:
            account_id = str(entry.meta[ACCOUNT_ID_KEY])
            net_pay_distribution.append(('x+' + account_id[-4:], entry.account))
    return net_pay_distribution


statement_filename_re = r'([0-9]{4}-[0-9]{2}-[0-9]{2}).statement-(.*)\.pdf'


class UltiproSource(Config, Source):
    def __init__(self, directory: str, rules, **kwargs) -> None:
        super().__init__(**kwargs)
        self.directory = directory
        self.rules = rules
        self.example_posting_key_extractors = {self.desc_key: None}

    def get_statement_path(self, pay_date: datetime.date, document: str) -> str:
        return os.path.join(
            self.directory, '%s.statement-%s.pdf' %
            (pay_date.strftime(journal_date_format), document))

    def _is_associated_entry(
            self, entry: Directive) -> Optional[Tuple[datetime.date, str]]:
        if not isinstance(entry, Transaction): return None
        meta = entry.meta
        if (meta and self.pay_date_key in meta and self.document_key in meta):
            return (meta[self.pay_date_key], meta[self.document_key])
        return None

    def _preprocess_entries(self, entries: Entries):
        seen_documents = dict()  # type: Dict[Tuple[datetime.date, str], Entries]
        for entry in entries:
            association = self._is_associated_entry(entry)
            if association is not None:
                seen_documents.setdefault(association, []).append(entry)
        return seen_documents

    def _get_import_result(self,
                           parse_result: ultipro_google_statement.ParseResult,
                           account_pattern_for_row_name, path: str):
        return make_import_result(
            config=self,
            parse_result=parse_result,
            account_pattern_for_row_name=account_pattern_for_row_name,
            info=dict(
                type='application/pdf',
                filename=path,
            ),
        )

    def prepare(self, journal, results: SourceResults):

        documents_seen_in_journal = self._preprocess_entries(journal.all_entries)

        net_pay_rules = get_net_pay_rules(journal)

        seen_paths = {
            os.path.realpath(self.get_statement_path(date, document)):
            (date, document)
            for date, document in documents_seen_in_journal
        }

        documents_seen_in_directory = set(
        )  # type: Set[Tuple[datetime.date, str]]

        parsed_statements = []

        realpaths_seen = set()  # type: Set[str]

        for filename in os.listdir(self.directory):
            if not filename.endswith('.pdf'):
                continue
            path = os.path.realpath(os.path.join(self.directory, filename))
            if path in realpaths_seen: continue
            realpaths_seen.add(path)
            seen_key_from_path = seen_paths.get(path)
            if seen_key_from_path is not None:
                documents_seen_in_directory.add(seen_key_from_path)
                continue
            self.log_status('ultipro_google: processing %s' % (path, ))
            try:
                parse_result = ultipro_google_statement.parse_filename(path)
            except ValueError as e:
                raise ValueError(f'{filename}: {e}')
            general = parse_result.general
            document_number = general['Document']['number']
            pay_date = general['Pay Date']['date']
            new_path = self.get_statement_path(pay_date, document_number)
            if not os.path.exists(new_path):
                os.symlink(filename, new_path)
            seen_key = (pay_date, document_number)
            documents_seen_in_directory.add(seen_key)
            if seen_key in documents_seen_in_journal:
                continue
            parsed_statements.append((pay_date, document_number, parse_result,
                                      filename))

        rules = self.rules.copy()
        rules.setdefault('Net Pay Distribution', []).extend(net_pay_rules)

        # This cache exists only for the duration of the
        # self._get_import_result calls that follow.
        @functools.lru_cache(maxsize=None)
        def account_pattern_for_row_name(row_name, section):
            """Returns an account patern.

            Uses `rules, which maps section names to lists of rules
            specifying the account corresponding to a line entry in the
            statement.  For the 'Earnings', 'Deductions', 'Taxes', and 'Net Pay
            Distribution' sections, the rules are specified as
            (description_regex, account) pairs.  The description_regex is
            matched against the textual description for the line entry (it must
            match the entire string).  All account patterns are transformed
            by calling format with the year parameter set to the appropriate
            year.
            """
            for row_re, account_pattern in rules[section]:
                if re.fullmatch(row_re, row_name) is not None:
                    return account_pattern
            return FIXME_ACCOUNT

        parsed_statements.sort(key=lambda x: (x[0], x[1]))
        for pay_date, _, parse_result, filename in parsed_statements:
            results.add_pending_entry(
                self._get_import_result(
                    parse_result,
                    account_pattern_for_row_name,
                    path))

        for seen_key, entries in documents_seen_in_journal.items():
            num_expected = (1 if seen_key in documents_seen_in_directory else 0)
            if len(entries) == num_expected: continue
            num_extra = len(entries) - num_expected
            results.add_invalid_reference(
                InvalidSourceReference(num_extra,
                                       [(entry, None) for entry in entries]))

    def get_associated_data(self,
                            entry: Directive) -> Optional[List[AssociatedData]]:
        association = self._is_associated_entry(entry)
        if association is None: return None
        return [
            AssociatedData(
                meta=(self.document_key, association[1]),
                description='%s payroll statement' % (self.company_name, ),
                type='application/pdf',
                path=os.path.realpath(
                    self.get_statement_path(association[0], association[1])),
            ),
        ]

    @property
    def name(self):
        return 'ultipro_google'


def load(spec, log_status):
    return UltiproSource(log_status=log_status, **spec)
