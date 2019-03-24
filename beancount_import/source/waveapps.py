"""Waveapps receipt transaction source.

This imports transactions from downloaded Waveapps receipts, which can be
obtained using the `finance_dl.waveapps` module.

Data format
===========

To use, first download Waveapps data into a directory on the filesystem using the
finance_dl.waveapps module.

You might have a directory structure like:

    financial/
      data/
        waveapps/
          <id>.json
          <id>.jpg

Specifying the source to beancount_import
=========================================

Within your Python script for invoking beancount_import, you might use an
expression like the following to specify the Waveapps source:

    dict(module='beancount_import.source.paypal',
         receipt_directory=os.path.join(journal_dir, 'data', 'waveapps'),
         link_prefix='waveapps.',
    )

where `journal_dir` refers to the financial/ directory.

The `directory` specifies the directory containing the `.json` and `.html`
files.  The `link_prefix` should be unique over all of your sources, and should
end with a `.` or other delimiter.  It is concatenated with the receipt `id` to
form a unique `link` to apply to the generated transaction that associates it
with the receipt data.

Imported transaction format
===========================

Generated transactions have the following format:

    2019-01-10 * "Solar4America Ice" ^waveapps.12345678
      Expenses:FIXME   34.00 USD
      Expenses:FIXME  -34.00 USD

    2019-01-19 * "Robee's Falafel" "Falafel plate" ^waveapps.10000001
      Expenses:FIXME   13.50 USD
      Expenses:FIXME  -13.50 USD

No metadata is included for unknown account prediction, because it is expected
that these transactions will be matched to transactions imported from bank
statements.

"""

from typing import Dict, List, Any, Optional, Iterable
import datetime
import os
import collections
import json

import jsonschema
from beancount.core.number import D, ZERO
from beancount.core.data import Open, Transaction, Posting, Amount, Pad, Balance, Entries, Directive

from . import ImportResult, SourceResults, Source, InvalidSourceReference, AssociatedData
from ..matching import FIXME_ACCOUNT

from .link_based_source import LinkBasedSource

date_format = '%Y-%m-%d'

schema = {
    '#schema': 'http://json-schema.org/draft-07/schema#',
    'description': 'JSON schema for the receipt details.',
    'type': 'object',
    'properties': {
        'id': {
            'type': 'number',
        },
        'currency_code': {
            'type': 'string',
        },
        'note': {
            'type': 'string',
        },
        'merchant': {
            'type': 'string',
        },
        'date': {
            'type': 'string',
        },
        'total': {
            'type': 'string',
        },
        'status': {
            'type': 'string',
        },
    },
    'required': ['id', 'currency_code', 'merchant', 'date', 'note', 'status'],
}


def make_import_result(receipt: Any, receipt_directory: str,
                       link_prefix: str) -> ImportResult:
    receipt_id = str(receipt['id'])
    date = datetime.datetime.strptime(receipt['date'], date_format).date()
    merchant = receipt['merchant']
    note = receipt['note']
    if note:
        payee = merchant
        narration = note
    else:
        payee = None
        narration = merchant
    amount = Amount(
        number=D(receipt['total']), currency=receipt['currency_code'])
    postings = [
        Posting(
            account=FIXME_ACCOUNT,
            units=amount,
            cost=None,
            meta=None,
            price=None,
            flag=None,
        ),
        Posting(
            account=FIXME_ACCOUNT,
            units=-amount,
            cost=None,
            meta=None,
            price=None,
            flag=None,
        )
    ]
    return ImportResult(
        date=date,
        entries=[
            Transaction(
                meta=collections.OrderedDict(),
                date=date,
                flag='*',
                payee=payee,
                narration=narration,
                links=frozenset([link_prefix + receipt_id]),
                tags=frozenset(),
                postings=postings,
            ),
        ],
        info=dict(
            type='image/jpeg',
            filename=os.path.realpath(
                os.path.join(receipt_directory, receipt_id + '.jpg')),
        ),
    )


def _get_image_paths(receipt_directory: str, receipt_id: str) -> Iterable[str]:
    i = 0
    while True:
        if i == 0:
            suffix = ''
        else:
            suffix = '.%02d' % i
        path = os.path.join(receipt_directory, receipt_id + suffix + '.jpg')
        if not os.path.exists(path):
            break
        yield path
        i += 1


class WaveappsSource(LinkBasedSource, Source):
    def __init__(self, receipt_directory: str, **kwargs) -> None:
        super().__init__(**kwargs)
        self.receipt_directory = receipt_directory

    def prepare(self, journal, results: SourceResults):
        json_suffix = '.json'
        receipt_ids = frozenset(x[:-len(json_suffix)]
                                for x in os.listdir(self.receipt_directory)
                                if x.endswith(json_suffix))
        receipts_seen_in_journal = self.get_entries_with_link(
            all_entries=journal.all_entries,
            valid_links=receipt_ids,
            results=results)
        for receipt_id in sorted(receipt_ids):
            if receipt_id in receipts_seen_in_journal: continue
            path = os.path.join(self.receipt_directory,
                                receipt_id + json_suffix)
            self.log_status('waveapps: processing %s' % (path, ))
            with open(path, 'r', newline='\n', encoding='utf-8') as f:
                receipt = json.load(f)
            jsonschema.validate(receipt, schema)
            if receipt['status'] != 'Ready':
                results.add_warning('Skipping receipt %r due to status of %r' %
                                    (path, receipt['status']))
                continue
            results.add_pending_entry(
                make_import_result(receipt,
                                   receipt_directory=self.receipt_directory,
                                   link_prefix=self.link_prefix))

    def get_associated_data_for_link(
            self, entry_id: str) -> Optional[List[AssociatedData]]:
        return [
            AssociatedData(
                description='Receipt image',
                type='image/jpeg',
                path=image_path,
            ) for image_path in _get_image_paths(self.receipt_directory,
                                                 entry_id)
        ]

    @property
    def name(self):
        return 'waveapps'


def load(spec, log_status):
    return WaveappsSource(log_status=log_status, **spec)
