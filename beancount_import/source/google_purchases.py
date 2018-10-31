"""Google Purchases transaction source.

This imports transactions from downloaded purchase history from
https://myaccount.google.com/purchases, as obtained using the
`finance_dl.google_purchases` module.

The primary intended use of this data source is to associate the downloaded
purchase details HTML page with a transaction also imported from a bank
statement.

Data format
===========

To use, first download data using the `finance_dl.google_purchases` module,
using a directory structure like:

    financial/
      data/
        google_purchases/
          XXXXXXXXXXXXXXXXXXXX.json
          XXXXXXXXXXXXXXXXXXXX.html

Specifying the source to beancount_import
=========================================

Within your Python script for invoking beancount_import, you might use an
expression like the following to specify the Google Purchases source:

    dict(module='beancount_import.source.google_purchases',
         directory=os.path.join(journal_dir, 'data/google_purchases'),
         link_prefix='google_purchases.',
    )

The `directory` specifies the directory containing the `.json` and `.html`
files.  The `link_prefix` should be unique over all of your sources, and should
end with a `.` or other delimiter.  It is concatenated with the purchase `id` to
form a unique `link` to apply to the generated transaction that associates it
with the purchase data.

You may also optionally specify a `time_zone` property to specify the time zone
to use to convert the timestamps to dates.  The value of `time_zone` should be a
IANA time zone name, or a valid TZ environment variable value, or `None` (to
indicate local time), and is passed to the `dateutil.tz.gettz` function.  By
default, the local time is used.

Imported transaction format
===========================

Generated transactions have the following format:

    2017-10-15 * "AutoZone" "Relay - Blower Motor" ^google_purchase.88348393419580772365
      Expenses:FIXME   21.84 USD
      Expenses:FIXME  -21.84 USD

    2018-07-23 * "Square - Some merchant" "Some purchase" ^google_purchase.79184124027478022589
      Expenses:FIXME   10.00 USD
      Expenses:FIXME  -10.00 USD

No metadata is included for unknown account prediction, because it is expected
that these transactions will be matched to transactions imported from bank
statements.

"""

from typing import Dict, List, Any, Optional
import datetime
import os
import collections
import json

import dateutil.tz
from beancount.core.number import D, ZERO
from beancount.core.data import Open, Transaction, Posting, Amount, Pad, Balance, Entries, Directive
from beancount_import.source import ImportResult, SourceResults, Source, InvalidSourceReference, AssociatedData
from beancount_import.matching import FIXME_ACCOUNT

date_format = '%Y-%m-%d'


def make_import_result(purchase: Any, link_prefix: str,
                       tz_info: Optional[datetime.tzinfo],
                       html_path: str) -> ImportResult:
    purchase_id = str(purchase['id'])
    date = datetime.datetime.fromtimestamp(purchase['timestamp'] / 1000,
                                           tz_info).date()
    payment_processor = purchase['payment_processor']
    merchant = purchase['merchant']
    items = purchase['items']
    payee = ' - '.join(x for x in [payment_processor, merchant]
                       if x is not None)  # type: Optional[str]
    narration = '; '.join(x for x in items)  # type: Optional[str]
    if not narration:
        narration = payee
        payee = None
    if purchase['currency'] is None or purchase['units'] is None:
        pos_amount = neg_amount = Amount(D('0.00'), 'USD')
    else:
        pos_amount = Amount(
            round(D(purchase['units']), 2), purchase['currency'])
        neg_amount = -pos_amount
    postings = [
        Posting(
            account=FIXME_ACCOUNT,
            units=pos_amount,
            cost=None,
            meta=None,
            price=None,
            flag=None,
        ),
        Posting(
            account=FIXME_ACCOUNT,
            units=neg_amount,
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
                links=frozenset([link_prefix + purchase_id]),
                tags=frozenset(),
                postings=postings,
            ),
        ],
        info=dict(
            type='text/html',
            filename=os.path.realpath(html_path),
        ),
    )


class GooglePurchasesSource(Source):
    def __init__(self, directory: str, link_prefix: str,
                 time_zone: Optional[str], **kwargs) -> None:
        super().__init__(**kwargs)
        self.directory = directory
        self.link_prefix = link_prefix
        self.tz_info = dateutil.tz.gettz(time_zone)

    def _preprocess_entries(self,
                            entries: Entries) -> Dict[str, List[Transaction]]:
        link_prefix = self.link_prefix
        seen_receipts = dict()  # type: Dict[str, Entries]
        for entry in entries:
            if not isinstance(entry, Transaction): continue
            for link in entry.links:
                if not link.startswith(link_prefix): continue
                receipt_id = link[len(link_prefix):]
                seen_receipts.setdefault(receipt_id, []).append(entry)
        return seen_receipts

    def prepare(self, journal, results: SourceResults):
        receipts_seen_in_journal = self._preprocess_entries(journal.all_entries)
        json_suffix = '.json'
        receipts_seen_in_directory = set()
        for filename in os.listdir(self.directory):
            if not filename.endswith(json_suffix): continue
            receipt_id = os.path.basename(filename)[:-len(json_suffix)]
            receipts_seen_in_directory.add(receipt_id)
            if receipt_id in receipts_seen_in_journal: continue
            path = os.path.join(self.directory, filename)
            self.log_status('google_purchases: processing %s' % (path, ))
            with open(path, 'r') as f:
                receipt = json.load(f)
            results.add_pending_entry(
                make_import_result(
                    receipt,
                    tz_info=self.tz_info,
                    link_prefix=self.link_prefix,
                    html_path=self._get_html_path(receipt_id)))
        for receipt_id, entries in receipts_seen_in_journal.items():
            expected_count = 1 if receipt_id in receipts_seen_in_directory else 0
            if len(entries) == expected_count: continue
            results.add_invalid_reference(
                InvalidSourceReference(
                    num_extras=len(entries) - expected_count,
                    transaction_posting_pairs=[(t, None) for t in entries]))

    def _get_html_path(self, receipt_id: str):
        return os.path.join(self.directory, receipt_id + '.html')

    def get_associated_data(self,
                            entry: Directive) -> Optional[List[AssociatedData]]:
        if not isinstance(entry, Transaction): return None
        link_prefix = self.link_prefix
        associated_data = []  # type: List[AssociatedData]
        for link in entry.links:
            if link.startswith(link_prefix):
                receipt_id = link[len(link_prefix):]
                associated_data.append(
                    AssociatedData(
                        link=link,
                        description='Purchase details',
                        type='text/html',
                        path=self._get_html_path(receipt_id),
                    ))
        return associated_data

    @property
    def name(self):
        return 'google_purchases'


def load(spec, log_status):
    return GooglePurchasesSource(log_status=log_status, **spec)
