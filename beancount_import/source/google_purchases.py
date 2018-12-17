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
from . import ImportResult, SourceResults, Source, InvalidSourceReference, AssociatedData
from .link_based_source import LinkBasedSource
from ..matching import FIXME_ACCOUNT

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


class GooglePurchasesSource(LinkBasedSource, Source):
    def __init__(self,
                 directory: str,
                 time_zone: Optional[str] = None,
                 **kwargs) -> None:
        super().__init__(**kwargs)
        self.directory = directory
        self.tz_info = dateutil.tz.gettz(time_zone)

    def prepare(self, journal, results: SourceResults):
        json_suffix = '.json'
        receipt_ids = frozenset(x[:-len(json_suffix)]
                                for x in os.listdir(self.directory)
                                if x.endswith(json_suffix))
        receipts_seen_in_journal = self.get_entries_with_link(
            all_entries=journal.all_entries,
            valid_links=receipt_ids,
            results=results)
        for receipt_id in sorted(receipt_ids):
            if receipt_id in receipts_seen_in_journal: continue
            path = os.path.join(self.directory, receipt_id + json_suffix)
            self.log_status('google_purchases: processing %s' % (path, ))
            with open(path, 'r') as f:
                receipt = json.load(f)
            results.add_pending_entry(
                make_import_result(
                    receipt,
                    tz_info=self.tz_info,
                    link_prefix=self.link_prefix,
                    html_path=self._get_html_path(receipt_id)))

    def _get_html_path(self, receipt_id: str):
        return os.path.join(self.directory, receipt_id + '.html')

    def get_associated_data_for_link(
            self, entry_id: str) -> Optional[List[AssociatedData]]:
        return [
            AssociatedData(
                description='Purchase details',
                type='text/html',
                path=self._get_html_path(entry_id),
            )
        ]

    @property
    def name(self):
        return 'google_purchases'


def load(spec, log_status):
    return GooglePurchasesSource(log_status=log_status, **spec)
