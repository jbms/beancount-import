"""Google Purchases transaction source.

This imports transactions from downloaded purchase history from Google Takeout,
as obtained using the `finance_dl.google_purchases` module.

The primary intended use of this data source is to associate the downloaded
purchase details HTML page with a transaction also imported from a bank
statement.

Data format
===========

To use, first download data manually with Google Takeout or automatically with
the `finance_dl.google_purchases` module, using a directory structure like:

    financial/
      data/
        google_purchases/
          order_XXXXXXXXXXXXXXXXXXXX.json
          XXXXXXXXXXXXXXXXXXXX.html

The `.html` files are optional (and won't be available if you download the data
manually using Google Takeout).

Specifying the source to beancount_import
=========================================

Within your Python script for invoking beancount_import, you might use an
expression like the following to specify the Google Purchases source:

    dict(module='beancount_import.source.google_purchases',
         directory=os.path.join(journal_dir, 'data/google_purchases'),
         link_prefix='google_purchases.',
         ignored_transaction_merchants_pattern=r'Amazon\.com',
    )

The `directory` specifies the directory containing the `.json` and `.html`
files.  The `link_prefix` should be unique over all of your sources, and should
end with a `.` or other delimiter.  It is concatenated with the purchase `id` to
form a unique `link` to apply to the generated transaction that associates it
with the purchase data.

The optional `ignored_transaction_merchants_pattern` member specifies a regular
expression that is matched against the full transaction merchant; if it matches,
the transaction is ignored.  This is useful, for example, for transactions that
are already covered by another data source.

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

from typing import List, Any, Optional
import datetime
import os
import re
import collections
import json

import dateutil.tz
from beancount.core.number import D, ZERO
from beancount.core.data import Transaction, Posting, Amount
from . import ImportResult, SourceResults, Source, AssociatedData
from .link_based_source import LinkBasedSource
from ..matching import FIXME_ACCOUNT, SimpleInventory

date_format = '%Y-%m-%d'


def make_old_import_result(purchase: Any, purchase_id: str, link_prefix: str,
                           ignored_transaction_merchants_pattern: str,
                           tz_info: Optional[datetime.tzinfo],
                           html_path: str) -> Optional[ImportResult]:
    date = datetime.datetime.fromtimestamp(purchase['timestamp'] / 1000,
                                           tz_info).date()
    payment_processor = purchase['payment_processor']
    if (payment_processor is not None and re.fullmatch(
            ignored_transaction_merchants_pattern, payment_processor)):
        return None
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


def parse_amount_from_priceline(x: Any):
    return Amount(D(x['amountMicros']) / 1000000, x['currencyCode']['code'])


def make_takeout_import_result(purchase: Any, purchase_id: str,
                               link_prefix: str,
                               ignored_transaction_merchants_pattern: str,
                               tz_info: Optional[datetime.tzinfo],
                               html_path: str) -> Optional[ImportResult]:
    if ('creationTime' not in purchase or
            'transactionMerchant' not in purchase or
            'name' not in purchase['transactionMerchant'] or
            'usecSinceEpochUtc' not in purchase['creationTime']):
        # May be a reservation rather than a purchase
        return None
    date = datetime.datetime.fromtimestamp(
        int(purchase['creationTime']['usecSinceEpochUtc']) / 1000000,
        tz_info).date()
    payment_processor = purchase['transactionMerchant']['name']
    if (payment_processor is not None and re.fullmatch(
            ignored_transaction_merchants_pattern, payment_processor)):
        return None
    unique_merchants = set()
    merchant = None  # type: Optional[str]
    item_names = []
    for line_item in purchase['lineItem']:
        if 'provider' in line_item:
            merchant = line_item['provider']['name']
            unique_merchants.add(merchant)
        if 'purchase' not in line_item:
            continue
        line_item_purchase = line_item['purchase']
        if 'productInfo' in line_item_purchase:
            product_info = line_item_purchase['productInfo']
            text = product_info['name']
            if 'description' in line_item:
                text += '; '
                text += product_info['description']
            item_names.append(text)
    if len(unique_merchants) != 1:
        merchant = None
    inventory = SimpleInventory()
    for priceline in purchase.get('priceline', []):
        inventory += parse_amount_from_priceline(priceline['amount'])
    payee = ' - '.join(x for x in [payment_processor, merchant]
                       if x is not None)  # type: Optional[str]
    narration = '; '.join(x for x in item_names)  # type: Optional[str]
    if not narration:
        narration = payee
        payee = None
    postings = []
    if len(inventory) == 0:
        inventory['USD'] = ZERO
    for currency, units in inventory.items():
        pos_amount = Amount(round(units, 2), currency)
        neg_amount = -pos_amount
        postings.append(
            Posting(
                account=FIXME_ACCOUNT,
                units=pos_amount,
                cost=None,
                meta=None,
                price=None,
                flag=None,
            ))
        postings.append(
            Posting(
                account=FIXME_ACCOUNT,
                units=neg_amount,
                cost=None,
                meta=None,
                price=None,
                flag=None,
            ))
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
                 ignored_transaction_merchants_pattern: str = '',
                 **kwargs) -> None:
        super().__init__(**kwargs)
        self.directory = directory
        self.tz_info = dateutil.tz.gettz(time_zone)
        self.ignored_transaction_merchants_pattern = ignored_transaction_merchants_pattern

    def prepare(self, journal, results: SourceResults):
        json_suffix = '.json'
        # Prefix for takeout JSON files
        takeout_prefix = 'order_'
        old_receipt_ids = frozenset(
            x[:-len(json_suffix)] for x in os.listdir(self.directory)
            if x.endswith(json_suffix) and not x.startswith(takeout_prefix))
        takeout_receipt_ids = frozenset(
            x[len(takeout_prefix):-len(json_suffix)]
            for x in os.listdir(self.directory)
            if x.endswith(json_suffix) and x.startswith(takeout_prefix))
        receipt_ids = old_receipt_ids.union(takeout_receipt_ids)
        receipts_seen_in_journal = self.get_entries_with_link(
            all_entries=journal.all_entries,
            valid_links=receipt_ids,
            results=results)
        for receipt_id in sorted(receipt_ids):
            if receipt_id in receipts_seen_in_journal: continue
            # Prefer takeout-format data if available
            if receipt_id in takeout_receipt_ids:
                prefix = 'order_'
            else:
                prefix = ''
            path = os.path.join(self.directory,
                                prefix + receipt_id + json_suffix)
            self.log_status('google_purchases: processing %s' % (path, ))
            with open(path, 'r', encoding='utf-8', newline='\n') as f:
                receipt = json.load(f)
            if receipt_id in takeout_receipt_ids:
                import_result = make_takeout_import_result(
                    receipt,
                    purchase_id=receipt_id,
                    ignored_transaction_merchants_pattern=self.
                    ignored_transaction_merchants_pattern,
                    tz_info=self.tz_info,
                    link_prefix=self.link_prefix,
                    html_path=self._get_html_path(receipt_id))
            else:
                import_result = make_old_import_result(
                    receipt,
                    purchase_id=receipt_id,
                    ignored_transaction_merchants_pattern=self.
                    ignored_transaction_merchants_pattern,
                    tz_info=self.tz_info,
                    link_prefix=self.link_prefix,
                    html_path=self._get_html_path(receipt_id))
            if import_result is None: continue
            results.add_pending_entry(import_result)

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
