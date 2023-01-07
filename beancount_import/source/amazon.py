"""Amazon transaction source.

This imports transactions from Amazon order invoice (regular and digital) HTML
files.  The primary advantage this provides over just relying on bank statements
is that the imported transactions include the full order breakdown, which makes
it much easier to properly categorize transactions.  As a secondary advantage,
it also includes gift card and reward point information.

Data format
===========

To use, first download order invoices into a directory on the filesystem.
Although you can manually download individual order invoices by using the "Save"
command in a web browser, the easiest way to download data from Amazon in the
requisite format is to use the finance_dl.amazon module.

Typically, you would use a directory structure like:

    financial/
      data/
        amazon/
          XXX-XXXXXXX-XXXXXXX.html

Each `.html` file must have the full order id as the part of the name before the
`.html` extension.  To download these files manually, view the "Your Orders"
page on the Amazon website, then click on the "Invoice" link (not the "Order
Details" link) for a given order, wait for it to finish loading, and then save
the webpage as HTML, and rename the HTML file to have the appropriate name.

Specifying the source to beancount_import
=========================================

Within your Python script for invoking beancount_import, you might use an
expression like the following to specify the Amazon source:

    dict(module='beancount_import.source.amazon',
         directory=os.path.join(journal_dir, 'data/amazon'),
         pickle_dir=os.path.join(journal_dir, 'data/amazon/.pickle')
         amazon_account='name@domain.com',
         posttax_adjustment_accounts={
             'Gift Card Amount': 'Assets:Gift-Cards:Amazon',
             'Rewards Points': 'Income:Amazon:Cashback',
         },
         locale='en_US'  # optional, defaults to 'en_US'
    )

The `amazon_account` key must be specified, and should be set to the email
address used for logging into Amazon.  In principle, the value could be set to
any arbitrary string, as it is not checked in any way, but if you specify
multiple `amazon_source` sources, each must have a unique `amazon_account`
value.

The `posttax_adjustment_accounts` dictionary is optional.  Currently the only
valid keys are `"Gift Card Amount"` and `"Rewards Points"`.  Even if you don't
specify these keys in the configuration, the generic automatic account
prediction will likely handle them.

The `locale` sets country/language specific settings.
Currently, `en_US` and `de_DE` are available. 

Specifying credit cards
=======================

Optionally, you can add a credit_card_last_4_digits metadata property to credit
card and debit card accounts that you use with Amazon, as follows:

2003-01-01 open Assets:Checking:Fidelity USD
  credit_card_last_4_digits: "1234 5678"

The digits are specified as a string property which may contain any number of
space-separated 4-digit strings.  Specifying more than one string is useful in
case multiple credit/debit cards have been issued for the account over the years
with different numbers.

Even if you don't specify a `credit_card_last_4_digits` metadata field on the
`open` directive, the generic automatic account prediction will likely handle
them.

Imported transaction format
===========================

In the simple case of an order containing a single shipment with a single item,
a transaction of the following form is generated:

    2016-02-07 * "Amazon.com" "Order"
      amazon_account: "name@domain.com"
      amazon_order_id: "166-7926740-5141621"
      Expenses:FIXME:A   11.87 USD
        amazon_item_condition: "New"
        amazon_item_description: "Casio Men's W916-8AV Alarm Chronograph Watch, classic"
        amazon_item_quantity: 1
        amazon_seller: "Amazon.com LLC"
        shipped_date: 2016-02-08
      Expenses:FIXME:A    1.13 USD
        amazon_invoice_description: "Sales Tax"
      Expenses:FIXME     -1.06 USD
        amazon_posttax_adjustment: "Rewards Points"
      Expenses:FIXME    -11.94 USD
        amazon_credit_card_description: "Amazon.com Visa Signature ending in 1234"
        transaction_date: 2016-02-08

The `amazon_account` and `amazon_order_id` metadata fields on the transaction
associate the transaction with an order invoice.

Because Amazon invoices list tax and shipping charges on a per-shipment rather
than per-item basis, all postings associated with a given shipment are assigned
the same unknown account name of the form `Expenses:FIXME:X` (where `X` is any
letter), rather than the usual `Expenses:FIXME` account name.  All accounts with
the same unknown account name are predicted as a single group, rather than
individually, based on the transaction's `amazon_account` metadata field and the
combined `amazon_item_description` metadata fields of each posting in the group.
The `amazon_invoice_description` field does not contribute to the prediction.

The accounts for postings corresponding to payments using rewards points and
gift cards, if not specified directly in the configuration, can be predicted
based on the transaction's `amazon_account` metadata field and the posting's
`amazon_posttax_adjustment` metadata field.

The accounts for credit/debit card payments, if not identified based on a
`credit_card_last_4_digits` metadata field specified in an account `open`
directive, can be predicted based on the transaction's `amazon_account` metadata
field and the posting's `amazon_credit_card_description` metadata field.  Note
that in some cases, the order invoice will show multiple credit card
transactions that actually correspond to a single credit card transaction on the
bank statement.  In most cases the transaction matching logic in
beancount-import is able to handle that correctly.

For orders containing multiple shipments of multiple items each, you can
sometimes end up with monster transactions like the following:

    2017-03-23 * "Amazon.com" "Order"
      amazon_account: "name@domain.com"
      amazon_order_id: "277-5312419-9119541"
      Expenses:FIXME:A   10.80 USD
        amazon_item_condition: "New"
        amazon_item_description: "C.R. Gibson 9-Count Coloring File Folders, 3 of Each Design, 10 Adhesive Labels, Measures 11.5 x 9.5 - Gold"
        amazon_item_quantity: 1
        amazon_seller: "Amazon.com LLC"
        shipped_date: 2017-03-24
      Expenses:FIXME:A   12.60 USD
        amazon_item_condition: "New"
        amazon_item_description: "MUJI Gel Ink Ballpoint Pens 0.38mm 9-colors Pack"
        amazon_item_quantity: 1
        amazon_seller: "hidamarifarm"
        shipped_date: 2017-03-24
      Expenses:FIXME:A   16.50 USD
        amazon_item_condition: "New"
        amazon_item_description: "Cynthia Rowley File Folders, 3 Tab, 6 Folders per package, Leaves Assorted Patterns"
        amazon_item_quantity: 1
        amazon_seller: "Sherry Pappas"
        shipped_date: 2017-03-24
      Expenses:FIXME:A    8.99 USD
        amazon_item_condition: "New"
        amazon_item_description: "V&A William Morris Garden File Folder, Galison"
        amazon_item_quantity: 1
        amazon_seller: "Amazon.com LLC"
        shipped_date: 2017-03-24
      Expenses:FIXME:A   23.95 USD
        amazon_item_condition: "New"
        amazon_item_description: "Bloom Daily Planners All In One Planner, Calendar, Notebook, To-Do List Book, Sketch Book, Coloring Book and More! 9 x 11 Do More of What Makes You Happy"
        amazon_item_quantity: 1
        amazon_seller: "bloom daily planners"
        shipped_date: 2017-03-24
      Expenses:FIXME:A    8.99 USD
        amazon_item_condition: "New"
        amazon_item_description: "Skydue Floral Printed Accordion Document File Folder Expanding Letter Organizer (Pink)"
        amazon_item_quantity: 1
        amazon_seller: "Skydue"
        shipped_date: 2017-03-24
      Expenses:FIXME:A    1.83 USD
        amazon_invoice_description: "Sales Tax"
      Expenses:FIXME:B    7.99 USD
        amazon_item_condition: "New"
        amazon_item_description: "Skydue Letter A4 Paper Expanding File Folder Pockets Accordion Document Organizer (Jade)"
        amazon_item_quantity: 1
        amazon_seller: "Skydue"
        shipped_date: 2017-03-27
      Expenses:FIXME:C   14.84 USD
        amazon_item_condition: "New"
        amazon_item_description: "Design Ideas 8758616-DI 8758616-DI Cabo LetterHolder-Copper,Copper,"
        amazon_item_quantity: 1
        amazon_seller: "Quidsi Retail LLC"
        shipped_date: 2017-03-24
      Expenses:FIXME:C    1.37 USD
        amazon_invoice_description: "Sales Tax"
      Expenses:FIXME:D   17.95 USD
        amazon_item_condition: "New"
        amazon_item_description: "Cynthia Rowley File Folders, 3 Tab, 6 folders per package, Gold Assorted Patterns"
        amazon_item_quantity: 1
        amazon_seller: "Cailler's LLC"
        shipped_date: 2017-03-25
      Expenses:FIXME:D   14.99 USD
        amazon_item_condition: "New"
        amazon_item_description: "Suncatchers Colorful Bird Stained Glass Effect Resin Mobile - Beautiful Window Hanging - Home Decoration"
        amazon_item_quantity: 1
        amazon_seller: "That Internet Shop USA"
        shipped_date: 2017-03-25
      Expenses:FIXME:D    1.39 USD
        amazon_invoice_description: "Sales Tax"
      Expenses:FIXME:E   12.00 USD
        amazon_item_condition: "New"
        amazon_item_description: "Rifle Paper Co. Jardin Weekly Desk Planner Notepad"
        amazon_item_quantity: 1
        amazon_seller: "Our Pampered Home"
        shipped_date: 2017-03-24
      Expenses:FIXME    -71.06 USD
        amazon_credit_card_description: "Amazon.com Store Card ending in 1234"
        transaction_date: 2017-03-25
      Expenses:FIXME    -16.21 USD
        amazon_credit_card_description: "Amazon.com Store Card ending in 1234"
        transaction_date: 2017-03-25
      Expenses:FIXME    -12.60 USD
        amazon_credit_card_description: "Amazon.com Store Card ending in 1234"
        transaction_date: 2017-03-27
      Expenses:FIXME    -34.33 USD
        amazon_credit_card_description: "Amazon.com Store Card ending in 1234"
        transaction_date: 2017-03-27
      Expenses:FIXME    -12.00 USD
        amazon_credit_card_description: "Amazon.com Store Card ending in 1234"
        transaction_date: 2017-03-27
      Expenses:FIXME     -7.99 USD
        amazon_credit_card_description: "Amazon.com Store Card ending in 1234"
        transaction_date: 2017-03-28

Dealing with transaction balance errors
=======================================

In some cases, either due to a failure to properly parse the invoice, or due to
an error in the invoice itself (or in even rarer cases, due to an error in the
actual transaction), a mismatch in amounts may be detected.  In these cases, a
transaction is still generated, but with additional `amazon_invoice_errorN`
metadata fields explaining the mismatches.  For these transactions, you should
manually inspect the original invoice (and if that is not sufficient, possibly
check your bank statements and/or confirm with Amazon), and then manually edit
the transaction as needed.

Dealing with parsing errors
===========================

If you run into an invoice that results in a parse error, you can use the
`beancount_import.source.amazon_invoice` module, which has a command-line
interface, to try to debug it.

Caching Parsing Results
=======================

Parsing the HTML files can be slow, so this module uses the Python pickle 
module/file format to cache the results of parsing the individual HTML files.
These cached results are loaded as long as their mtime is more recent than
the HTML file to load. If you want to enable this funcionality, pass a path
to the `pickle_dir` parameter when initializing this class.

Skipping Older Invoices
=======================

In the event you want to only process invoices that happened after a certain
date you can pass the earliest date you want to process as a configuration 
parameter `earliest_date` when initializing the this class.

This requires parsing the HTML file in order to determine the date of the
invoice, so it is recommended to use the caching/pickling mechanism described
above if you choose to have a large number of invoices in your data folder that
are not accounted for in your journal.
"""

import collections
from typing import Dict, List, Tuple, Optional, Union
import os
import sys
import pickle
import logging

from beancount.core.data import Transaction, Posting, Balance, Commodity, Price, EMPTY_SET, Directive
from beancount.core.amount import Amount
from beancount.core.flags import FLAG_OKAY
from beancount.core.number import ZERO, ONE
import beancount.core.amount

from .amazon_invoice import LOCALES, parse_invoice, DigitalItem, Order

from ..matching import FIXME_ACCOUNT, SimpleInventory
from ..posting_date import POSTING_DATE_KEY, POSTING_TRANSACTION_DATE_KEY
from . import ImportResult, Source, SourceResults, InvalidSourceReference, AssociatedData
from ..journal_editor import JournalEditor

import datetime

logger = logging.getLogger('amazon')

ITEM_DESCRIPTION_KEY = 'amazon_item_description'
ITEM_URL_KEY = 'amazon_item_url'
ITEM_BY_KEY = 'amazon_item_by'
ITEM_QUANTITY_KEY = 'amazon_item_quantity'
SELLER_KEY = 'amazon_seller'
ORDER_ID_KEY = 'amazon_order_id'
ITEM_CONDITION_KEY = 'amazon_item_condition'
SHIPPED_DATE_KEY = 'shipped_date'
INVOICE_DESCRIPTION = 'amazon_invoice_description'
CREDIT_CARD_DESCRIPTION_KEY = 'amazon_credit_card_description'
CREDIT_CARD_LAST_4_KEY = 'credit_card_last_4_digits'
AMAZON_ACCOUNT_KEY = 'amazon_account'
POSTTAX_DESCRIPTION_KEY = 'amazon_posttax_adjustment'


def make_amazon_transaction(
        invoice: Order,
        posttax_adjustment_accounts,
        credit_card_accounts,
        amazon_account: str,
        payee='Amazon.com'
):
    txn = Transaction(
        date=invoice.order_date,
        meta=collections.OrderedDict([
            (ORDER_ID_KEY, invoice.order_id),
            (AMAZON_ACCOUNT_KEY, amazon_account),
        ]),
        payee=payee,
        narration='Order',
        flag=FLAG_OKAY,
        tags=EMPTY_SET,
        links=EMPTY_SET,
        postings=[],
    )
    for i, error in enumerate(invoice.errors):
        txn.meta['amazon_invoice_error%d' % i] = error
    for i, shipment in enumerate(invoice.shipments):
        unknown_account_name = FIXME_ACCOUNT + ':' + chr(ord('A') + i)
        for item in shipment.items:
            meta = collections.OrderedDict([
                (ITEM_DESCRIPTION_KEY, item.description),
                (SELLER_KEY, item.sold_by),
            ])  # type: Dict[str, Optional[Union[str, datetime.date]]]
            if isinstance(item, DigitalItem):
                if item.url:
                    meta[ITEM_URL_KEY] = item.url
                if item.by:
                    meta[ITEM_BY_KEY] = item.by
                quantity = ONE
            else:
                if item.condition:
                    meta[ITEM_CONDITION_KEY] = item.condition
                if shipment.shipped_date is not None:
                    meta[SHIPPED_DATE_KEY] = shipment.shipped_date
                meta[ITEM_QUANTITY_KEY] = item.quantity
                quantity = item.quantity
            txn.postings.append(
                Posting(
                    account=unknown_account_name,
                    units=beancount.core.amount.mul(item.price, quantity),
                    cost=None,
                    price=None,
                    flag=None,
                    meta=meta,
                ))

        for adjustment in shipment.pretax_adjustments + shipment.tax:
            if adjustment.amount.number == ZERO:
                continue
            txn.postings.append(
                Posting(
                    account=unknown_account_name,
                    units=adjustment.amount,
                    cost=None,
                    price=None,
                    flag=None,
                    meta=collections.OrderedDict([
                        (INVOICE_DESCRIPTION, adjustment.description),
                    ]),
                ))
    # New Amazon invoices do not have shipment-level tax and pretax-adjustment
    # info, so we have to add these at invoice level. (We detect this situation
    # by the absence of even a subtotal for any shipment, since lack of
    # adjustments or tax might otherwise be normal.)
    if all([s.items_subtotal is None for s in invoice.shipments]):
        # If there's only one shipment, we can automatically attribute tax and
        # pretax adjustments to that one. Otherwise we need to use an ungrouped
        # unknown account.
        unknown_account_name = FIXME_ACCOUNT
        if len(invoice.shipments) == 1:
          unknown_account_name += ':A'

        for adjustment in invoice.pretax_adjustments:
            amount = adjustment.amount
            if amount.number == ZERO:
                continue
            txn.postings.append(
                Posting(
                    account=unknown_account_name,
                    units=adjustment.amount,
                    cost=None,
                    price=None,
                    flag=None,
                    meta=collections.OrderedDict([
                        (INVOICE_DESCRIPTION, adjustment.description),
                    ]),
                ))
        if invoice.tax is not None and invoice.tax.number != ZERO:
            txn.postings.append(
                Posting(
                    account=unknown_account_name,
                    units=invoice.tax,
                    cost=None,
                    price=None,
                    flag=None,
                    meta=collections.OrderedDict([
                        (INVOICE_DESCRIPTION, 'Sales Tax')
                    ]),
                ))
    for adjustment in invoice.posttax_adjustments:
        amount = adjustment.amount
        if amount.number == ZERO:
            continue
        txn.postings.append(
            Posting(
                account=posttax_adjustment_accounts.get(adjustment.description,
                                                        FIXME_ACCOUNT),
                units=amount,
                cost=None,
                price=None,
                flag=None,
                meta=collections.OrderedDict([
                    (POSTTAX_DESCRIPTION_KEY, adjustment.description),
                ]),
            ))
    for payment in invoice.credit_card_transactions:
        txn.postings.append(
            Posting(
                account=credit_card_accounts.get(payment.card_ending_in,
                                                 FIXME_ACCOUNT),
                units=-payment.amount,
                cost=None,
                price=None,
                flag=None,
                meta=collections.OrderedDict([
                    (POSTING_TRANSACTION_DATE_KEY, payment.date),
                    (CREDIT_CARD_DESCRIPTION_KEY, '%s ending in %s' %
                     (payment.card_description, payment.card_ending_in)),
                ]),
            ))
    # If there is a balance remaining, add a FIXME transaction.
    inventory = SimpleInventory()
    for posting in txn.postings:
        inventory += posting.units
    for currency in inventory:
        txn.postings.append(
            Posting(
                account=FIXME_ACCOUNT,
                units=Amount(currency=currency, number=-inventory[currency]),
                cost=None,
                price=None,
                flag=None,
                meta={},
            ))
    return txn


def get_credit_card_accounts(journal: JournalEditor) -> Dict[str, str]:
    accounts = dict()
    for entry in journal.accounts.values():
        if entry.meta and CREDIT_CARD_LAST_4_KEY in entry.meta:
            last_4_list_str = entry.meta[CREDIT_CARD_LAST_4_KEY].split()
            for last_4 in last_4_list_str:
                accounts[last_4] = entry.account
    return accounts


def _get_entry_order_id(entry: Directive, amazon_account: str):
    if not isinstance(entry, Transaction): return None
    meta = entry.meta
    if meta is None: return None
    if meta.get(AMAZON_ACCOUNT_KEY) != amazon_account:
        return None
    order_id = meta.get(ORDER_ID_KEY)
    return order_id


def get_order_ids_seen(journal: JournalEditor,
                       amazon_account: str) -> Dict[str, List[Transaction]]:
    order_ids = dict()  # type: Dict[str, List[Transaction]]
    for entry in journal.all_entries:
        order_id = _get_entry_order_id(entry, amazon_account)
        if not order_id: continue
        order_ids.setdefault(order_id, []).append(entry)
    return order_ids

class AmazonPickler():
    def __init__( self, pickle_dir: Optional[str] ):
        self.pickle_dir = pickle_dir
        if pickle_dir is not None and not os.access(pickle_dir, os.W_OK):
            raise Exception("Amazon pickled invoice path is not writable: %s" % pickle_dir)
        
    @staticmethod
    def try_get_mtime( path: str ):
        try:
            return os.stat(path).st_mtime
        except:
            return None

    def _build_pickle_path( self, pickle_dir: str, invoice_path: str ):
        invoice_dir, invoice_file = os.path.split(invoice_path)
        pickle_file = invoice_file.replace(".html", ".order.p")
        return os.path.join(pickle_dir, pickle_file)

    def load( self, results: SourceResults, invoice_path: str ):
        if not self.pickle_dir: return None

        pickle_path = self._build_pickle_path( self.pickle_dir, invoice_path )

        try:
            invoice_mtime = AmazonPickler.try_get_mtime( invoice_path )
            pickle_mtime  = AmazonPickler.try_get_mtime( pickle_path  )

            if invoice_mtime is None or pickle_mtime is None: return None
            if pickle_mtime < invoice_mtime: return None

            with open(pickle_path, "rb") as f:
                return pickle.load( f )
        except:
            results.add_error('Failed to load pickled invoice %s: %s' % (
                        pickle_path, sys.exc_info()))

    def dump( self, results: SourceResults, invoice_path: str, invoice: Optional[Order]):
        if not self.pickle_dir: return None
        if not invoice: return None

        try:
            pickle_path = self._build_pickle_path( self.pickle_dir, invoice_path )

            if invoice is None:
                # remove existing pickles if invoice couldn't be parsed
                pickle_mtime = AmazonPickler.try_get_mtime( pickle_path ) 
                if pickle_mtime: os.remove( pickle_path )
                return

            with open(pickle_path, "wb") as f:
                return pickle.dump( invoice, f )

        except:
            results.add_error('Failed to save pickled invoice %s: %s' % (
                        pickle_path, sys.exc_info()))

class AmazonSource(Source):
    def __init__(self,
                 directory: str,
                 amazon_account: str,
                 posttax_adjustment_accounts: Dict[str, str] = {},
                 pickle_dir: Optional[str] = None,
                 earliest_date: Optional[datetime.date] = None,
                 locale='en_US',
                 **kwargs) -> None:
        super().__init__(**kwargs)
        self.directory = directory
        self.amazon_account = amazon_account
        self.posttax_adjustment_accounts = posttax_adjustment_accounts
        self.example_posting_key_extractors[ITEM_DESCRIPTION_KEY] = None
        self.example_posting_key_extractors[CREDIT_CARD_DESCRIPTION_KEY] = None
        self.example_posting_key_extractors[POSTTAX_DESCRIPTION_KEY] = None
        self.example_transaction_key_extractors[AMAZON_ACCOUNT_KEY] = None
        self.pickler = AmazonPickler(pickle_dir)

        self.earliest_date = earliest_date
        self.locale = LOCALES[locale]

        self.invoice_filenames = []  # type: List[Tuple[str, str]]
        for filename in os.listdir(self.directory):
            suffix = '.html'
            if not filename.endswith(suffix):
                continue
            order_id = filename[:-len(suffix)]
            self.invoice_filenames.append((order_id, filename))
        self._cached_invoices = {
        }  # type: Dict[str, Tuple[Optional[Order], str]]

    def _get_invoice(self, results: SourceResults, order_id: str, invoice_filename: str):
        if invoice_filename in self._cached_invoices:
            return self._cached_invoices.get(invoice_filename)
        invoice_path = os.path.realpath(os.path.join(self.directory, invoice_filename))

        invoice = self.pickler.load(results, invoice_path) # type: Optional[Order]
        if invoice is None:
            self.log_status('amazon: processing %s: %s' % (order_id, invoice_path, ))
            invoice = parse_invoice(invoice_path, locale=self.locale)
            self.pickler.dump( results, invoice_path, invoice )

        self._cached_invoices[invoice_filename] = invoice, invoice_path
        return invoice, invoice_path

    def prepare(self, journal: JournalEditor, results: SourceResults):
        credit_card_accounts = get_credit_card_accounts(journal)
        order_ids_seen = get_order_ids_seen(journal, self.amazon_account)
        for order_id, transactions in order_ids_seen.items():
            if len(transactions) > 1:
                results.add_invalid_reference(
                    InvalidSourceReference(
                        len(transactions) - 1,
                        [(t, None) for t in transactions]))

        for order_id, invoice_filename in self.invoice_filenames:
            if order_id in order_ids_seen: continue
            try:
              invoice, path = self._get_invoice(results, order_id, invoice_filename)
            except:
                results.add_error('Failed to parse invoice %s: %s' % (
                    invoice_filename, sys.exc_info()))
                continue
            if invoice is None:
              continue

            if self.earliest_date is not None and invoice.order_date < self.earliest_date:
                self.log_status("Skipping order with date [%s] before [%s]" % ( str(invoice.order_date), self.earliest_date ) )
                continue

            transaction = make_amazon_transaction(
                invoice=invoice,
                posttax_adjustment_accounts=self.posttax_adjustment_accounts,
                amazon_account=self.amazon_account,
                credit_card_accounts=credit_card_accounts,
                payee=self.locale.payee)
            results.add_pending_entry(
                ImportResult(
                    date=transaction.date,
                    info=dict(
                        type='text/html',
                        filename=path,
                    ),
                    entries=[transaction],
                ))

    @property
    def name(self):
        return 'amazon'

    def get_associated_data(self,
                            entry: Directive) -> Optional[List[AssociatedData]]:
        order_id = _get_entry_order_id(entry, self.amazon_account)
        if not order_id: return None
        return [
            AssociatedData(
                meta=(ORDER_ID_KEY, order_id),
                description='Amazon order invoice',
                type='text/html',
                path=os.path.realpath(
                    os.path.join(self.directory, order_id + '.html')),
            ),
        ]


def load(spec, log_status):
    return AmazonSource(log_status=log_status, **spec)
