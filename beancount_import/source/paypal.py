"""Paypal.com transaction source.

Data format
===========

To use, first download Paypal data into a directory on the filesystem using the
finance_dl.paypal module.

You might have a directory structure like:

    financial/
      data/
        paypal/
          <account_id>/
            <id>.json
            <id>.html


Specifying the source to beancount_import
=========================================

Within your Python script for invoking beancount_import, you might use an
expression like the following to specify the venmo source:

    dict(module='beancount_import.source.paypal',
         directory=os.path.join(journal_dir, 'data', 'paypal', '<account_id>'),
         assets_account='Assets:Paypal',
         fee_account='Expenses:Financial:Paypal:Fees',
         prefix='paypal',
    )

where `journal_dir` refers to the financial/ directory.

The `directory` specifies the directory containing the `.json` and `.html`
files.  The `prefix` should be unique over all of your sources, and should
consist only of letters and underscores..  It is concatenated with '.' and the
transaction `id` to form a unique `link` to apply to the generated transaction
that associates it with the transaction data, and also serves as a prefix for
various metadata keys.  The account in your Beancount journal corresponding to
your Paypal balance should be specified as the `assets_account`.  The
`fee_account` is the Expense account for fees charged by Paypal.

Imported transaction format
===========================

Payment transaction with item details:

    2016-11-27 * "eBay - abcdef (abcdef)" "Payment" ^paypal.Q7K91N1B9WI6F4RW6
      Expenses:FIXME:A   25.98 USD
        paypal_counterparty: "eBay - abcdef (abcdef)"
        paypal_item_name: "Men's Jeans"
        paypal_item_number: "123457890123"
        paypal_item_quantity: 2
        paypal_item_url: "http://cgi.ebay.com/ws/eBayISAPI.dll?ViewItem&item=123457890123"
        paypal_merchant_category: "Retail"
      Expenses:FIXME:A    2.34 USD
        paypal_counterparty: "eBay - abcdef (abcdef)"
        paypal_item_type: "salesTax"
        paypal_merchant_category: "Retail"
      Expenses:FIXME:A   13.00 USD
        paypal_counterparty: "eBay - abcdef (abcdef)"
        paypal_item_type: "shippingAmount"
        paypal_merchant_category: "Retail"
      Expenses:FIXME    -41.32 USD
        paypal_funding_source_description: "Chase Visa"
        paypal_funding_source_institution: "VISA"
        paypal_funding_source_last4: "1234"

Sending money:

    2012-10-16 * "Payment Administrator" "Money Sent" ^paypal.JNNTICIP9966PELUB
      Expenses:FIXME:A   12.70 USD
        paypal_counterparty: "Payment Administrator"
        paypal_counterparty_email: "payment@example.com"
      Assets:Paypal     -12.70 USD
        date: 2012-10-16
        paypal_transaction_id: "JNNTICIP9966PELUB"

Transfer from bank to Paypal balance:

    2012-10-16 * "Bank Account" "Transfer from Bank" ^paypal.RNPYPAWXEWBUUBNER
      Assets:Paypal    12.70 USD
        date: 2012-10-16
        paypal_transaction_id: "RNPYPAWXEWBUUBNER"
      Expenses:FIXME  -12.70 USD
        paypal_funding_source_institution: "My Bank"
        paypal_funding_source_last4: "1234"

Transfer from Paypal balance to bank:

    2014-09-09 * "Bank Account" "Transfer to Bank" ^paypal.266LYBQVWC7P8PXV0
      Assets:Paypal   -70.00 USD
        date: 2014-09-09
        paypal_transaction_id: "266LYBQVWC7P8PXV0"
      Expenses:FIXME   70.00 USD
        paypal_funding_source_institution: "My Bank"
        paypal_funding_source_last4: "1234"

Sending money with credit card:

    2016-05-15 * "John Smith" "Money Sent - Reimbursement for something" ^paypal.2RG4T9AJK3BT2DOV6
      Expenses:Financial:Paypal:Fees    2.62 USD
      Expenses:FIXME:A                 80.00 USD
        paypal_counterparty: "John Smith"
        paypal_counterparty_email: "john.smith@example.com"
        paypal_note: "Reimbursement for something"
      Expenses:FIXME                  -82.62 USD
        paypal_funding_source_description: "Debit Card"
        paypal_funding_source_institution: "VISA"
        paypal_funding_source_last4: "1234"


"""
from typing import Dict, List, Tuple, Optional, Any

import collections
import os
import re
import json

import jsonschema
import dateutil.parser

from beancount.core.data import Transaction, Posting, Balance, Commodity, Price, EMPTY_SET, Directive, Entries, Meta
from beancount.core.amount import Amount
from beancount.core.flags import FLAG_OKAY
from beancount.core.number import ZERO, ONE, D
import beancount.core.amount

from . import ImportResult, Source, SourceResults, InvalidSourceReference, AssociatedData
from .link_based_source import LinkBasedSource
from ..posting_date import POSTING_DATE_KEY
from ..journal_editor import JournalEditor
from ..matching import FIXME_ACCOUNT, SimpleInventory
from ..amount_parsing import parse_amount

transaction_schema = {
    '#schema': 'http://json-schema.org/draft-07/schema#',
    'description': 'JSON schema for the transaction details.',
    'type': 'object',
    'properties': {
        "fundingSource": {
            "type": "object",
            "properties": {
                "fundingSourceList": {
                    "type": "array",
                    "items": {
                        "type": "object",
                        "properties": {
                            "type": {
                                "type": "string"
                            },
                            "last4": {
                                "type": "string"
                            },
                            "institution": {
                                "type": "string"
                            },
                            "issuer_product_description": {
                                "type": "string"
                            },
                            "statementName": {
                                "type": "string"
                            },
                            "amount": {
                                "type": "string"
                            },
                            "currencyCode": {
                                "type": "string"
                            }
                        },
                        "required": ["amount", "currencyCode", "type"]
                    }
                },
                "isMultipleFundingSourceUsed": {
                    "type": "boolean"
                }
            },
            "required": ["fundingSourceList"]
        },
        "notesInfo": {
            "type": "object",
            "properties": {
                "context": {
                    "type": "string"
                },
                "note": {
                    "type": "string"
                }
            },
            "required": ["context", "note"]
        },
        "merchantCategory": {
            "type": "string"
        },
        "invoiceId": {
            "type": "string"
        },
        "itemDetails": {
            "type": "object",
            "properties": {
                "itemList": {
                    "type": "array",
                    "items": {
                        "type": "object",
                        "properties": {
                            "name": {
                                "type": "string"
                            },
                            "number": {
                                "type": "string"
                            },
                            "price": {
                                "type": "string"
                            },
                            "isNegative": {
                                "type": "boolean"
                            },
                            "itemTotalPrice": {
                                "type": "string"
                            },
                            "url": {
                                "type": "string"
                            },
                            "quantity": {
                                "type": "integer"
                            },
                            "description": {
                                "type": "string"
                            }
                        },
                        "required": ["itemTotalPrice", "name"]
                    }
                },
                "salesTax": {
                    "type": "string"
                },
                "shippingAmount": {
                    "type": "string"
                },
                "itemTotalAmount": {
                    "type": "string"
                },
                "discount": {
                    "type": "array",
                    "items": {
                        "type": "object",
                        "properties": {
                            "name": {
                                "type": "string"
                            },
                            "value": {
                                "type": "string"
                            }
                        },
                        "required": ["name", "value"]
                    }
                }
            },
            "required": ["itemList"]
        },
    },
    "transactionType": {
        "type": "string"
    },
    "counterparty": {
        "type": "object",
        "properties": {
            "detailsCounterpartyText": {
                "type": "string"
            },
            "name": {
                "type": "string"
            },
            "email": {
                "type": "string"
            },
            "url": {
                "type": "string"
            },
            "phone": {
                "type": "string"
            },
            "isBusiness": {
                "type": "boolean"
            }
        },
        "required": ["detailsCounterpartyText", "isBusiness", "name"]
    },
    "amount": {
        "type":
        "object",
        "properties": {
            "grossAmount": {
                "type": "string"
            },
            "netAmount": {
                "type": "string"
            },
            "fullPageNetAmount": {
                "type": "string"
            },
            "feeAmount": {
                "type": "string"
            },
            "grossExceedsNet": {
                "type": "boolean"
            },
            "isZeroFee": {
                "type": "boolean"
            },
            "feeLabel": {
                "type": "string"
            }
        },
        "required": [
            "feeAmount", "feeLabel", "fullPageNetAmount", "grossAmount",
            "grossExceedsNet", "isZeroFee", "netAmount"
        ]
    },
    "isCredit": {
        "type": "boolean"
    },
    "required": [
        "amount",
        "counterparty",
        "isCredit",
        "date",
        "transactionType",
    ],
}


class PaypalSource(LinkBasedSource, Source):
    def __init__(self, directory: str, assets_account: str, fee_account: str, prefix: str,
                 **kwargs) -> None:
        super().__init__(link_prefix=prefix + '.', **kwargs)
        self.directory = directory
        self.prefix = prefix
        self.assets_account = assets_account
        self.fee_account = fee_account
        self.example_transaction_key_extractors[prefix + '_counterparty'] = None
        self.example_posting_key_extractors[prefix + '_counterparty'] = None
        self.example_posting_key_extractors[prefix + '_counterparty_note'] = None
        self.example_posting_key_extractors[prefix + '_merchant_category'] = None
        self.example_posting_key_extractors[prefix + '_item_name'] = None
        self.example_posting_key_extractors[prefix + '_item_number'] = None
        self.example_posting_key_extractors[prefix + '_item_description'] = None
        self.example_posting_key_extractors[prefix + '_funding_source_institution'] = None
        self.example_posting_key_extractors[prefix + '_funding_source_description'] = None
        self.example_posting_key_extractors[prefix + '_funding_source_last4'] = None
        self.transaction_meta_key = prefix + '_transaction_id'

    def _make_import_result(self, txn_id: str, data: Dict[str, Any],
                            json_path: str):
        date = dateutil.parser.parse(data['date']).date()
        payee = data['counterparty']['name']
        narration = data['transactionType']

        txn_meta = collections.OrderedDict()  # type: Meta
        counterparty_metadata = [(self.prefix + '_counterparty', payee)]
        funding_source_metadata = []  # type: List[Tuple[str, Any]]
        for key in ('email', 'url', 'phone'):
            if key in data['counterparty']:
                value = data['counterparty'][key]
                if not value: continue
                counterparty_metadata.append((self.prefix + '_counterparty_' + key, value))
        if 'merchantCategory' in data:
            counterparty_metadata.append((self.prefix + '_merchant_category', data['merchantCategory']))
        if 'invoiceId' in data:
            counterparty_metadata.append((self.prefix + '_invoice_id', data['invoiceId']))
        if 'notesInfo' in data:
            note = data['notesInfo']['note']
            note = re.sub(r'\s+', ' ', note)
            narration += ' - ' + note
            counterparty_metadata.append((self.prefix + '_note', note))

        transaction = Transaction(
            meta=txn_meta,
            date=date,
            flag='*',
            payee=payee,
            narration=narration,
            links=frozenset([self.link_prefix + txn_id]),
            tags=EMPTY_SET,
            postings=[])
        is_credit = data['isCredit']
        counterparty_amount = parse_amount(data['amount']['grossAmount'])
        funding_source_amount = parse_amount(data['amount']['netAmount'])
        fee_amount = parse_amount(data['amount']['feeAmount'])
        transaction_type_enum = data['transactionTypeEnum']

        # Metadata added to postings to the `self.assets_account` account.
        assets_account_metadata = [
            (self.transaction_meta_key, txn_id),
            (POSTING_DATE_KEY, date),
        ]

        # If True, the posting legs to the funding source are negative, and all
        # other legs are positive.
        negate_funding_source_amounts = True

        counterparty_remainder_account = FIXME_ACCOUNT + ':A'

        if transaction_type_enum == 'TRANSFER_TO_BANK':
            counterparty_remainder_account = self.assets_account
            counterparty_metadata = []
            negate_funding_source_amounts = False
        elif transaction_type_enum == 'TRANSFER_FROM_BANK':
            counterparty_remainder_account = self.assets_account
            counterparty_metadata = []
            negate_funding_source_amounts = True

        elif transaction_type_enum.endswith('_SENT') or transaction_type_enum.endswith('_PURCHASE'):
            negate_funding_source_amounts = True

        elif transaction_type_enum.endswith('_RECEIVED'):
            negate_funding_source_amounts = False
        elif transaction_type_enum == 'MONEY_TRANSFER':
            counterparty_remainder_account = self.assets_account
            funding_source_metadata = counterparty_metadata
            counterparty_metadata = []
            negate_funding_source_amounts = is_credit
        elif transaction_type_enum == 'REFUND':
            negate_funding_source_amounts = False
        else:
            raise RuntimeError('Unknown transaction type: %s' % transaction_type_enum)

        negate_counterparty_amounts = not negate_funding_source_amounts


        if negate_funding_source_amounts:
            funding_source_amount = -funding_source_amount
        else:
            funding_source_amount = funding_source_amount

        counterparty_inventory = SimpleInventory()
        counterparty_inventory += counterparty_amount
        def add_counterparty_posting(amount,
                                     extra_meta=[],
                                     account=FIXME_ACCOUNT + ':A'):
            nonlocal counterparty_inventory
            if amount.number == ZERO:
                return
            counterparty_inventory -= amount
            if negate_counterparty_amounts:
                amount = -amount
            meta = counterparty_metadata + extra_meta
            if account == self.assets_account:
                meta.extend(assets_account_metadata)
            transaction.postings.append(
                Posting(
                    meta=collections.OrderedDict(meta),
                    account=account,
                    units=amount,
                    cost=None,
                    price=None,
                    flag=None,
                ))


        if fee_amount.number != ZERO:
            if negate_counterparty_amounts:
                amount = -fee_amount
            else:
                amount = fee_amount
            transaction.postings.append(
                Posting(
                    meta=collections.OrderedDict(),
                    account=self.fee_account,
                    units=amount,
                    cost=None,
                    price=None,
                    flag=None,
                ))

        if 'itemDetails' in data:
            for item in data['itemDetails']['itemList']:
                units = parse_amount(item['itemTotalPrice'])
                if 'quantity' in item:
                    quantity = D(item['quantity'])
                else:
                    quantity = None
                if units.number == ZERO and 'price' in item:
                    units = parse_amount(item['price'])
                extra_meta = [
                    (self.prefix + '_item_name', item['name']),
                ]
                for key in ('url', 'number', 'description'):
                    value = item.get(key, None)
                    if value:
                        extra_meta.append((self.prefix + '_item_%s' % key, value))
                if quantity is not None:
                    extra_meta.append((self.prefix + '_item_quantity', quantity))
                add_counterparty_posting(amount=units, extra_meta=extra_meta)
            for key in ('salesTax', 'shippingAmount'):
                if key in data['itemDetails']:
                    units = parse_amount(data['itemDetails'][key])
                    add_counterparty_posting(amount=units, extra_meta=[
                        (self.prefix + '_item_type', key),
                    ])

            if 'discount' in data['itemDetails']:
                for discount in data['itemDetails']['discount']:
                    units = -parse_amount(discount['value'])
                    add_counterparty_posting(amount=units, extra_meta=[
                        (self.prefix + '_item_discount', discount['name']),
                    ])
        counterparty_inventory_copy = counterparty_inventory.copy()
        for currency in counterparty_inventory_copy:
            add_counterparty_posting(
                Amount(
                    currency=currency, number=counterparty_inventory_copy[currency]),
                account=counterparty_remainder_account)

        funding_source_inventory = SimpleInventory()
        funding_source_inventory += funding_source_amount
        funding_source_account = FIXME_ACCOUNT
        if transaction_type_enum == 'SEND_MONEY_RECEIVED':
            funding_source_account = self.assets_account
            assert 'fundingSource' not in data
            funding_source_metadata = assets_account_metadata

        if 'fundingSource' in data:
            for source in data['fundingSource']['fundingSourceList']:
                meta = collections.OrderedDict()  # type: Meta
                account = FIXME_ACCOUNT
                source_type = source['type']
                if (source_type == 'BALANCE' or
                    (transaction_type_enum == 'SEND_MONEY_SENT' and
                     source_type != 'CREDIT_CARD')):
                    # For SEND_MONEY_SENT, sources other than CREDIT_CARD
                    # are actually handled by a separate transfer transaction.
                    account = self.assets_account
                    meta.update(assets_account_metadata)
                else:
                    for key, meta_suffix in [
                        ('issuer_product_description',
                         'funding_source_description'),
                        ('institution', 'funding_source_institution'),
                        ('last4', 'funding_source_last4'),
                    ]:
                        if key in source:
                            meta[self.prefix + '_' + meta_suffix] = source[key]
                # FIXME handle currencyCode
                units = parse_amount(source['amount'])
                if negate_funding_source_amounts:
                    units = -units
                funding_source_inventory -= units
                transaction.postings.append(
                    Posting(
                        meta=meta,
                        account=account,
                        units=units,
                        cost=None,
                        price=None,
                        flag=None,
                    ))

        for currency in funding_source_inventory:
            transaction.postings.append(
                Posting(
                    account=funding_source_account,
                    units=Amount(currency=currency, number=funding_source_inventory[currency]),
                    cost=None,
                    price=None,
                    flag=None,
                    meta=collections.OrderedDict(funding_source_metadata),
                ))

        return ImportResult(
            date=transaction.date,
            info=dict(
                type='application/json',
                filename=json_path,
            ),
            entries=[transaction])

    def prepare(self, journal: JournalEditor, results: SourceResults):
        transaction_json_suffix = '.json'
        invoice_json_suffix = '.invoice.json'
        transaction_ids = set(x[:-len(transaction_json_suffix)]
                              for x in os.listdir(self.directory)
                              if x.endswith(transaction_json_suffix) and
                              not x.endswith(invoice_json_suffix))
        seen_in_journal = self.get_entries_with_link(
            all_entries=journal.all_entries,
            results=results,
            valid_links=transaction_ids)
        for txn_id in sorted(transaction_ids):
            if txn_id in seen_in_journal: continue
            path = os.path.join(self.directory,
                                txn_id + transaction_json_suffix)
            self.log_status('paypal: processing %s' % (path, ))
            with open(path, 'r') as f:
                txn = json.load(f)
            jsonschema.validate(txn, transaction_schema)
            results.add_pending_entry(
                self._make_import_result(
                    txn_id=txn_id, data=txn, json_path=path))
        results.add_account(self.assets_account)

    def is_posting_cleared(self, posting: Posting):
        if posting.meta is None:
            return False
        return self.transaction_meta_key in posting.meta

    def _get_html_path(self, txn_id):
        return os.path.join(self.directory, txn_id + '.html')

    @property
    def name(self):
        return 'paypal'

    def get_associated_data_for_link(
            self, entry_id: str) -> Optional[List[AssociatedData]]:
        return [
            AssociatedData(
                description='Paypal transaction',
                type='text/html',
                path=self._get_html_path(entry_id),
            )
        ]


def load(spec, log_status):
    return PaypalSource(log_status=log_status, **spec)
