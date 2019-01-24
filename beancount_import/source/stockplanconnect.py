"""Stockplanconnect release/trade transaction source.

Data format
===========

To use, first download PDF release and trade confirmations into a directory on
the filesystem either manually or using the `finance_dl.stockplanconnect`
module.

You might have a directory structure like:

    financial/
      documents/
        stockplanconnect/
          %Y-%m-%d.Restricted_Units.Trade_Confirmations.Confirmation.pdf
          %Y-%m-%d.Restricted_Units.Trade_Confirmations.Confirmation.<N>.pdf
          %Y-%m-%d.Restricted_Units.Trade_Confirmations.Release_Confirmation.pdf
          %Y-%m-%d.Restricted_Units.Trade_Confirmations.Release_Confirmation.<N>.pdf

where `<N>` is a base-10 number.  Only filenames with these patterns are
recognized.

Specifying the source to beancount_import
=========================================

Within your Python script for invoking beancount_import, you might use an
expression like the following to specify the Stockplanconnect source:

    dict(
        module='beancount_import.source.stockplanconnect',
        payee='My Company',
        directory=os.path.join(journal_dir,
                               'documents', 'stockplanconnect'),
        income_account='Income:MyCompany:Equity',
        capital_gains_account='Income:Morgan-Stanley:Capital-Gains',
        fees_account='Income:Expenses:Financial:Investment-Fees:Morgan-Stanley',
        asset_account='Assets:Investment:Morgan-Stanley:MyCompany',
    )

Optionally, you may also specify a `tax_accounts` key with value like:

        tax_accounts=collections.OrderedDict([
            ('Federal Tax', 'Income:Expenses:Taxes:TY{year:04d}:Federal:Income'),
            ('State Tax', 'Income:Expenses:Taxes:TY{year:04d}:California:Income'),
            ('Medicare Tax', 'Income:Expenses:Taxes:TY{year:04d}:Federal:Medicare'),
        ]),

However, if you are also importing payroll statements that include the tax
breakdown as well, it works better to leave `tax_accounts` unspecified.

"""

from typing import Union, Optional, List, Set, Dict, Tuple, Any
import datetime
import os
import re
import collections

from beancount.core.data import Open, Transaction, Posting, Amount, Pad, Balance, Directive, EMPTY_SET, Entries
from beancount.core.amount import sub as amount_sub
from beancount.core.position import CostSpec
from beancount.core.number import D, ZERO
from beancount.core.number import MISSING
from beancount_import.posting_date import POSTING_DATE_KEY
from beancount_import.amount_parsing import parse_amount
from beancount_import.matching import FIXME_ACCOUNT
from beancount_import.source import ImportResult, Source, InvalidSourceReference, SourceResults, AssociatedData, LogFunction

from .stockplanconnect_statement import Release, TradeConfirmation, get_document_type

AWARD_NOTE_KEY = 'stock_award_note'
AWARD_ID_KEY = 'stock_award_id'
TRADE_REFERENCE_NUMBER_KEY = 'trade_ref_number'


def load_documents(directory: str, log_status: LogFunction):
    releases = []
    trades = []
    for name in sorted(os.listdir(directory)):
        path = os.path.join(directory, name)
        class_type = get_document_type(path)
        if class_type is None: continue
        log_status('stockplanconnect_source: loading %s' % name)
        doc = class_type(path)
        if class_type is Release:
            releases.append(doc)
        else:
            trades.append(doc)
    return releases, trades


class StockplanconnectSource(Source):
    def __init__(self,
                 directory: str,
                 income_account: str,
                 asset_account: str,
                 capital_gains_account: str,
                 fees_account: str,
                 payee: str,
                 tax_accounts: Optional[Dict[str, str]] = None,
                 **kwargs):
        super().__init__(**kwargs)
        self.directory = directory
        self.releases, self.trades = load_documents(directory, self.log_status)
        self.income_account = income_account
        self.asset_account = asset_account
        self.asset_cash_account = asset_account + ':Cash'
        self.fees_account = fees_account
        self.capital_gains_account = capital_gains_account
        self.tax_accounts = tax_accounts
        self.payee = payee

        def check_for_duplicates(documents, get_key):
            result = dict()
            documents_without_duplicates = []
            for x in documents:
                key = get_key(x)
                if key in result:
                    # raise RuntimeError('Duplicate document found: existing=%r, new=%r' %
                    #                    (result[key].path, x.path))
                    continue
                documents_without_duplicates.append(x)
                result[key] = x
            return result, documents_without_duplicates

        # Maps (income_account, release_date) pair to release object
        self.release_dates, self.releases = check_for_duplicates(
            self.releases, self.get_release_key)
        self.release_stock_posting_keys = set(
            self.get_stock_posting_key(r) for r in self.releases
            if r.net_release_shares is not None)
        self.trade_keys, self.trades = check_for_duplicates(
            self.trades, self.get_trade_key)

        self.expected_trade_transfers = {(t.settlement_date,
                                          t.reference_number): t
                                         for t in self.trades}
        self.expected_release_transfers = {(r.settlement_date or r.release_date,
                                            r.award_id): r
                                           for r in self.releases}

        self.income_accounts = set(
            self.get_income_account(r) for r in self.releases)
        self.managed_accounts = set(self.income_accounts)
        self.managed_accounts.add(self.asset_cash_account)
        self.stock_accounts = set(
            self.asset_account + ':' + r.symbol for r in self.releases)
        self.managed_accounts.update(self.stock_accounts)

    def get_income_account(self, r: Release):
        return '%s:%s:%s' % (self.income_account, r.award_id, r.symbol)

    def get_stock_account(self, x: Union[Release, TradeConfirmation]):
        return self.asset_account + ':' + x.symbol

    def get_stock_posting_key(self, r: Release):
        stock_account = '%s:%s' % (self.asset_account, r.symbol)
        return (stock_account, r.release_date, r.award_id)

    def get_release_key(self, r: Release):
        return (self.get_income_account(r), r.release_date)

    def get_trade_key(self, r: TradeConfirmation):
        return (self.get_stock_account(r), r.trade_date, r.reference_number)

    def _preprocess_entries(self, entries: Entries):
        seen_releases = dict(
        )  # type: Dict[Tuple[str, Optional[datetime.date]], List[Tuple[Transaction, Posting]]]
        seen_release_stock_postings = dict(
        )  # type: Dict[Tuple[str, datetime.date, Optional[str]], List[Tuple[Transaction, Posting]]]
        seen_trades = dict(
        )  # type: Dict[Tuple[str, datetime.date, Any], List[Tuple[Transaction, Posting]]]
        seen_release_transfers = dict(
        )  # type: Dict[Tuple[datetime.date, str], List[Tuple[Transaction, Posting]]]
        seen_trade_transfers = dict(
        )  # type: Dict[Tuple[datetime.date, str], List[Tuple[Transaction, Posting]]]
        income_account_prefix = self.income_account + ':'
        for entry in entries:
            if not isinstance(entry, Transaction): continue
            for posting in entry.postings:
                if posting.account.startswith(income_account_prefix):
                    date = (posting.meta.get(POSTING_DATE_KEY)
                            if posting.meta is not None else None)
                    seen_releases.setdefault((posting.account, date),
                                             []).append((entry, posting))
                elif posting.account in self.stock_accounts:
                    date = (posting.meta.get(POSTING_DATE_KEY)
                            if posting.meta is not None else None)
                    if posting.units.number > ZERO:
                        seen_release_stock_postings.setdefault(
                            (posting.account, date,
                             (posting.cost or posting.cost_spec).label),
                            []).append((entry, posting))
                    else:
                        ref = posting.meta.get(TRADE_REFERENCE_NUMBER_KEY)
                        seen_trades.setdefault((posting.account, date, ref),
                                               []).append((entry, posting))
                elif posting.account == self.asset_cash_account:
                    date = (posting.meta.get(POSTING_DATE_KEY)
                            if posting.meta is not None else None)
                    ref = (posting.meta.get(TRADE_REFERENCE_NUMBER_KEY)
                           if posting.meta is not None else None)
                    if ref is not None and ref.startswith('>'):
                        seen_trade_transfers.setdefault(
                            (date, ref[1:]), []).append((entry, posting))
                    award_id = posting.meta.get(AWARD_ID_KEY)
                    if award_id is not None and award_id.startswith('>'):
                        seen_release_transfers.setdefault(
                            (date, award_id[1:]), []).append((entry, posting))

        return seen_releases, seen_release_stock_postings, seen_trades, seen_release_transfers, seen_trade_transfers

    def _make_journal_entry(self, r: Release):
        txn = Transaction(
            meta=collections.OrderedDict(),
            date=r.release_date,
            flag='*',
            payee=self.payee,
            narration='Stock Vesting',
            tags=EMPTY_SET,
            links=EMPTY_SET,
            postings=[])
        txn.postings.append(
            Posting(
                account=self.get_income_account(r),
                units=-r.amount_released,
                cost=None,
                meta={POSTING_DATE_KEY: r.release_date},
                price=r.vest_price,
                flag=None,
            ))

        vest_cost_spec = CostSpec(
            number_per=r.vest_price.number,
            currency=r.vest_price.currency,
            number_total=None,
            date=r.vest_date,
            label=r.award_id,
            merge=False)

        txn.postings.append(
            Posting(
                account=self.asset_account + ':Cash',
                units=r.released_market_value_minus_taxes,
                cost=None,
                meta={
                    POSTING_DATE_KEY: r.release_date,
                    AWARD_NOTE_KEY:
                    'Market value of vested shares minus taxes.',
                    AWARD_ID_KEY: r.award_id,
                },
                price=None,
                flag=None))

        if r.net_release_shares is not None:
            # Shares were retained
            txn.postings.append(
                Posting(
                    account=self.asset_account + ':' + r.symbol,
                    units=r.net_release_shares,
                    cost=vest_cost_spec,
                    meta={
                        POSTING_DATE_KEY: r.release_date,
                        AWARD_ID_KEY: r.award_id,
                    },
                    price=None,
                    flag=None))
            txn.postings.append(
                Posting(
                    account=self.asset_account + ':Cash',
                    units=-Amount(
                        number=round(
                            r.vest_price.number * r.net_release_shares.number,
                            2),
                        currency=r.vest_price.currency),
                    cost=None,
                    meta={
                        POSTING_DATE_KEY: r.release_date,
                        AWARD_NOTE_KEY: 'Cost of shares retained',
                        AWARD_ID_KEY: r.award_id,
                    },
                    price=None,
                    flag=None,
                ))
        else:
            # Shares were sold

            # Add capital gains posting.
            txn.postings.append(
                Posting(
                    meta=None,
                    account=self.capital_gains_account + ':' + r.symbol,
                    units=-r.capital_gains,
                    cost=None,
                    price=None,
                    flag=None,
                ))

            capital_gains_amount = r.capital_gains
            if r.fee_amount is not None:
                capital_gains_amount = amount_sub(capital_gains_amount,
                                                  r.fee_amount)

            # Add cash posting for capital gains.
            txn.postings.append(
                Posting(
                    account=self.asset_account + ':Cash',
                    units=capital_gains_amount,
                    cost=None,
                    meta={
                        POSTING_DATE_KEY: r.release_date,
                        AWARD_NOTE_KEY: 'Capital gains less transaction fees',
                        AWARD_ID_KEY: r.award_id,
                    },
                    price=None,
                    flag=None,
                ))

        if r.fee_amount is not None:
            txn.postings.append(
                Posting(
                    account=self.fees_account,
                    units=r.fee_amount,
                    cost=None,
                    meta={
                        POSTING_DATE_KEY: r.release_date,
                        AWARD_NOTE_KEY: 'Supplemental transaction fee',
                        AWARD_ID_KEY: r.award_id,
                    },
                    price=None,
                    flag=None,
                ))

        if self.tax_accounts is None:
            # Just use a single unknown account to catch all of the tax costs.
            # This allows the resultant entry to match a payroll entry that includes the tax costs.
            txn.postings.append(
                Posting(
                    account=FIXME_ACCOUNT,
                    units=r.total_tax_amount,
                    cost=None,
                    meta=dict(),
                    price=None,
                    flag=None,
                ))
        else:
            for tax_key, tax_account_pattern in self.tax_accounts.items():
                if tax_key not in r.fields:
                    continue
                amount = parse_amount(r.fields[tax_key])
                account = tax_account_pattern.format(year=r.release_date.year)
                txn.postings.append(
                    Posting(
                        account=account,
                        units=amount,
                        cost=None,
                        meta={POSTING_DATE_KEY: r.release_date},
                        price=None,
                        flag=None,
                    ))
        return txn

    def _make_transfer_journal_entry(self, r: Release):
        date = r.settlement_date or r.release_date
        return Transaction(
            meta=collections.OrderedDict(),
            date=date,
            flag='*',
            payee=self.payee,
            narration='Stock Vesting - %s' % r.transfer_description,
            tags=EMPTY_SET,
            links=EMPTY_SET,
            postings=[
                Posting(
                    account=self.asset_cash_account,
                    units=-r.transfer_amount,
                    cost=None,
                    meta=collections.OrderedDict([
                        (POSTING_DATE_KEY, date),
                        (AWARD_ID_KEY, '>' + r.award_id),
                        (AWARD_NOTE_KEY, r.transfer_description),
                    ]),
                    price=None,
                    flag=None,
                ),
                Posting(
                    account=FIXME_ACCOUNT,
                    units=r.transfer_amount,
                    cost=None,
                    meta=None,
                    price=None,
                    flag=None,
                ),
            ])

    def _make_transfer_trade_journal_entry(self, t: TradeConfirmation):
        return Transaction(
            meta=collections.OrderedDict(),
            date=t.settlement_date,
            flag='*',
            payee=self.payee,
            narration='Transfer due to stock sale',
            tags=EMPTY_SET,
            links=EMPTY_SET,
            postings=[
                Posting(
                    account=self.asset_cash_account,
                    units=-t.net_amount,
                    cost=None,
                    meta=collections.OrderedDict([
                        (POSTING_DATE_KEY, t.settlement_date),
                        (TRADE_REFERENCE_NUMBER_KEY, '>' + t.reference_number),
                    ]),
                    price=None,
                    flag=None,
                ),
                Posting(
                    account=FIXME_ACCOUNT,
                    units=t.net_amount,
                    cost=None,
                    meta=None,
                    price=None,
                    flag=None,
                ),
            ])

    def _make_trade_journal_entry(self, t: TradeConfirmation):
        txn = Transaction(
            meta=collections.OrderedDict(),
            date=t.settlement_date,
            flag='*',
            payee=self.payee,
            narration='Sell',
            tags=EMPTY_SET,
            links=EMPTY_SET,
            postings=[])
        txn.postings.append(
            Posting(
                account=self.get_stock_account(t),
                units=-t.quantity,
                cost=CostSpec(
                    number_per=MISSING,
                    number_total=None,
                    currency=t.gross_amount.currency,
                    date=None,
                    label=None,
                    merge=False),
                price=t.share_price,
                meta={
                    POSTING_DATE_KEY: t.trade_date,
                    TRADE_REFERENCE_NUMBER_KEY: t.reference_number
                },
                flag=None,
            ))
        txn.postings.append(
            Posting(
                account=self.capital_gains_account + ':' + t.symbol,
                units=MISSING,
                meta=None,
                cost=None,
                price=None,
                flag=None,
            ))
        txn.postings.append(
            Posting(
                account=self.fees_account,
                units=t.fees,
                cost=None,
                meta={
                    POSTING_DATE_KEY: t.trade_date,
                    TRADE_REFERENCE_NUMBER_KEY: t.reference_number,
                },
                price=None,
                flag=None,
            ))
        txn.postings.append(
            Posting(
                account=self.asset_cash_account,
                units=t.net_amount,
                cost=None,
                meta=None,
                price=None,
                flag=None,
            ))
        return txn

    def is_posting_cleared(self, posting: Posting):
        if posting.meta is None:
            return False
        if posting.account.startswith('Income:'):
            return True
        return ((AWARD_ID_KEY in posting.meta or
                 TRADE_REFERENCE_NUMBER_KEY in posting.meta) and
                POSTING_DATE_KEY in posting.meta)

    def prepare(self, journal, results: SourceResults):
        seen_releases, seen_release_stock_postings, seen_trades, seen_release_transfers, seen_trade_transfers = self._preprocess_entries(
            journal.all_entries)

        for seen_dict, valid_set in (
            (seen_releases, self.release_dates),
            (seen_release_stock_postings, self.release_stock_posting_keys),
            (seen_trades, self.trade_keys),
            (seen_release_transfers, self.expected_release_transfers),
            (seen_trade_transfers, self.expected_trade_transfers),
        ):
            for seen_key, pairs in seen_dict.items():
                expected = 1 if seen_key in valid_set else 0
                num_extra = len(pairs) - expected
                if num_extra != 0:
                    results.add_invalid_reference(
                        InvalidSourceReference(num_extra, pairs))

        for r in self.releases:
            key = self.get_release_key(r)
            if key not in seen_releases:
                results.add_pending_entry(
                    self._make_import_result(self._make_journal_entry(r), r))
            if r.transfer_amount is not None and (
                    r.settlement_date or r.release_date,
                    r.award_id) not in seen_release_transfers:
                results.add_pending_entry(
                    self._make_import_result(
                        self._make_transfer_journal_entry(r), r))
        for r in self.trades:
            key = self.get_trade_key(r)
            if key not in seen_trades:
                results.add_pending_entry(
                    self._make_import_result(
                        self._make_trade_journal_entry(r), r))
            if (r.settlement_date,
                    r.reference_number) not in seen_trade_transfers:
                results.add_pending_entry(
                    self._make_import_result(
                        self._make_transfer_trade_journal_entry(r), r))
        results.add_accounts(self.managed_accounts)

    def _make_import_result(self, txn: Transaction,
                            x: Union[Release, TradeConfirmation]):
        return ImportResult(
            date=txn.date,
            entries=[txn],
            info=dict(type='application/pdf', filename=os.path.abspath(x.path)))

    @property
    def name(self):
        return 'stockplanconnect'

    def get_associated_data(self,
                            entry: Directive) -> Optional[List[AssociatedData]]:
        if not isinstance(entry, Transaction): return None
        result = []  # type: List[AssociatedData]
        already_seen = set()  # type: Set[int]
        for posting in entry.postings:
            if posting.account in self.income_accounts and posting.meta is not None:
                date = posting.meta.get(POSTING_DATE_KEY)
                if date is not None:
                    release = self.release_dates.get((posting.account, date))
                    if release is not None and id(release) not in already_seen:
                        already_seen.add(id(release))
                        result.append(
                            AssociatedData(
                                meta=(POSTING_DATE_KEY, date),
                                posting=posting,
                                description='Release confirmation',
                                type='application/pdf',
                                path=os.path.realpath(release.path),
                            ))
            elif posting.account in self.stock_accounts and posting.meta is not None:
                date = posting.meta.get(POSTING_DATE_KEY)
                if posting.units.number > ZERO:
                    pass
                else:
                    ref = posting.meta.get(TRADE_REFERENCE_NUMBER_KEY)
                    trade = self.trade_keys.get((posting.account, date, ref))
                    if trade is not None and id(trade) not in already_seen:
                        already_seen.add(id(trade))
                        result.append(
                            AssociatedData(
                                meta=(TRADE_REFERENCE_NUMBER_KEY, ref),
                                posting=posting,
                                description='Trade confirmation',
                                type='application/pdf',
                                path=os.path.realpath(trade.path),
                            ))
            elif posting.account == self.asset_cash_account:
                date = posting.meta.get(POSTING_DATE_KEY)
                ref = posting.meta.get(TRADE_REFERENCE_NUMBER_KEY)
                if ref is not None and ref.startswith('>'):
                    trade = self.trade_keys.get((date, ref[1:]))
                    if trade is not None and id(trade) not in already_seen:
                        already_seen.add(id(trade))
                        result.append(
                            AssociatedData(
                                meta=(TRADE_REFERENCE_NUMBER_KEY, ref),
                                posting=posting,
                                description='Trade confirmation',
                                type='application/pdf',
                                path=os.path.realpath(trade.path),
                            ))
                award_id = posting.meta.get(AWARD_ID_KEY)
                if award_id is not None and award_id.startswith('>'):
                    release = self.expected_release_transfers.get((date, award_id[1:]))
                    if release is not None and id(release) not in already_seen:
                        already_seen.add(id(release))
                        result.append(
                            AssociatedData(
                                meta=(AWARD_ID_KEY, award_id),
                                posting=posting,
                                description='Release confirmation',
                                type='application/pdf',
                                path=os.path.realpath(release.path),
                            ))

        return result


def load(spec, log_status):
    return StockplanconnectSource(log_status=log_status, **spec)
