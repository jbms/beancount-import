"""Schwab.com brokerage transaction source.

Imports transactions from Schwab.com brokerage/banking history CSV files.

To use, first you have to download Schwab CSV data into a directory on your filesystem. If
you have a structure like this:

    financial/
      data/
        schwab/
          transactions/
          positions/

And you download your transaction history CSV into `transactions/` and your positions
statement CSV into `positions/`, then you could specify your beancount-import source like
this:

    dict(module="beancount_import.source.schwab_csv",
         transaction_csv_filenames=glob.glob("data/schwab/transactions/*.csv"),
         position_csv_filenames=glob.glob("data/schwab/positions/*.csv"),
    )

The importer can also optionally make use of Schwab's lot details CSV downloads in order
to correctly fill in cost-basis on stock sales, even for sales of stock from multiple
lots. For this to work reliably, you should ensure that you download the lot details
regularly (ideally at least once between each transaction involving a given commodity).
Downloading lot details CSV by hand could be quite tedious; the
[finance-dl](https://github.com/jbms/finance-dl) package is recommended.

To use lot details, add a `lots_csv_filenames` key to your beancount-import source.
Finance-dl will place lot details under `positions/lots/` with one directory per date
downloaded and one file per commodity. So your source spec might look like this:

    dict(module="beancount_import.source.schwab_csv",
         transaction_csv_filenames=glob.glob("data/schwab/transactions/*.csv"),
         position_csv_filenames=glob.glob("data/schwab/positions/*.csv"),
         lots_csv_filenames=glob.glob("data/schwab/positions/lots/*/*.csv"),
    )

This importer also makes use of certain metadata keys on your accounts. In order to label
a beancount account as a Schwab account whose authoritative transaction source is this
importer, specify the `schwab_account` metadata key as the account ID exactly as it
appears in your Schwab CSV downloads. For example:

    2015-11-09 open Assets:Investments:Schwab:Brokerage-1234
         schwab_account: "XXXX-1234"

You can also optionally specify accounts to be used for recording dividends, capital
gains, interest, fees, and taxes:

    2015-11-09 open Assets:Investments:Schwab:Brokerage-1234
         schwab_account: "XXXX-1234"
         div_income_account: "Income:Dividend:Schwab"
         interest_income_account: "Income:Interest:Schwab"
         capital_gains_account: "Income:Capital-Gains:Schwab"
         fees_account: "Expenses:Brokerage-Fees:Schwab"
         taxes_account: "Expenses:Taxes"

These are all optional and will fall back to `Expenses:FIXME` if not specified.

This importer will add the metadata keys `date`, `source_desc`, and `schwab_action` to the
imported transactions; these (along with the account and transaction amount) are used to
match and reconcile/clear already-imported transactions with transactions found in the
CSV.

Sub-accounts of the asset, dividend, and capital gains accounts will be created per
security as needed; e.g. `Assets:Investments:Schwab:Brokerage-1234:XYZ` would be created
to track the balance of `XYZ` shares owned, and `Income:Dividend:Schwab:XYZ` for dividends
earned from `XYZ`, etc.

Caveats
=======

* Because Schwab CSV downloads do not provide any unique transaction identifier, and it is
possible for two identical rows to exist in the CSV and be actual separate but identical
transactions, no de-duplication is performed on incoming CSV rows. Thus, it's required to
download non-overlapping CSV statements.

* Not all Schwab "actions" (transaction types) are supported. There's no reference for all
the possible actions, and Schwab could add new ones any time. If your CSV includes an
unsupported action, you'll get a `ValueError: 'Foo' is not a valid BrokerageAction` or
`ValueError: 'Foo' is not a valid BankingEntryType` for banking files. Please
file an issue (and ideally a pull request!) to add support for that action.

* If you have multiple transactions involving a commodity between two downloads of lot
details (particularly two different sales), the importer may not be able to infer the lots
involved in each sale. In this case it will fall back to empty cost-basis on the sale and
you may have to fill it in manually in order to avoid ambiguity errors from beancount.

* The lot details logic assumes that if you have lot details for any commodity at a given
point in time, you have downloaded lot details for all commodities. If lot details are
missing for a commodity, it will assume that's because you no longer hold that commodity.

"""
from __future__ import annotations

import csv
import datetime
import enum
import os.path
import re
from collections import Counter, OrderedDict
from dataclasses import dataclass
from decimal import Decimal
from io import StringIO
from typing import (
    AbstractSet,
    Callable,
    Dict,
    Iterable,
    Iterator,
    List,
    Mapping,
    Optional,
    Sequence,
    Set,
    Tuple,
    Type,
    TypeVar,
    Union,
    cast,
)

from typing_extensions import TypedDict

from beancount.core.amount import Amount
from beancount.core.data import (
    EMPTY_SET,
    Balance,
    Directive,
    Meta,
    Open,
    Posting,
    Price,
    Transaction,
)
from beancount.core.flags import FLAG_OKAY
from beancount.core.number import MISSING, D
from beancount.core.position import CostSpec
from beancount_import.journal_editor import JournalEditor
from beancount_import.matching import FIXME_ACCOUNT
from beancount_import.posting_date import POSTING_DATE_KEY
from beancount_import.source import ImportResult, InvalidSourceReference, SourceResults
from beancount_import.source.description_based_source import (
    SOURCE_DESC_KEYS,
    DescriptionBasedSource,
    get_account_mapping as orig_get_account_mapping,
)
from beancount_import.unbook import group_postings_by_meta, unbook_postings

CASH_CURRENCY="USD"

class BrokerageAction(enum.Enum):
    # Please keep these alphabetized:
    ADR_MGMT_FEE = "ADR Mgmt Fee"
    BANK_INTEREST = "Bank Interest"
    BOND_INTEREST = "Bond Interest"
    BUY = "Buy"
    BUY_TO_CLOSE = "Buy to Close"
    BUY_TO_OPEN = "Buy to Open"
    CASH_DIVIDEND = "Cash Dividend"
    CASH_IN_LIEU = "Cash In Lieu"
    EXPIRED = "Expired"
    FOREIGN_TAX_PAID = "Foreign Tax Paid"
    JOURNAL = "Journal"
    JOURNALED_SHARES = "Journaled Shares"
    LONG_TERM_CAP_GAIN = "Long Term Cap Gain"
    LONG_TERM_CAP_GAIN_REINVEST = "Long Term Cap Gain Reinvest"
    MARGIN_INTEREST = "Margin Interest"
    CREDIT_INTEREST = "Credit Interest"
    MISC_CASH_ENTRY = "Misc Cash Entry"
    MONEYLINK_DEPOSIT = "MoneyLink Deposit"
    MONEYLINK_TRANSFER = "MoneyLink Transfer"
    PRIOR_YEAR_CASH_DIVIDEND = "Pr Yr Cash Div"
    PRIOR_YEAR_DIV_REINVEST = "Pr Yr Div Reinvest"
    PRIOR_YEAR_SPECIAL_DIVIDEND = "Pr Yr Special Div"
    PROMOTIONAL_AWARD = "Promotional Award"
    QUAL_DIV_REINVEST = "Qual Div Reinvest"
    QUALIFIED_DIVIDEND = "Qualified Dividend"
    NON_QUALIFIED_DIVIDEND = "Non-Qualified Div"
    REINVEST_DIVIDEND = "Reinvest Dividend"
    REINVEST_SHARES = "Reinvest Shares"
    REVERSE_SPLIT = "Reverse Split"
    SECURITY_TRANSFER = "Security Transfer"
    SELL = "Sell"
    SELL_TO_CLOSE = "Sell to Close"
    SELL_TO_OPEN = "Sell to Open"
    SERVICE_FEE = "Service Fee"
    SHORT_TERM_CAP_GAIN = "Short Term Cap Gain"
    SHORT_TERM_CAP_GAIN_REINVEST = "Short Term Cap Gain Reinvest"
    SPECIAL_DIVIDEND = "Special Dividend"
    STOCK_MERGER = "Stock Merger"
    STOCK_PLAN_ACTIVITY = "Stock Plan Activity"
    STOCK_SPLIT = "Stock Split"
    WIRE_FUNDS = "Wire Funds"
    WIRE_FUNDS_RECEIVED = "Wire Funds Received"
    FUNDS_RECEIVED = "Funds Received"

class BankingEntryType(enum.Enum):
    # Please keep these alphabetized:
    ACH = "ACH"
    ATM = "ATM"
    ATMREBATE = "ATMREBATE"
    CHECK = "CHECK"
    DEPOSIT = "DEPOSIT"
    INTADJUST = "INTADJUST"
    TRANSFER = "TRANSFER"
    VISA = "VISA"
    WIRE = "WIRE"

@dataclass(frozen=True)
class MergerSpecification:
    symbol: str
    quantity: Optional[Decimal]
    description: str

@dataclass(frozen=True)
class RawEntry:
    account: str
    date: datetime.date
    description: str
    amount: Optional[Amount]
    filename: str
    line: int

    def get_meta_account(self, account_meta: Meta, key: str) -> str:
        return cast(str, account_meta.get(key, FIXME_ACCOUNT))

    def get_processed_entry(
        self, account: str, account_meta: Meta, lots: LotsDB
    ) -> Optional[TransactionEntry]:
        raise NotImplementedError("subclasses must implement get_processed_entry")


@dataclass(frozen=True)
class RawBankEntry(RawEntry):
    entry_type: BankingEntryType
    check_no: Optional[int]
    running_balance: Amount

    def get_processed_entry(
        self, account: str, account_meta: Meta, lots: LotsDB
    ) -> Optional[TransactionEntry]:
        interest_account = self.get_meta_account(account_meta,
                                            INTEREST_INCOME_ACCOUNT_KEY)
        fees_account = self.get_meta_account(account_meta, FEES_ACCOUNT_KEY)
        shared_attrs: SharedAttrsDict = SharedAttrsDict(
            account=account,
            date=self.date,
            action=self.entry_type,
            description=self.description,
            amount=self.amount,
            filename=self.filename,
            line=self.line,
        )
        if self.amount is None:
            return None
        if self.entry_type == BankingEntryType.INTADJUST:
            return BankInterest(
               interest_account=interest_account,
               **shared_attrs,
            )
        elif self.entry_type == BankingEntryType.ATMREBATE:
            return BankFee(fees_account=fees_account, **shared_attrs)
        return TransactionEntry(**shared_attrs)


@dataclass(frozen=True)
class RawBrokerageEntry(RawEntry):
    action: BrokerageAction
    symbol: str
    quantity: Optional[Decimal]
    price: Optional[Decimal]
    fees: Optional[Decimal]
    merger_spec: Optional[MergerSpecification]

    def get_processed_entry(
        self, account: str, account_meta: Meta, lots: LotsDB
    ) -> Optional[TransactionEntry]:
        capital_gains_account = self.get_meta_account(account_meta, CAPITAL_GAINS_ACCOUNT_KEY)
        fees_account = self.get_meta_account(account_meta, FEES_ACCOUNT_KEY)
        interest_account = self.get_meta_account(account_meta, INTEREST_INCOME_ACCOUNT_KEY)
        dividend_account = self.get_meta_account(account_meta, DIV_INCOME_ACCOUNT_KEY)
        taxes_account = self.get_meta_account(account_meta, TAXES_ACCOUNT_KEY)
        schwab_account = get_schwab_account_from_meta(account_meta)
        amount = self.amount
        if self.action == BrokerageAction.STOCK_PLAN_ACTIVITY:
            quantity = self.quantity
            assert quantity is not None, quantity
            symbol = self.symbol
            assert symbol, symbol
            amount = Amount(quantity, currency=symbol)
        if self.action == BrokerageAction.EXPIRED:
            # could expire/settle to non-zero value, otherwise turn the None to zero
            amount = Amount(Decimal(0), currency=CASH_CURRENCY) if self.amount is None else self.amount
        if amount is None and self.quantity is not None:
            amount = Amount(self.quantity, self.symbol)
        assert amount is not None, self
        shared_attrs: SharedAttrsDict = dict(
            account=account,
            date=self.date,
            action=self.action,
            description=self.description,
            amount=amount,
            filename=self.filename,
            line=self.line,
        )
        if self.action == BrokerageAction.STOCK_MERGER:
            assert self.quantity is not None
            assert self.merger_spec is not None
            assert self.merger_spec.quantity is not None
            return Merger(
                    fees_account=fees_account,
                    symbol=self.symbol,
                    quantity=self.quantity,
                    price=self.price,
                    fees=self.fees,
                    merger_spec=self.merger_spec,
                    **shared_attrs
            )
        if self.action == BrokerageAction.STOCK_PLAN_ACTIVITY:
            cost = lots.get_cost(schwab_account, self.symbol, self.date)
            return StockPlanActivity(symbol=self.symbol, cost=cost, **shared_attrs)
        if self.action in (
            BrokerageAction.CASH_DIVIDEND,
            BrokerageAction.CASH_IN_LIEU,
            BrokerageAction.PRIOR_YEAR_CASH_DIVIDEND,
            BrokerageAction.PRIOR_YEAR_DIV_REINVEST,
            BrokerageAction.PRIOR_YEAR_SPECIAL_DIVIDEND,
            BrokerageAction.SPECIAL_DIVIDEND,
            BrokerageAction.QUALIFIED_DIVIDEND,
            BrokerageAction.NON_QUALIFIED_DIVIDEND,
            BrokerageAction.QUAL_DIV_REINVEST,
            BrokerageAction.REINVEST_DIVIDEND,
            BrokerageAction.LONG_TERM_CAP_GAIN_REINVEST,
            BrokerageAction.SHORT_TERM_CAP_GAIN_REINVEST,
        ):
            return CashDividend(
                symbol=self.symbol,
                dividend_account=dividend_account,
                **shared_attrs,
            )
        if self.action == BrokerageAction.BANK_INTEREST:
            return BankInterest(
                interest_account=interest_account,
                **shared_attrs,
            )
        if self.action in (BrokerageAction.REVERSE_SPLIT, BrokerageAction.STOCK_SPLIT):
            assert self.quantity is not None
            lot_splits = lots.split(schwab_account, self.symbol, self.date, self.quantity)
            return StockSplit(
                lot_splits=lot_splits,
                **shared_attrs,
            )
        if self.action == BrokerageAction.PROMOTIONAL_AWARD:
            return PromotionalAward(
                interest_account=interest_account,
                **shared_attrs,
            )
        if self.action == BrokerageAction.BOND_INTEREST:
            return BondInterest(
                symbol=self.symbol,
                interest_account=interest_account,
                **shared_attrs,
            )
        if self.action in (BrokerageAction.MONEYLINK_TRANSFER,
                           BrokerageAction.MONEYLINK_DEPOSIT,
                           BrokerageAction.JOURNAL,
                           BrokerageAction.JOURNALED_SHARES,
                           BrokerageAction.SECURITY_TRANSFER,
                           BrokerageAction.WIRE_FUNDS,
                           BrokerageAction.WIRE_FUNDS_RECEIVED,
                           BrokerageAction.FUNDS_RECEIVED):
            return Transfer(**shared_attrs)
        if self.action in (BrokerageAction.SELL,
                            BrokerageAction.SELL_TO_OPEN,
                            BrokerageAction.SELL_TO_CLOSE
                           ):
            quantity = self.quantity
            assert quantity is not None
            price = self.price
            assert price is not None
            lot_info = lots.get_sale_lots(schwab_account, self.symbol, self.date, quantity)
            return Sell(
                capital_gains_account=capital_gains_account,
                fees_account=fees_account,
                symbol=self.symbol,
                price=price,
                quantity=quantity,
                fees=self.fees,
                lots=lot_info,
                **shared_attrs,
            )
        if self.action in (BrokerageAction.BUY,
                           BrokerageAction.BUY_TO_OPEN,
                           BrokerageAction.BUY_TO_CLOSE,
                           BrokerageAction.REINVEST_SHARES
                           ):
            quantity = self.quantity
            assert quantity is not None
            price = self.price
            assert price is not None
            return Buy(
                capital_gains_account=capital_gains_account,
                fees_account=fees_account,
                symbol=self.symbol,
                price=price,
                quantity=quantity,
                fees=self.fees,
                **shared_attrs,
            )
        if self.action in (BrokerageAction.SHORT_TERM_CAP_GAIN, BrokerageAction.LONG_TERM_CAP_GAIN):
            return FundGainsDistribution(symbol=self.symbol, capital_gains_account=capital_gains_account, **shared_attrs)

        if self.action in (BrokerageAction.ADR_MGMT_FEE,
                           BrokerageAction.SERVICE_FEE,
                           BrokerageAction.MISC_CASH_ENTRY):
            # MISC_CASH_ENTRY appears to only be used to refund fees.
            # If that changes, it will need to be re-categorized.
            return Fee(fees_account=fees_account, **shared_attrs)
        if self.action == BrokerageAction.FOREIGN_TAX_PAID:
            return TaxPaid(taxes_account=taxes_account, **shared_attrs)
        if self.action == BrokerageAction.MARGIN_INTEREST or self.action == BrokerageAction.CREDIT_INTEREST:
            return Interest(interest_account=interest_account, **shared_attrs)
        if self.action == BrokerageAction.EXPIRED:
            assert self.quantity is not None
            price = Decimal(0) if self.price is None else self.price
            lot_info = lots.get_sale_lots(schwab_account, self.symbol, self.date, self.quantity)
            if self.quantity > 0:
                # an expiring long option means it is sold at the end => the posting has a negative 'quantity'
                return Buy(
                        capital_gains_account=capital_gains_account,
                        fees_account=fees_account,
                        symbol=self.symbol,
                        quantity=self.quantity,
                        price=price,
                        fees=self.fees,
                        **shared_attrs
                        )
            else:
                return Sell(
                        capital_gains_account=capital_gains_account,
                        fees_account=fees_account,
                        symbol=self.symbol,
                        quantity=self.quantity,
                        price=price,
                        fees=self.fees,
                        lots=lot_info,
                        **shared_attrs
                        )
        assert False, self.action


@dataclass(frozen=True)
class RawLot:
    """A single cost-basis lot of a single holding from Lot Details CSV."""
    symbol: str
    account: str
    asof: datetime.date
    opened: datetime.date
    quantity: Decimal
    price: Decimal
    cost: Decimal


class SharedAttrsDict(TypedDict):
    account: str
    date: datetime.date
    action: Union[BrokerageAction, BankingEntryType]
    description: str
    amount: Amount
    filename: str
    line: int


class InfoDict(TypedDict):
    type: str
    filename: str
    line: int


@dataclass(frozen=True)
class DirectiveEntry:
    date: datetime.date
    filename: str
    line: int

    def get_directive(self) -> Directive:
        raise NotImplementedError("subclasses must implement get_directive")

    def get_import_result(self) -> ImportResult:
        return ImportResult(
            date=self.date, info=self.get_info(), entries=[self.get_directive()]
        )

    def get_info(self) -> InfoDict:
        return dict(
            type="text/csv",
            filename=self.filename,
            line=self.line,
        )

    def get_accounts(self) -> List[str]:
        """Get any accounts for which this importer is authoritative."""
        return []


@dataclass(frozen=True)
class TransactionEntry(DirectiveEntry):
    account: str
    date: datetime.date
    action: Union[BrokerageAction, BankingEntryType]
    description: str
    amount: Amount
    filename: str
    line: int

    def get_action(self) -> str:
        return self.action.value

    def get_directive(self) -> Transaction:
        return Transaction(
            meta=None,
            date=self.date,
            flag=FLAG_OKAY,
            payee=None,
            narration=f"{self.get_narration_prefix()} - {self.description}",
            tags=EMPTY_SET,
            links=EMPTY_SET,
            postings=self.get_postings(),
        )

    def get_postings(self) -> List[Posting]:
        return [
            Posting(
                account=self.get_primary_account(),
                units=self.amount,
                cost=self.get_cost(),
                price=None,
                flag=None,
                meta=self.get_meta(),
            ),
            Posting(
                account=self.get_other_account(),
                units=self.get_other_units(),
                cost=None,
                price=None,
                flag=None,
                meta={},
            ),
        ]

    def get_primary_account(self) -> str:
        sub = self.get_sub_account()
        return f"{self.account}:{sub}" if sub is not None else self.account

    def get_accounts(self) -> List[str]:
        return [self.get_primary_account()]

    def get_cost(self) -> Optional[CostSpec]:
        return None

    def get_sub_account(self) -> Optional[str]:
        return None

    def get_other_account(self) -> str:
        return FIXME_ACCOUNT

    def get_other_units(self) -> Union[Amount, Type[MISSING]]:
        return -self.amount

    def get_meta(self) -> Meta:
        return OrderedDict(
            source_desc=self.description,
            date=self.date,
            **{POSTING_META_ACTION_KEY: self.get_action()},
        )

    def get_narration_prefix(self) -> str:
        return self.action.value


@dataclass(frozen=True)
class BankFee(TransactionEntry):
    fees_account: str

    def get_other_account(self) -> str:
        return self.fees_account


@dataclass(frozen=True)
class Fee(BankFee):
    def get_sub_account(self) -> Optional[str]:
        return "Cash"


@dataclass(frozen=True)
class TaxPaid(TransactionEntry):
    taxes_account: str

    def get_sub_account(self) -> Optional[str]:
        return "Cash"

    def get_other_account(self) -> str:
        return self.taxes_account

    def get_narration_prefix(self) -> str:
        return "INVBANKTRAN"

@dataclass(frozen=True)
class Interest(TransactionEntry):
    interest_account: str

    def get_sub_account(self) -> Optional[str]:
        return "Cash"

    def get_other_account(self) -> str:
        return self.interest_account


@dataclass(frozen=True)
class StockPlanActivity(TransactionEntry):
    symbol: str
    cost: Optional[Decimal]

    def get_cost(self) -> Optional[CostSpec]:
        cost = self.cost
        if cost is None:
            cost = Decimal("1")
            currency = "FIXME"
        else:
            currency = CASH_CURRENCY
        return CostSpec(
            number_per=cost,
            number_total=None,
            currency=currency,
            date=None,
            label=None,
            merge=None,
        )

    def get_sub_account(self) -> Optional[str]:
        return self.symbol

    def get_other_units(self) -> Union[Amount, Type[MISSING]]:
        return MISSING

    def get_narration_prefix(self) -> str:
        return "TRANSFER"


@dataclass(frozen=True)
class CashDividend(TransactionEntry):
    symbol: str
    dividend_account: str

    def get_sub_account(self) -> Optional[str]:
        return "Cash"

    def get_other_account(self) -> str:
        return f"{self.dividend_account}:{self.symbol}"

    def get_narration_prefix(self) -> str:
        return "INCOME - DIV"

@dataclass(frozen=True)
class BondInterest(TransactionEntry):
    symbol: str
    interest_account: str

    def get_sub_account(self) -> Optional[str]:
        return "Cash"

    def get_other_account(self) -> str:
        return f"{self.interest_account}:{self.symbol}"

    def get_narration_prefix(self) -> str:
        return "BOND INTEREST"


@dataclass(frozen=True)
class BankInterest(TransactionEntry):
    interest_account: str

    def get_sub_account(self) -> Optional[str]:
        if self.action == BankingEntryType.INTADJUST:
            # Checking interest, cash is held in main account
            return None
        return "Cash"

    def get_other_account(self) -> str:
        return self.interest_account

    def get_narration_prefix(self) -> str:
        return "INTEREST"


@dataclass(frozen=True)
class PromotionalAward(TransactionEntry):
    interest_account: str

    def get_sub_account(self) -> Optional[str]:
        if self.action == BankingEntryType.INTADJUST:
            # Checking interest, cash is held in main account
            return None
        return "Cash"

    def get_other_account(self) -> str:
        return self.interest_account

    def get_narration_prefix(self) -> str:
        return "PROMOTIONAL AWARD"

@dataclass(frozen=True)
class FundGainsDistribution(TransactionEntry):
    """
    ETFs and Mutual Funds can have distributions of capital gains
    generated by internal activity.
    """
    symbol: str
    capital_gains_account: str

    def get_sub_account(self) -> Optional[str]:
        return "Cash"

    def get_other_account(self) -> str:
        return f"{self.capital_gains_account}:{self.symbol}"

    def get_narration_prefix(self) -> str:
        return "INCOME - CAP GAINS"

@dataclass(frozen=True)
class Transfer(TransactionEntry):
    def get_sub_account(self) -> Optional[str]:
        if self.amount.currency != CASH_CURRENCY:
            return self.amount.currency
        return "Cash"

    def get_narration_prefix(self) -> str:
        return "TRANSFER"


@dataclass(frozen=True)
class StockSplit(TransactionEntry):
    lot_splits: List[LotSplit]

    def get_sub_account(self) -> Optional[str]:
        return self.amount.currency

    def get_postings(self) -> List[Posting]:
        postings = []
        if not self.lot_splits:
            return super().get_postings()

        for split in self.lot_splits:
            postings.append(
                Posting(
                    account=self.get_primary_account(),
                    units=Amount(-split.prev_qty, self.amount.currency),
                    cost=CostSpec(
                        number_per=split.prev_cost,
                        number_total=None,
                        currency=CASH_CURRENCY,
                        date=split.date,
                        label=None,
                        merge=None,
                    ),
                    price=None,
                    flag=None,
                    meta=self.get_meta(),
                )
            )
            postings.append(
                Posting(
                    account=self.get_primary_account(),
                    units=Amount(split.new_qty, self.amount.currency),
                    cost=CostSpec(
                        number_per=split.new_cost,
                        number_total=None,
                        currency=CASH_CURRENCY,
                        date=split.date,
                        label=None,
                        merge=None,
                    ),
                    price=None,
                    flag=None,
                    meta=self.get_meta(),
                )
            )

        return postings

    def get_narration_prefix(self) -> str:
        return "STOCKSPLIT"


@dataclass(frozen=True)
class Sell(TransactionEntry):
    capital_gains_account: str
    fees_account: str
    symbol: str
    quantity: Decimal
    price: Decimal
    fees: Optional[Decimal]
    lots: Mapping[Decimal, Decimal]

    def get_sub_account(self) -> Optional[str]:
        return self.symbol

    def get_other_account(self) -> str:
        return f"{self.account}:Cash"

    def get_postings(self) -> List[Posting]:
        lots = list(self.lots.items())
        cost_currency = CASH_CURRENCY
        if not lots:
            lots = [(MISSING, self.quantity)]
            cost_currency = MISSING
        postings = [
            Posting(
                account=self.get_primary_account(),
                units=-Amount(lot_qty, currency=self.symbol),
                cost=CostSpec(
                    number_per=lot_cost,
                    number_total=None,
                    currency=cost_currency,
                    date=None,
                    label=None,
                    merge=None,
                ),
                price=Amount(self.price, currency=CASH_CURRENCY),
                flag=None,
                meta=self.get_meta(),
            )
            for lot_cost, lot_qty in lots
        ]
        postings.append(
            Posting(
                account=self.get_other_account(),
                units=self.amount,
                cost=None,
                price=None,
                flag=None,
                meta=self.get_meta(),
            )
        )
        if self.action != BrokerageAction.SELL_TO_OPEN:
            # too early to realize gains/losses when opening a short position
            postings.append(
                Posting(
                    account=self.get_cap_gains_account(),
                    units=MISSING,
                    cost=None,
                    price=None,
                    flag=None,
                    meta={},
                )
            )
        fees = self.fees
        if fees is not None:
            postings.append(
                Posting(
                    account=self.fees_account,
                    units=Amount(self.fees, currency=CASH_CURRENCY),
                    cost=None,
                    price=None,
                    flag=None,
                    meta={},
                )
            )
        return postings

    def get_narration_prefix(self) -> str:
        if self.action in (BrokerageAction.SELL_TO_OPEN, BrokerageAction.SELL_TO_CLOSE):
            return "SELLOPT"
        elif self.action == BrokerageAction.EXPIRED:
            return "SELLOPT - EXPIRED"
        else:
            return "SELLSTOCK"

    def get_cap_gains_account(self) -> str:
        return f"{self.capital_gains_account}:{self.symbol}"

    def get_accounts(self) -> List[str]:
        return [self.get_primary_account(), self.get_other_account()]

@dataclass(frozen=True)
class Buy(TransactionEntry):
    capital_gains_account: str
    fees_account: str
    symbol: str
    quantity: Decimal
    price: Decimal
    fees: Optional[Decimal]

    def get_sub_account(self) -> Optional[str]:
        return self.symbol

    def get_other_account(self) -> str:
        return f"{self.account}:Cash"

    def get_postings(self) -> List[Posting]:
        postings = [
            Posting(
                account=self.get_primary_account(),
                units=Amount(self.quantity, currency=self.symbol),
                cost=CostSpec(
                    number_per=self.price,
                    number_total=None,
                    currency=CASH_CURRENCY,
                    date=None,
                    label=None,
                    merge=None,
                ),
                price=None,
                flag=None,
                meta=self.get_meta(),
            ),
            Posting(
                account=self.get_other_account(),
                units=self.amount,
                cost=None,
                price=None,
                flag=None,
                meta=self.get_meta(),
            ),
        ]
        if self.action in (BrokerageAction.BUY_TO_CLOSE, BrokerageAction.EXPIRED):
            # need to record gains when closing a short position
            postings.append(
                Posting(
                    account=self.get_cap_gains_account(),
                    units=MISSING,
                    cost=None,
                    price=None,
                    flag=None,
                    meta={},
                )
            )
        fees = self.fees
        if fees is not None:
            postings.append(
                Posting(
                    account=self.fees_account,
                    units=Amount(self.fees, currency=CASH_CURRENCY),
                    cost=None,
                    price=None,
                    flag=None,
                    meta={},
                )
            )
        return postings

    def get_narration_prefix(self) -> str:
        if self.action in (BrokerageAction.BUY_TO_OPEN, BrokerageAction.BUY_TO_CLOSE):
            return "BUYOPT"
        elif self.action == BrokerageAction.EXPIRED:
            return "BUYOPT - EXPIRED"
        else:
            return "BUYSTOCK"

    def get_cap_gains_account(self) -> str:
        return f"{self.capital_gains_account}:{self.symbol}"

    def get_accounts(self) -> List[str]:
        return [self.get_primary_account(), self.get_other_account()]


@dataclass(frozen=True)
class Merger(TransactionEntry):
    fees_account: str
    symbol: str
    quantity: Decimal
    price: Optional[Decimal]
    merger_spec: MergerSpecification
    fees: Optional[Decimal]

    def get_sub_account(self) -> Optional[str]:
        return self.symbol

    def get_other_account(self) -> str:
        return f"{self.account}:{self.merger_spec.symbol}"

    def get_postings(self) -> List[Posting]:
        postings = [
            Posting(
                account=self.get_primary_account(),
                units=Amount(self.quantity, currency=self.symbol),
                cost=CostSpec(
                    number_per=self.price,
                    number_total=None,
                    currency=CASH_CURRENCY,
                    date=None,
                    # at the moment requires manually choosing the lot
                    label=None,
                    merge=None,
                ),
                price=None,
                flag=None,
                meta=self.get_meta(),
            ),
            Posting(
                account=self.get_other_account(),
                units=Amount(self.merger_spec.quantity, currency=self.merger_spec.symbol),
                cost=CostSpec(
                    number_per=None,
                    number_total=None,
                    currency=CASH_CURRENCY,
                    date=None,
                    label="merger",
                    merge=None
                ),
                price=None,
                flag=None,
                meta=self.get_meta(),
            ),
        ]
        fees = self.fees
        # Mergers should not have fees, but just in case
        if fees is not None:
            postings.append(
                Posting(
                    account=self.fees_account,
                    units=Amount(self.fees, currency=CASH_CURRENCY),
                    cost=None,
                    price=None,
                    flag=None,
                    meta={},
                )
            )
        return postings

    def get_narration_prefix(self) -> str:
        return f"STOCKMERGER - {self.merger_spec.description}"

    def get_accounts(self) -> List[str]:
        return [self.get_primary_account(), self.get_other_account()]

@dataclass(frozen=True)
class RawPosition:
    date: datetime.date
    account: str
    symbol: str
    quantity: Optional[Decimal]
    price: Optional[Amount]
    value: Amount
    filename: str
    line: int

    def get_balance(self, account: str) -> BalanceEntry:
        if self.symbol == "Cash":
            amount = self.value
        else:
            qty = self.quantity
            assert qty is not None
            amount = Amount(Decimal(str(qty)), currency=self.symbol)
        return BalanceEntry(
            date=self.date,
            account=f"{account}:{self.symbol}",
            amount=amount,
            filename=self.filename,
            line=self.line,
        )

    def get_price(self) -> Optional[PriceEntry]:
        price = self.price
        if self.symbol == "Cash" or price is None:
            return None
        return PriceEntry(
            date=self.date,
            symbol=self.symbol,
            price=price,
            filename=self.filename,
            line=self.line,
        )


@dataclass(frozen=True)
class PriceEntry(DirectiveEntry):
    symbol: str
    price: Amount

    def get_directive(self) -> Price:
        return Price(meta=None, date=self.date, currency=self.symbol, amount=self.price)


@dataclass(frozen=True)
class BalanceEntry(DirectiveEntry):
    account: str
    amount: Amount

    def get_directive(self) -> Balance:
        return Balance(
            meta=None,
            date=self.date,
            account=self.account,
            amount=self.amount,
            tolerance=None,
            diff_amount=None,
        )


class EntryProcessor:
    def __init__(self, journal: JournalEditor, lots: LotsDB) -> None:
        (
            self.account_to_schwab,
            self.schwab_to_account,
        ) = get_account_mapping(journal.accounts)
        self.journal = journal
        self.lots = lots
        self.missing_accounts: Set[str] = set()
        self.found_accounts: Set[str] = set()

    def process_entry(self, raw_entry: RawEntry) -> Optional[TransactionEntry]:
        account = self.schwab_to_account.get(raw_entry.account)
        if account is None:
            self.missing_accounts.add(raw_entry.account)
            return None
        account_meta = self.journal.accounts[account].meta
        return raw_entry.get_processed_entry(account, account_meta, self.lots)

    def process_entries(
        self, raw_entries: Iterable[RawEntry]
    ) -> Iterator[TransactionEntry]:
        for raw_entry in raw_entries:
            processed = self.process_entry(raw_entry)
            if processed is not None:
                self.found_accounts.update(processed.get_accounts())
                yield processed

    def process_positions(
        self, raw_positions: Iterable[RawPosition]
    ) -> Iterator[Tuple[BalanceEntry, Optional[PriceEntry]]]:
        for raw_position in raw_positions:
            account = self.schwab_to_account.get(raw_position.account)
            if account is None:
                self.missing_accounts.add(raw_position.account)
                return None
            balance = raw_position.get_balance(account)
            price = raw_position.get_price()
            yield (balance, price)


POSTING_META_ACTION_KEY = "schwab_action"
POSTING_META_ACCOUNT_KEY = "schwab_account"
INTEREST_INCOME_ACCOUNT_KEY = "interest_income_account"
DIV_INCOME_ACCOUNT_KEY = "div_income_account"
FEES_ACCOUNT_KEY = "fees_account"
CAPITAL_GAINS_ACCOUNT_KEY = "capital_gains_account"
TAXES_ACCOUNT_KEY = "taxes_account"
ACCOUNT_RE = r"(?P<account_type>[a-zA-Z\s]*\s+)?(?P<account>[^\s]+)"
TITLE_RE = re.compile(
    r'"?Transactions\s+for\s+(?:[a-zA-Z]*\s+)?account '
    + ACCOUNT_RE +
    r' as of (?P<when>.+)"?')
OPTION_RE = re.compile(r'\w{1,4} \d\d\/\d\d\/\d\d\d\d \d*\.\d* [PC]')
STRIP_FROM_SYMBOL_RE = re.compile(r'[^\d\w]')


def get_schwab_account_from_meta(account_meta: Mapping[str, str]) -> str:
    raw_acct = account_meta[POSTING_META_ACCOUNT_KEY]
    # Because Schwab CSVs sometimes list accounts like "Foobar XXXX-1234" and
    # sometimes like "XXXX-1234", we strip the initial part and only use the
    # XXXX-1234 account ID. But for backward-compatibility we still need to
    # support the longer names being used in the Beancount account meta.
    return raw_acct.split()[-1]


def get_account_mapping(accounts: Dict[str, Open]) -> Tuple[Dict[str, str], Dict[str, str]]:
    """Wrap the base `get_account_mapping` with the account name to ID truncation."""
    acct_to_id, id_to_acct = orig_get_account_mapping(accounts, POSTING_META_ACCOUNT_KEY)
    return {a: i.split()[-1] for a, i in acct_to_id.items()}, {i.split()[-1]: a for i, a in id_to_acct.items()}


class SchwabSourceSpecDict(TypedDict):
    transaction_csv_filenames: Sequence[str]
    position_csv_filenames: Sequence[str]
    lots_csv_filenames: Optional[Sequence[str]]


LogStatusCallable = Callable[[str], None]


# (account, action, date, amount, description)
PostingKey = Tuple[str, str, datetime.date, Optional[Amount], str]
# (account, date, amount)
BalanceKey = Tuple[str, datetime.date, Amount]
# (symbol, date, price)
PriceKey = Tuple[str, datetime.date, Amount]


def load(spec: SchwabSourceSpecDict, log_status: LogStatusCallable):
    spec.setdefault("lots_csv_filenames", [])
    return SchwabSource(**spec, log_status=log_status)


class SchwabSource(DescriptionBasedSource):
    name = "schwab"

    def __init__(
        self,
        transaction_csv_filenames: Sequence[str],
        position_csv_filenames: Sequence[str],
        lots_csv_filenames: Optional[Sequence[str]],
        log_status: LogStatusCallable,
    ) -> None:
        super().__init__(log_status)
        self.transaction_csv_filename = transaction_csv_filenames
        self.position_csv_filenames = position_csv_filenames
        self.lots_csv_filenames = lots_csv_filenames or []
        self.raw_entries: List[RawEntry] = []
        self.raw_positions: List[RawPosition] = []
        for csv_filename in transaction_csv_filenames:
            self.raw_entries.extend(_load_transactions(csv_filename))
        for csv_filename in position_csv_filenames:
            self.raw_positions.extend(_load_positions(csv_filename))
        self.lots = LotsDB()
        for csv_filename in self.lots_csv_filenames:
            self.lots.load(_load_lots_csv(csv_filename))

    def get_example_key_value_pairs(self, transaction: Transaction, posting: Posting) -> Dict[str, Union[str, Sequence[str]]]:
        base = super().get_example_key_value_pairs(transaction, posting)
        action = posting.meta.get(POSTING_META_ACTION_KEY)
        if action:
            base[POSTING_META_ACTION_KEY] = action
        return base

    def prepare(self, journal: JournalEditor, results: SourceResults) -> None:

        processor = EntryProcessor(journal, self.lots)
        account_set = set(processor.account_to_schwab.keys())
        base_accounts = tuple(f"{a}:" for a in account_set)
        account_set.update(a for a in journal.accounts if a.startswith(base_accounts))

        balance_entries: List[BalanceEntry] = []
        price_entries: List[PriceEntry] = []
        for balance_entry, price_entry in processor.process_positions(
            self.raw_positions
        ):
            balance_entries.append(balance_entry)
            if price_entry is not None:
                price_entries.append(price_entry)

        source_entries = list(processor.process_entries(self.raw_entries))

        account_set.update(processor.found_accounts)

        self._get_pending_and_invalid_entries(
            source_entries=source_entries,
            balance_entries=balance_entries,
            price_entries=price_entries,
            journal_entries=journal.all_entries,
            account_set=account_set,
            results=results,
        )

        for acct in processor.missing_accounts:
            results.add_warning(
                f"No Beancount account associated with Schwab account {acct}."
            )

    def _get_pending_and_invalid_entries(
        self,
        source_entries: Iterable[TransactionEntry],
        balance_entries: Iterable[BalanceEntry],
        price_entries: Iterable[PriceEntry],
        journal_entries: Iterable[Directive],
        account_set: AbstractSet[str],
        results: SourceResults,
    ) -> None:
        matched_postings: Dict[PostingKey, List[Tuple[Transaction, Posting]]] = {}

        for entry in journal_entries:
            if isinstance(entry, Transaction):
                for postings in group_postings_by_meta(entry.postings):
                    posting = unbook_postings(postings)
                    key = self._get_key_from_posting(entry, posting, account_set)
                    if key is None:
                        continue
                    matched_postings.setdefault(key, []).append((entry, posting))

        matched_postings_counter: Dict[PostingKey, int] = Counter()
        for key, entry_posting_pairs in matched_postings.items():
            matched_postings_counter[key] += len(entry_posting_pairs)

        for source_entry in source_entries:
            matched = 0
            keys = 0
            import_result = source_entry.get_import_result()
            for entry in import_result.entries:
                if not isinstance(entry, Transaction):
                    continue
                for posting in entry.postings:
                    key = self._get_key_from_posting(entry, posting, account_set)
                    if key is None:
                        continue
                    keys += 1
                    if matched_postings_counter[key] > 0:
                        matched_postings_counter[key] -= 1
                        matched += 1
            if not matched:
                results.add_pending_entry(import_result)
            elif matched != keys:
                results.add_error(
                    f"Schwab CSV entry at {source_entry.line} of {source_entry.filename} "
                    "generates multiple postings and only some matched existing postings."
                )

        for key, entry_posting_pairs in matched_postings.items():
            extra = matched_postings_counter[key]
            if extra:
                results.add_invalid_reference(
                    InvalidSourceReference(extra, entry_posting_pairs)
                )

        for balance_entry in balance_entries:
            import_result = balance_entry.get_import_result()
            for directive in import_result.entries:
                results.add_pending_entry(import_result)

        for price_entry in price_entries:
            import_result = price_entry.get_import_result()
            for directive in import_result.entries:
                results.add_pending_entry(import_result)

        results.add_accounts(account_set)

    def _get_key_from_posting(
        self,
        entry: Transaction,
        posting: Posting,
        account_set: AbstractSet[str],
    ) -> Optional[PostingKey]:
        if posting.meta is None:
            return None
        if not posting.account in account_set:
            return None
        source_desc = cast(str, posting.meta.get(SOURCE_DESC_KEYS[0], ""))
        if not source_desc:
            return None
        action = cast(str, posting.meta.get(POSTING_META_ACTION_KEY, ""))
        units = posting.units
        final_units = None
        if isinstance(units, Amount) or units is None:
            final_units = units
        else:
            assert False, units
        return (
            posting.account,
            action,
            cast(datetime.date, posting.meta[POSTING_DATE_KEY]),
            final_units,
            source_desc,
        )


@dataclass(frozen=True)
class LotSplit:
    date: datetime.date
    prev_cost: Decimal
    prev_qty: Decimal
    new_cost: Decimal
    new_qty: Decimal


class LotsDB:
    """In-memory database of historical lot information from Schwab lot details CSVs.

    Can answer these queries:
        - Cost basis of the shares of symbol X in acct Y acquired on date Z?
        - Cost bases of the N shares of symbol X sold from acct Y on date Z?
        - LotSplits for a split of symbol X in acct Y on date Z, adding N shares?

    """
    def __init__(self) -> None:
        self.holdings: Dict[Tuple[str, str], HoldingLotsDB] = {}
        self.asof_dates: Set[datetime.date] = set()

    def load(self, raw_lots: Iterable[RawLot]) -> None:
        for raw_lot in raw_lots:
            self.asof_dates.add(raw_lot.asof)
            db = self._get_db(raw_lot.account, raw_lot.symbol)
            db.add(raw_lot)
        self.zero_fill()

    def zero_fill(self):
        """Fill zero quantities for all holdings without a quantity on an asof date."""
        for db in self.holdings.values():
            db.zero_fill(self.asof_dates)

    def get_cost(
        self, account: str, symbol: str, date: datetime.date,
    ) -> Optional[Decimal]:
        """Get cost of lot of `symbol` opened most recently before `date`.

        Return None if it can't be determined given lot info in db.
        """
        db = self.holdings.get((account, symbol))
        return db.get_cost(date) if db else None

    def get_sale_lots(
        self, account: str, symbol: str, date: datetime.date, quantity_sold: Decimal,
    ) -> Mapping[Decimal, Decimal]:
        """Get costs of lots of `symbol` from which `quantity_sold` were sold on `date`.

        Key of returned mapping is lot cost, value is quantity sold from that lot.
        `quantity_sold` may have come from multiple lots in which case returned mapping
        will have multiple entries.

        If the total quantity we have recorded as sold from the holding in that time
        period doesn't match `quantity_sold`, return empty dict; we don't want to guess in
        the face of ambiguity.

        The assumption here is that we will download lot details at least once between
        every sale of a given holding, so things should match up exactly; if they don't,
        we revert to unknown cost.
        """
        db = self.holdings.get((account, symbol))
        return db.get_sale_lots(date, quantity_sold) if db else {}

    def split(
        self, account: str, symbol: str, date: datetime.date, quantity_added: Decimal,
    ) -> List[LotSplit]:
        """Get LotSplit for each lot of `symbol` in `account` as of `date`."""
        db = self.holdings.get((account, symbol))
        return db.split(date, quantity_added) if db else []

    def _get_db(self, account: str, symbol: str) -> HoldingLotsDB:
        key = (account, symbol)
        db = self.holdings.get(key)
        if db is None:
            db = self.holdings[key] = HoldingLotsDB(*key)
        return db


class HoldingLotsDB:
    """Historical information for lots of a single holding in a single account."""
    def __init__(self, account: str, symbol: str) -> None:
        self.account = account
        self.symbol = symbol
        # We uniquely identify a lot by (opened, cost). (Schwab will track separate
        # lots opened at the same moment with the same cost, but they don't give us any
        # unique key to distinguish these by, so the best we can do is collapse them, and
        # that's good enough for our query needs anyway, since all we ever want to get out
        # is a cost.) So this dict maps (opened, cost) to a per-lot dict mapping as-of
        # date to quantity of shares in the lot at that point in time.
        self.lots: Dict[Tuple[datetime.date, Decimal], Dict[datetime.date, Decimal]] = {}

    def add(self, raw_lot: RawLot) -> None:
        assert raw_lot.account == self.account, raw_lot.account
        assert raw_lot.symbol == self.symbol, raw_lot.symbol
        lot = self.lots.setdefault((raw_lot.opened, raw_lot.cost), {})
        lot[raw_lot.asof] = lot.get(raw_lot.asof, 0) + raw_lot.quantity

    def zero_fill(self, asof_dates: Set[datetime.date]) -> None:
        for dt in asof_dates:
            for lot in self.lots.values():
                lot.setdefault(dt, Decimal("0"))

    def get_cost(self, date: datetime.date) -> Optional[Decimal]:
        ret = None
        for opened, cost in sorted(self.lots.keys()):
            if opened > date:
                break
            ret = cost
        return ret

    def get_sale_lots(
        self, date: datetime.date, quantity_sold: Decimal
    ) -> Mapping[Decimal, Decimal]:
        ret: Dict[Decimal, Decimal] = {}
        for (opened, cost), quantities in self.lots.items():
            last_qty = None
            last_asof = None
            for asof, qty in sorted(quantities.items()):
                if last_asof and (last_asof < date < asof):
                    # the given date falls in this interval
                    sold = last_qty - qty
                    if sold > quantity_sold:
                        # Too many sold; return empty dict
                        return {}
                    if sold:
                        quantity_sold -= sold
                        ret[cost] = sold
                    break
                last_qty = qty
                last_asof = asof

        # We weren't able to account for all of `quantity_sold`; return empty dict
        if quantity_sold:
            return {}
        return ret

    def split(self, date: datetime.date, quantity_added: Decimal) -> List[LotSplit]:
        existing_quantity = Decimal("0")
        existing_lots: List[Tuple[datetime.date, Decimal, Decimal]] = []
        for (opened, cost), quantities in self.lots.items():
            current_lot: Optional[Tuple[datetime.date, Decimal, Decimal]] = None
            for asof, qty in sorted(quantities.items()):
                if asof > date:
                    break
                current_lot = (opened, cost, qty)
            if current_lot is not None and current_lot[2] > Decimal("0"):
                existing_quantity += current_lot[2]
                existing_lots.append(current_lot)
        ratio = (existing_quantity + quantity_added) / existing_quantity
        splits: List[LotSplit] = []
        for (date, cost, qty) in existing_lots:
            new_cost = cost / ratio
            new_qty = qty * ratio
            splits.append(
                LotSplit(
                    date=date,
                    prev_cost=cost,
                    prev_qty=qty,
                    new_cost=new_cost,
                    new_qty=new_qty,
                )
            )
        return splits


def _load_transactions(filename: str) -> List[RawEntry]:
    expected_brokerage_field_names = [
        "Date",
        "Action",
        "Symbol",
        "Description",
        "Quantity",
        "Price",
        "Fees & Comm",
        "Amount",
        "",
    ]
    expected_banking_field_names = [
        "Date",
        "Type",
        "Check #",
        "Description",
        "Withdrawal (-)",
        "Deposit (+)",
        "RunningBalance",
    ]
    filename = os.path.abspath(filename)
    entries = []
    with open(filename, "r", encoding="utf-8", newline="") as csvfile:
        title = csvfile.readline()
        match = TITLE_RE.match(title)
        assert match, title
        account = match.groupdict()["account"]
        reader = csv.DictReader(csvfile)
        if reader.fieldnames == expected_brokerage_field_names:
            entries = _load_brokerage_transactions(reader, account, filename)
        elif reader.fieldnames == expected_banking_field_names:
            entries = _load_banking_transactions(reader, account, filename)
        else:
            raise RuntimeError(f"Unexpected header {reader.fieldnames}")
    entries.reverse()
    return entries


def _load_banking_transactions(reader: csv.DictReader, account: str, filename):
    entries = []
    transaction_start_line = 1
    non_posting_patterns = [
        "Pending Transactions are not reflected within this sort criterion.",
        "Posted Transactions",
        "There were no transactions for the search criteria you selected."
    ]
    for lno, row in enumerate(reader):
        # First two rows are info messages.
        if row["Date"] in non_posting_patterns:
            transaction_start_line += 1
            continue
        date = _convert_date(row["Date"])
        entry_type = BankingEntryType(row["Type"])
        check_no = None
        if row["Check #"] not in (None, ""):
            check_no = int(row["Check #"])
        description = row["Description"]
        withdrawal_amount = _convert_decimal(row["Withdrawal (-)"])
        deposit_amount = _convert_decimal(row["Deposit (+)"])
        running_balance = _convert_decimal(row["RunningBalance"])
        amount_present = withdrawal_amount or deposit_amount
        amount = D(0)
        if withdrawal_amount:
            amount -= withdrawal_amount
        if deposit_amount:
            amount += deposit_amount
        entries.append(
            RawBankEntry(
                account=account,
                date=date,
                entry_type=entry_type,
                check_no=check_no,
                description=description,
                amount=Amount(amount, currency=CASH_CURRENCY) if amount_present else None,
                running_balance=running_balance,
                filename=filename,
                line=lno + transaction_start_line,
            )
        )
    return entries


@dataclass(frozen=True)
class ReverseSplitFirstLine:
    symbol: str
    quantity: Decimal


def _load_brokerage_transactions(reader: csv.DictReader, account: str,
                                 filename):
    entries = []
    found_total_line = False
    merger_spec = None
    reverse_split = None
    for lno, row in enumerate(reader):
        # Final row in CSV is not a real transaction
        if row["Date"] == "Transactions Total":
            found_total_line = True
            continue
        if row["Date"] == "":
            continue
        assert not found_total_line
        date = _convert_date(row["Date"])
        action = BrokerageAction(row["Action"])
        symbol = STRIP_FROM_SYMBOL_RE.sub("", row["Symbol"])
        description = row["Description"]
        quantity = _convert_decimal(row["Quantity"])
        price = _convert_decimal(row["Price"])
        fees = _convert_decimal(row["Fees & Comm"])
        amount = _convert_decimal(row["Amount"])
        if OPTION_RE.match(row["Symbol"]) and quantity:
            # this is an option, sold in lots of 100
            quantity *= 100
        if action == BrokerageAction.STOCK_MERGER and merger_spec is None:
            # special logic: next CSV line is the second posting related to the merger
            merger_spec = MergerSpecification(symbol, quantity, description)
            continue
        elif action == BrokerageAction.REVERSE_SPLIT and reverse_split is None:
            # reverse splits occupy two lines in CSV
            assert quantity is not None, quantity
            reverse_split = ReverseSplitFirstLine(symbol=symbol, quantity=quantity)
            continue
        if reverse_split is not None:
            assert action == BrokerageAction.REVERSE_SPLIT
            assert quantity is not None, quantity
            quantity += reverse_split.quantity
            symbol = reverse_split.symbol
            reverse_split = None
        entries.append(
            RawBrokerageEntry(
                account=account,
                date=date,
                action=action,
                symbol=symbol,
                description=description,
                quantity=quantity,
                price=price,
                fees=fees,
                amount=Amount(amount, currency=CASH_CURRENCY) if amount else None,
                merger_spec=merger_spec,
                filename=filename,
                line=lno + 2,
            )
        )
        merger_spec = None
    return entries


POSITIONS_TITLE_RE = re.compile(
    r'"?Positions for ' + ACCOUNT_RE + ' as of (?P<time>.+), '
    r'(?P<date>[\d\/]+)"?'
)
POSITIONS_ACCT_RE = re.compile(f'"{ACCOUNT_RE}"')


def _load_positions(filename: str) -> Sequence[RawPosition]:
    filename = os.path.abspath(filename)
    entries: List[RawPosition] = []
    lines: List[str] = []
    line_count = 1
    empty_lines = 0
    with open(filename, "r", encoding="utf-8", newline="") as csvfile:
        title = csvfile.readline()
        match = POSITIONS_TITLE_RE.match(title)
        assert match, title
        groups = match.groupdict()
        date = _convert_date(match.groupdict()["date"])
        if groups["account"] == "All-Accounts":
            expect_account_headers = True
            looking_for_account = True
            account = None
        else:
            expect_account_headers = False
            looking_for_account = False
            account = groups["account"]
            line_count += 1

        for line in csvfile:
            if not line.strip():
                looking_for_account = expect_account_headers
                empty_lines += 1
                continue
            if looking_for_account:
                match = POSITIONS_ACCT_RE.match(line)
                assert match, line
                if account is not None:
                    entries.extend(
                        _load_positions_csv(
                            lines,
                            date,
                            account,
                            line_count,
                            filename,
                        )
                    )
                line_count += len(lines) + empty_lines + 1
                lines = []
                empty_lines = 0
                account = match.groupdict()["account"]
                looking_for_account = False
                continue
            lines.append(line)

        if account is not None:
            entries.extend(
                _load_positions_csv(lines, date, account, line_count, filename)
            )
        return entries


def _load_positions_csv(
    lines: Iterable[str],
    date: datetime.date,
    account: str,
    start_line: int,
    filename: str,
) -> Sequence[RawPosition]:
    expected_field_names = [
        "Symbol",
        "Description",
        "Quantity",
        "Price",
        "Price Change $",
        "Price Change %",
        "Market Value",
        "Day Change $",
        "Day Change %",
        "Cost Basis",
        "Gain/Loss $",
        "Gain/Loss %",
        "Reinvest Dividends?",
        "Capital Gains?",
        "% Of Account",
        "Dividend Yield",
        "Last Dividend",
        "Ex-Dividend Date",
        "P/E Ratio",
        "52 Week Low",
        "52 Week High",
        "Volume",
        "Intrinsic Value",
        "In The Money",
        "Security Type",
        "",
    ]
    csvfile = StringIO("\n".join(lines))
    reader = csv.DictReader(csvfile)
    assert reader.fieldnames == expected_field_names, reader.fieldnames
    entries = []
    found_account_total = False
    for lno, row in enumerate(reader):
        line = start_line + lno + 2
        symbol = row["Symbol"]
        if symbol == "Account Total":
            found_account_total = True
            continue
        assert not found_account_total, row
        if symbol == "Cash & Cash Investments":
            symbol = "Cash"
        symbol = STRIP_FROM_SYMBOL_RE.sub("", symbol)
        quantity = _convert_decimal(row["Quantity"])
        price_d = _convert_decimal(row["Price"])
        price = None if price_d is None else Amount(price_d, currency=CASH_CURRENCY)
        value_d = _convert_decimal(row["Market Value"])
        assert value_d is not None, row["Market Value"]
        value = Amount(value_d, currency=CASH_CURRENCY)
        if OPTION_RE.match(row["Symbol"]) and quantity:
            # this is an option, sold in lots of 100
            quantity *= 100
        entries.append(
            RawPosition(
                date=date,
                account=account,
                symbol=symbol,
                quantity=quantity,
                price=price,
                value=value,
                filename=filename,
                line=line,
            )
        )
    return entries


LOT_DETAILS_TITLE_RE = re.compile(
    r'"(?P<symbol>[A-Z]+) Lot Details for (?P<account>.+) as of (?P<datetime>.+)"'
)


def _load_lots_csv(filename: str) -> Sequence[RawLot]:
    expected_field_names = [
        "Open Date",
        "Quantity",
        "Price",
        "Cost/Share",
        "Market Value",
        "Cost Basis",
        "Gain/Loss $",
        "Gain/Loss %",
        "Holding Period",
        "",
    ]
    expected_extended_field_names = [
        "Open Date",
        "Transaction Open",
        "Quantity",
        "Price",
        "Cost/Share",
        "Transaction CPS",
        "Market Value",
        "Cost Basis",
        "Transaction CB",
        "Gain/Loss $",
        "Transaction G/L $",
        "Gain/Loss %",
        "Transaction G/L %",
        "Holding Period",
        "Disallowed Loss",
        "",
    ]
    filename = os.path.abspath(filename)
    entries: List[RawLot] = []
    lines: List[str] = []
    with open(filename, "r", encoding="utf-8", newline="") as csvfile:
        title = csvfile.readline()
        match = LOT_DETAILS_TITLE_RE.match(title)
        assert match, title
        groups = match.groupdict()
        symbol = groups["symbol"]
        account = groups["account"]
        asof = _convert_title_datetime(match.groupdict()["datetime"]).date()
        empty = csvfile.readline()
        assert not empty.strip(), empty
        reader = csv.DictReader(csvfile)
        assert (
            reader.fieldnames == expected_field_names or
            reader.fieldnames == expected_extended_field_names), reader.fieldnames
        for row in reader:
            if row["Open Date"] == "Total":
                break
            entries.append(
                RawLot(
                    account=account,
                    symbol=symbol,
                    asof=asof,
                    opened=_convert_datetime(row["Open Date"]).date(),
                    quantity=none_throws(_convert_decimal(row["Quantity"])),
                    price=none_throws(_convert_decimal(row["Price"])),
                    cost=none_throws(_convert_decimal(row["Cost/Share"])),
                )
            )
    return entries


T = TypeVar("T")


def none_throws(x: Optional[T]) -> T:
    assert x is not None
    return x


def _convert_int(raw: str) -> Optional[int]:
    if raw in ("", "--"):
        return None
    return int(raw.replace(",", ""))


def _convert_decimal(raw: str) -> Optional[Decimal]:
    if raw in ("", "--"):
        return None
    return D(raw.replace("$", ""))


DATE_FORMAT = "%m/%d/%Y"
TITLE_DATETIME_FORMAT = f"%I:%M %p ET, {DATE_FORMAT}"
DATETIME_FORMAT = f"{DATE_FORMAT} %H:%M:%S"


def _convert_date(raw: str) -> datetime.date:
    raw = raw.split(" as of ")[-1]
    return datetime.datetime.strptime(raw, DATE_FORMAT).date()


def _convert_title_datetime(raw: str) -> datetime.datetime:
    return datetime.datetime.strptime(raw, TITLE_DATETIME_FORMAT)


def _convert_datetime(raw: str) -> datetime.datetime:
    return datetime.datetime.strptime(raw, DATETIME_FORMAT)
