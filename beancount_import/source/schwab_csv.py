"""Schwab.com brokerage transaction source.

Imports transactions from Schwab.com brokerage transaction history CSV files.

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
         transaction_csv_filenames=glob.glob("data/schwab/transactions/*.CSV"),
         position_csv_filenames=glob.glob("data/schwab/positions/*.CSV"),
    )

This importer also makes use of certain metadata keys on your accounts. In order to label
a beancount account as a Schwab account whose authoritative transaction source is this
importer, specify the `schwab_account` metadata key as the account name exactly as it
appears in your Schwab CSV downloads, as well as specifying sub-accounts to be used for
recording dividends, capital gains, and fees. For example:

    2015-11-09 open Assets:Investments:Schwab:Brokerage-1234
         schwab_account: "Brokerage XXXX-1234"
         div_income_account: "Income:Dividend:Schwab"
         capital_gains_account: "Income:Capital-Gains:Schwab"
         fees_account: "Expenses:Brokerage-Fees:Schwab"

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
unsupported action, you'll get a `ValueError: 'Foo' is not a valid SchwabAction`. Please
file an issue (and ideally a pull request!) to add support for that action.

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
    Optional,
    Sequence,
    Set,
    Tuple,
    Type,
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
    get_account_mapping,
)
from beancount_import.unbook import group_postings_by_meta, unbook_postings


class SchwabAction(enum.Enum):
    CASH_DIVIDEND = "Cash Dividend"
    PRIOR_YEAR_CASH_DIVIDEND = "Pr Yr Cash Div"
    BUY = "Buy"
    SELL = "Sell"
    MONEYLINK_TRANSFER = "MoneyLink Transfer"
    BANK_INTEREST = "Bank Interest"
    JOURNAL = "Journal"
    STOCK_PLAN_ACTIVITY = "Stock Plan Activity"


@dataclass(frozen=True)
class RawEntry:
    account: str
    date: datetime.date
    action: SchwabAction
    symbol: str
    description: str
    quantity: Optional[int]
    price: Optional[Decimal]
    fees: Optional[Decimal]
    amount: Optional[Amount]
    filename: str
    line: int

    def get_processed_entry(
        self, account: str, account_meta: Meta
    ) -> Optional[TransactionEntry]:
        capital_gains_account = cast(str, account_meta[CAPITAL_GAINS_ACCOUNT_KEY])
        fees_account = cast(str, account_meta[FEES_ACCOUNT_KEY])
        dividend_account = cast(str, account_meta[DIV_INCOME_ACCOUNT_KEY])
        amount = self.amount
        if self.action == SchwabAction.STOCK_PLAN_ACTIVITY:
            quantity = self.quantity
            assert quantity is not None, quantity
            symbol = self.symbol
            assert symbol, symbol
            amount = Amount(Decimal(str(quantity)), currency=symbol)
        assert amount is not None
        shared_attrs: SharedAttrsDict = dict(
            account=account,
            date=self.date,
            action=self.action,
            description=self.description,
            amount=amount,
            filename=self.filename,
            line=self.line,
        )
        if self.action == SchwabAction.STOCK_PLAN_ACTIVITY:
            return StockPlanActivity(symbol=self.symbol, **shared_attrs)
        if self.action in (SchwabAction.CASH_DIVIDEND, SchwabAction.PRIOR_YEAR_CASH_DIVIDEND):
            return CashDividend(
                symbol=self.symbol,
                dividend_account=dividend_account,
                **shared_attrs,
            )
        if self.action == SchwabAction.BANK_INTEREST:
            return BankInterest(
                dividend_account=dividend_account,
                **shared_attrs,
            )
        if self.action in (SchwabAction.MONEYLINK_TRANSFER, SchwabAction.JOURNAL):
            return Transfer(**shared_attrs)
        if self.action == SchwabAction.SELL:
            quantity = self.quantity
            assert quantity is not None
            price = self.price
            assert price is not None
            return Sell(
                capital_gains_account=capital_gains_account,
                fees_account=fees_account,
                symbol=self.symbol,
                price=price,
                quantity=quantity,
                fees=self.fees,
                **shared_attrs,
            )
        if self.action == SchwabAction.BUY:
            quantity = self.quantity
            assert quantity is not None
            price = self.price
            assert price is not None
            return Buy(
                fees_account=fees_account,
                symbol=self.symbol,
                price=price,
                quantity=quantity,
                fees=self.fees,
                **shared_attrs,
            )
        assert False, self.action


class SharedAttrsDict(TypedDict):
    account: str
    date: datetime.date
    action: SchwabAction
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
    action: SchwabAction
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
        raise NotImplementedError()


@dataclass(frozen=True)
class StockPlanActivity(TransactionEntry):
    symbol: str

    def get_cost(self) -> Optional[CostSpec]:
        # TODO need a real cost here, not included in transactions CSV, probably need to
        # fill it in from parsing cost-basis lots CSV post-transaction.
        return CostSpec(
            number_per=Decimal("1"),
            number_total=None,
            currency="FIXME",
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
class BankInterest(TransactionEntry):
    dividend_account: str

    def get_sub_account(self) -> Optional[str]:
        return "Cash"

    def get_other_account(self) -> str:
        return self.dividend_account

    def get_narration_prefix(self) -> str:
        return "INVBANKTRAN"


@dataclass(frozen=True)
class Transfer(TransactionEntry):
    def get_sub_account(self) -> Optional[str]:
        return "Cash"

    def get_narration_prefix(self) -> str:
        return "INVBANKTRAN"


@dataclass(frozen=True)
class Sell(TransactionEntry):
    capital_gains_account: str
    fees_account: str
    symbol: str
    quantity: int
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
                units=-Amount(Decimal(str(self.quantity)), currency=self.symbol),
                # TODO handle cost basis by parsing cost-basis lots CSV, so we don't end
                # up getting beancount errors due to ambiguity
                cost=CostSpec(
                    number_per=MISSING,
                    number_total=None,
                    currency=MISSING,
                    date=None,
                    label=None,
                    merge=None,
                ),
                price=Amount(self.price, currency="USD"),
                flag=None,
                meta=self.get_meta(),
            ),
            Posting(
                account=self.get_cap_gains_account(),
                units=MISSING,
                cost=None,
                price=None,
                flag=None,
                meta={},
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
        fees = self.fees
        if fees is not None:
            postings.append(
                Posting(
                    account=self.fees_account,
                    units=Amount(self.fees, currency="USD"),
                    cost=None,
                    price=None,
                    flag=None,
                    meta={},
                )
            )
        return postings

    def get_narration_prefix(self) -> str:
        return "SELLSTOCK"

    def get_cap_gains_account(self) -> str:
        return f"{self.capital_gains_account}:{self.symbol}"

    def get_accounts(self) -> List[str]:
        return [self.get_primary_account(), self.get_other_account()]

@dataclass(frozen=True)
class Buy(TransactionEntry):
    fees_account: str
    symbol: str
    quantity: int
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
                units=Amount(Decimal(str(self.quantity)), currency=self.symbol),
                cost=CostSpec(
                    number_per=self.price,
                    number_total=None,
                    currency="USD",
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
        fees = self.fees
        if fees is not None:
            postings.append(
                Posting(
                    account=self.fees_account,
                    units=Amount(self.fees, currency="USD"),
                    cost=None,
                    price=None,
                    flag=None,
                    meta={},
                )
            )
        return postings

    def get_narration_prefix(self) -> str:
        return "BUYSTOCK"

    def get_accounts(self) -> List[str]:
        return [self.get_primary_account(), self.get_other_account()]


@dataclass(frozen=True)
class RawPosition:
    date: datetime.date
    account: str
    symbol: str
    quantity: Optional[int]
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
    def __init__(self, journal: JournalEditor) -> None:
        (
            self.account_to_schwab,
            self.schwab_to_account,
        ) = get_account_mapping(journal.accounts, POSTING_META_ACCOUNT_KEY)
        self.journal = journal
        self.missing_accounts: Set[str] = set()
        self.found_accounts: Set[str] = set()

    def process_entry(self, raw_entry: RawEntry) -> Optional[TransactionEntry]:
        account = self.schwab_to_account.get(raw_entry.account)
        if account is None:
            self.missing_accounts.add(raw_entry.account)
            return None
        account_meta = self.journal.accounts[account].meta
        return raw_entry.get_processed_entry(account, account_meta)

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
DIV_INCOME_ACCOUNT_KEY = "div_income_account"
FEES_ACCOUNT_KEY = "fees_account"
CAPITAL_GAINS_ACCOUNT_KEY = "capital_gains_account"
DATE_FORMAT = "%m/%d/%Y"
TITLE_RE = re.compile(r'"Transactions  for account (?P<account>.+) as of (?P<when>.+)"')


class SchwabSourceSpecDict(TypedDict):
    transaction_csv_filenames: Sequence[str]
    position_csv_filenames: Sequence[str]


LogStatusCallable = Callable[[str], None]


# (account, action, date, amount, description)
PostingKey = Tuple[str, str, datetime.date, Optional[Amount], str]
# (account, date, amount)
BalanceKey = Tuple[str, datetime.date, Amount]
# (symbol, date, price)
PriceKey = Tuple[str, datetime.date, Amount]


def load(spec: SchwabSourceSpecDict, log_status: LogStatusCallable):
    return SchwabSource(**spec, log_status=log_status)


class SchwabSource(DescriptionBasedSource):
    name = "schwab"

    def __init__(
        self,
        transaction_csv_filenames: Sequence[str],
        position_csv_filenames: Sequence[str],
        log_status: LogStatusCallable,
    ) -> None:
        super().__init__(log_status)
        self.transaction_csv_filename = transaction_csv_filenames
        self.position_csv_filenames = position_csv_filenames
        self.raw_entries: List[RawEntry] = []
        self.raw_positions: List[RawPosition] = []
        for csv_filename in transaction_csv_filenames:
            self.raw_entries.extend(_load_transactions(csv_filename))
        for csv_filename in position_csv_filenames:
            self.raw_positions.extend(_load_positions(csv_filename))

    def prepare(self, journal: JournalEditor, results: SourceResults) -> None:

        processor = EntryProcessor(journal)
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
        matched_balances: Set[BalanceKey] = set()
        matched_prices: Set[PriceKey] = set()

        for entry in journal_entries:
            if isinstance(entry, Balance):
                bkey = self._get_key_from_balance(entry)
                if bkey is not None:
                    matched_balances.add(bkey)
            elif isinstance(entry, Price):
                matched_prices.add(self._get_key_from_price(entry))
            elif isinstance(entry, Transaction):
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
                bkey = self._get_key_from_balance(directive)
                if bkey and bkey not in matched_balances:
                    results.add_pending_entry(import_result)

        for price_entry in price_entries:
            import_result = price_entry.get_import_result()
            for directive in import_result.entries:
                pkey = self._get_key_from_price(directive)
                if pkey not in matched_prices:
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
        units = posting.units
        final_units = None
        if isinstance(units, Amount) or units is None:
            final_units = units
        else:
            assert False, units
        return (
            posting.account,
            cast(str, posting.meta[POSTING_META_ACTION_KEY]),
            cast(datetime.date, posting.meta[POSTING_DATE_KEY]),
            final_units,
            source_desc,
        )

    def _get_key_from_balance(self, entry: Balance) -> Optional[BalanceKey]:
        return (
            entry.account,
            entry.date,
            entry.amount,
        )

    def _get_key_from_price(self, entry: Price) -> PriceKey:
        return (
            entry.currency,
            entry.date,
            entry.amount,
        )


def _load_transactions(filename: str) -> List[RawEntry]:
    expected_field_names = [
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
    filename = os.path.abspath(filename)
    entries = []
    with open(filename, "r", encoding="utf-8", newline="") as csvfile:
        title = csvfile.readline()
        match = TITLE_RE.match(title)
        assert match, title
        account = match.groupdict()["account"]
        reader = csv.DictReader(csvfile)
        assert reader.fieldnames == expected_field_names, reader.fieldnames
        found_total_line = False
        for lno, row in enumerate(reader):
            # Final row in CSV is not a real transaction
            if row["Date"] == "Transactions Total":
                found_total_line = True
                continue
            assert not found_total_line
            date = _convert_date(row["Date"])
            action = SchwabAction(row["Action"])
            symbol = row["Symbol"]
            description = row["Description"]
            quantity = int(row["Quantity"]) if row["Quantity"] else None
            price = _convert_decimal(row["Price"])
            fees = _convert_decimal(row["Fees & Comm"])
            amount = _convert_decimal(row["Amount"])
            entries.append(
                RawEntry(
                    account=account,
                    date=date,
                    action=action,
                    symbol=symbol,
                    description=description,
                    quantity=quantity,
                    price=price,
                    fees=fees,
                    amount=Amount(amount, currency="USD") if amount else None,
                    filename=filename,
                    line=lno + 2,
                )
            )
    entries.reverse()
    return entries


POSITIONS_TITLE_RE = re.compile(
    r'"Positions for All-Accounts as of (?P<time>.+), (?P<date>.+)"'
)
POSITIONS_ACCT_RE = re.compile(r'"(?P<account>.+)"')


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
        date = _convert_date(match.groupdict()["date"])

        account = None
        looking_for_account = True
        for line in csvfile:
            if not line.strip():
                looking_for_account = True
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
        assert 3 <= len(symbol) <= 4, symbol
        quantity = _convert_int(row["Quantity"])
        price_d = _convert_decimal(row["Price"])
        price = None if price_d is None else Amount(price_d, currency="USD")
        value_d = _convert_decimal(row["Market Value"])
        assert value_d is not None, row["Market Value"]
        value = Amount(value_d, currency="USD")
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


def _convert_int(raw: str) -> Optional[int]:
    if raw in ("", "--"):
        return None
    return int(raw.replace(",", ""))


def _convert_decimal(raw: str) -> Optional[Decimal]:
    if raw in ("", "--"):
        return None
    return D(raw.replace("$", ""))


def _convert_date(raw: str) -> datetime.date:
    raw = raw.split(" as of ")[-1]
    return datetime.datetime.strptime(raw, DATE_FORMAT).date()
