import datetime
import glob
import os
from decimal import Decimal as D

import pytest

from .schwab_csv import LotsDB, RawLot
from .source_test import check_source_example

testdata_dir = os.path.realpath(
    os.path.join(
        os.path.dirname(__file__), '..', '..', 'testdata', 'source', 'schwab_csv'))

examples = [
    'test_basic',
    'test_lots',
]


@pytest.mark.parametrize('name', examples)
def test_source(name: str):
    example_dir = os.path.join(testdata_dir, name)
    source_spec = {
        'module': 'beancount_import.source.schwab_csv',
        "transaction_csv_filenames": sorted(glob.glob(f"{example_dir}/transactions/*.CSV")),
        "position_csv_filenames": sorted(glob.glob(f"{example_dir}/positions/*.CSV")),
    }
    if name == "test_lots":
        source_spec["lots_csv_filenames"] = sorted(glob.glob(f"{example_dir}/positions/lots/*/*.csv"))
    check_source_example(
        example_dir=example_dir,
        source_spec=source_spec,
        replacements=[(testdata_dir, '<testdata>')],
    )


@pytest.fixture
def db() -> LotsDB:
    return LotsDB()


def lot(
    symbol: str = "XX",
    account: str = "XX-12",
    asof: int = 1,
    opened: int = 1,
    quantity: int = 1,
    price: str = "1.0",
    cost: str = "1.0",
) -> RawLot:
    return RawLot(
        symbol=symbol,
        account=account,
        asof=dt(asof),
        opened=dt(opened),
        quantity=quantity,
        price=D(price),
        cost=D(cost),
    )


def d(day: int) -> datetime.date:
    return datetime.date(2021, 1, day)


def dt(day: int) -> datetime.datetime:
    return datetime.datetime(2021, 1, day)


class TestLotsDB:
    def test_cost(self, db) -> None:
        db.load([lot(opened=1, cost="1.1"), lot(opened=2, cost="1.2")])
        assert db.get_cost("Brokerage XX-12", "XX", d(2)) == D("1.2")

    def test_cost_date_skew(self, db) -> None:
        db.load([lot(opened=1, cost="1.1"), lot(opened=2, cost="1.2")])
        assert db.get_cost("Brokerage XX-12", "XX", d(3)) == D("1.2")

    def test_cost_no_record(self, db) -> None:
        assert db.get_cost("Brokerage XX-12", "XX", d(1)) is None

    def test_cost_no_match(self, db) -> None:
        db.load([lot(opened=2, cost="1.1"), lot(opened=3, cost="1.2")])
        assert db.get_cost("Brokerage XX-12", "XX", d(1)) is None

    def test_cost_wrong_symbol(self, db) -> None:
        db.load([lot(opened=1, cost="1.1"), lot(opened=2, cost="1.2")])
        assert db.get_cost("Brokerage XX-12", "YY", d(2)) is None

    def test_sale_lots(self, db) -> None:
        db.load([
            lot(asof=1, cost="1.1", quantity=10),
            lot(asof=3, cost="1.1", quantity=7),
        ])
        assert db.get_sale_lots("Brokerage XX-12", "XX", d(2), 3) == {D("1.1"): 3}

    def test_sale_lots_no_record(self, db) -> None:
        assert db.get_sale_lots("Brokerage XX-12", "XX", d(2), 3) == {}

    def test_sale_lots_wrong_symbol(self, db) -> None:
        db.load([
            lot(asof=1, cost="1.1", quantity=10),
            lot(asof=3, cost="1.1", quantity=7),
        ])
        assert db.get_sale_lots("Brokerage XX-12", "YY", d(2), 3) == {}

    def test_sale_lots_too_early(self, db) -> None:
        db.load([
            lot(asof=2, cost="1.1", quantity=10),
            lot(asof=3, cost="1.1", quantity=7),
        ])
        assert db.get_sale_lots("Brokerage XX-12", "XX", d(1), 3) == {}

    def test_sale_lots_too_late(self, db) -> None:
        db.load([
            lot(asof=1, cost="1.1", quantity=10),
            lot(asof=3, cost="1.1", quantity=7),
        ])
        assert db.get_sale_lots("Brokerage XX-12", "XX", d(4), 3) == {}

    def test_sale_lots_insufficient_sold(self, db) -> None:
        db.load([
            lot(asof=1, cost="1.1", quantity=10),
            lot(asof=3, cost="1.1", quantity=7),
        ])
        assert db.get_sale_lots("Brokerage XX-12", "XX", d(2), 4) == {}

    def test_sale_lots_additional_sold(self, db) -> None:
        db.load([
            lot(asof=1, cost="1.1", quantity=10),
            lot(asof=3, cost="1.1", quantity=7),
        ])
        assert db.get_sale_lots("Brokerage XX-12", "XX", d(2), 1) == {}

    def test_sale_lots_multiple_candidates(self, db) -> None:
        db.load([
            lot(opened=1, asof=3, cost="1.1", quantity=10),
            lot(opened=1, asof=5, cost="1.1", quantity=5),
            lot(opened=2, asof=3, cost="1.2", quantity=10),
            lot(opened=2, asof=5, cost="1.2", quantity=7),
        ])
        assert db.get_sale_lots("Brokerage XX-12", "XX", d(4), 3) == {}

    def test_sale_lots_split(self, db) -> None:
        db.load([
            lot(opened=1, asof=3, cost="1.1", quantity=10),
            lot(opened=1, asof=5, cost="1.1", quantity=5),
            lot(opened=2, asof=3, cost="1.2", quantity=10),
            lot(opened=2, asof=5, cost="1.2", quantity=7),
        ])
        assert db.get_sale_lots("Brokerage XX-12", "XX", d(4), 8) == {
            D("1.1"): 5,
            D("1.2"): 3,
        }

    def test_sale_lots_zero_fill(self, db) -> None:
        db.load([
            lot(symbol="XX", asof=1, cost="1.1", quantity=5),
            lot(symbol="YY", asof=3, cost="1.2", quantity=2),
        ])
        assert db.get_sale_lots("Brokerage XX-12", "XX", d(2), 5) == {D("1.1"): 5}
