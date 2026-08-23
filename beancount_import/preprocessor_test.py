import os
import py
import re

from . import reconcile_test
from .preprocessor import BasePreprocessor, TransactionRuleMixin, PreprocessingRule

testdata_root = os.path.realpath(
    os.path.join(os.path.dirname(__file__), "..", "testdata")
)

venmo_dir = os.path.realpath(
    os.path.join(os.path.dirname(__file__), "..", "testdata", "source", "venmo")
)

rules = [
    # Rule 1: Match tutoring transactions by description
    PreprocessingRule(
        source_name="venmo",
        transaction_narration="Tutoring",
        posting_meta={"venmo_description": "Tutoring"},
        target_account="Expenses:Tutoring",
        action="record",
    ),
    # Rule 2: Match rent payments by amount and description
    PreprocessingRule(
        transaction_narration="Rent",
        transaction_flag="*",
        posting_account=re.compile(r"Assets:Venmo"),
        posting_amount=re.compile(r"1[0-9]{3}\.[0-9]{2} USD"),  # Matches 1xxx.xx USD
        posting_meta={"venmo_description": "Rent"},
        target_account="Income:Rent",
        action="record",
    ),
    # Rule 3: Ignore small food-related transactions
    PreprocessingRule(
        transaction_narration=re.compile(r"^(?!Transfer$).*$"),
        posting_meta={
            "venmo_description": re.compile(r"(Bagels|Local Eats|Distant Treats)")
        },
        posting_amount=re.compile(
            r"-?\d{1,2}\.\d{2} USD"
        ),  # Matches small dollar amounts
        action="ignore",
    ),
    # Rule 4: Modify utility payments
    PreprocessingRule(
        transaction_narration=re.compile(r"^(?!Transfer$).*$"),
        posting_meta={"venmo_description": "Utilities", "venmo_type": "Charge"},
        target_account="Expenses:Utilities",
        action="modify",
    ),
    # Rule 5: Handle security deposits
    PreprocessingRule(
        transaction_narration=re.compile(r"^(?!Transfer$).*$"),
        posting_meta={"venmo_description": "Security deposit"},
        target_account="Liabilities:Security-Deposits",
        action="record",
    ),
    # Rule 6: Match standard transfers to checking
    PreprocessingRule(
        posting_meta={
            "venmo_type": "Standard Transfer",
            "venmo_account_description": re.compile(r".*\d{4}"),
        },
        target_account="Assets:Checking",
        action="record",
    ),
]


class RuleBasedPreprocessor(TransactionRuleMixin, BasePreprocessor):
    def preprocess(self) -> None:
        """Apply the preprocessing rules then call parent's preprocess."""
        self.apply_rules(rules)


def test_rule_preprocessing(tmpdir: py.path.local):
    tester = reconcile_test.ReconcileGoldenTester(
        golden_directory=os.path.join(
            testdata_root, "preprocessor", "test_preprocessor"
        ),
        temp_dir=str(tmpdir),
        options=dict(
            preprocessor=RuleBasedPreprocessor,
            data_sources=[
                {
                    "module": "beancount_import.source.venmo",
                    "directory": venmo_dir,
                    "assets_account": "Assets:Venmo",
                },
            ],
        ),
    )
    tester.snapshot()
