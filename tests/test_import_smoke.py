"""Smoke tests to verify all modules can be imported.

These tests catch import-time failures (missing APIs, renamed modules)
such as those introduced by the beancount v2 -> v3 migration, before they
surface as runtime failures.
"""
import pytest

MODULES = [
    "beancount_import",
    "beancount_import.amount_parsing",
    "beancount_import.delete_transactions",
    "beancount_import.journal_editor",
    "beancount_import.list_balance_at_date",
    "beancount_import.matching",
    "beancount_import.posting_date",
    "beancount_import.reconcile",
    "beancount_import.remove_transfer_account",
    "beancount_import.rename_account",
    "beancount_import.sorted_entry_printer",
    "beancount_import.sorted_list",
    "beancount_import.test_util",
    "beancount_import.thread_helpers",
    "beancount_import.training",
    "beancount_import.unbook",
    "beancount_import.webserver",
    "beancount_import.source",
    "beancount_import.source.amazon",
    "beancount_import.source.amazon_invoice",
    "beancount_import.source.amazon_invoice_sanitize",
    "beancount_import.source.description_based_source",
    "beancount_import.source.generic_importer_source",
    "beancount_import.source.google_purchases",
    "beancount_import.source.google_purchases_sanitize",
    "beancount_import.source.healthequity",
    "beancount_import.source.link_based_source",
    "beancount_import.source.mint",
    "beancount_import.source.ofx",
    "beancount_import.source.ofx_sanitize",
    "beancount_import.source.paypal",
    "beancount_import.source.paypal_sanitize",
    "beancount_import.source.schwab_csv",
    "beancount_import.source.stockplanconnect",
    "beancount_import.source.stockplanconnect_statement",
    "beancount_import.source.ultipro_google",
    "beancount_import.source.ultipro_google_statement",
    "beancount_import.source.venmo",
    "beancount_import.source.venmo_sanitize",
    "beancount_import.source.waveapps",
]


@pytest.mark.parametrize("module", MODULES)
def test_module_imports(module: str) -> None:
    """Verify that the module can be imported without errors."""
    __import__(module)
