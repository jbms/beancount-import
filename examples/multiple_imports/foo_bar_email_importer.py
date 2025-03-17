"""
Imports a single transaction from transaction email received.
The same transaction would also exist in monthly csv statement.
so this importer does not clear the transaction,
by setting `self.account=None`
"""

import re
from beancount.ingest import importer
from beancount.core import data, flags
from pathlib import Path
from dateutil.parser import parse as date_parse


class FooBarTransactionEmailImporter(importer.ImporterProtocol):
    def __init__(self, filing_account='Assets:FooBarBank'):
        self._filing_account = filing_account
        self.account = None

    def identify(self, f):
        return (
            f.name.endswith(".html")
            and re.search(r"FooBar Bank Transaction Alert", Path(f.name).read_text())
            is not None
        )

    def extract(self, f, existing_entries=None):
        pattern = r"<tr>\s*<th>Date</th>\s*<th>Description</th>\s*<th>Amount</th>\s*</tr>\s*<tr>\s*<td>(?P<DATE>.*)</td>\s*<td>(?P<DESCRIPTION>.*)</td>\s*<td>(?P<AMOUNT>.*)</td>\s*</tr>"
        match = re.search(pattern, Path(f.name).read_text())
        if not match:
            return []
        groups = match.groupdict()
        txn = data.Transaction(
            meta=data.new_metadata(f.name, 0),
            date=date_parse(groups["DATE"]).date(),
            flag=flags.FLAG_OKAY,
            payee=None,
            narration=groups["DESCRIPTION"],
            tags=set(),
            links=set(),
            postings=[
                data.Posting(
                    account=self._filing_account,
                    units= data.Amount(data.D(groups["AMOUNT"]), "EUR"),
                    cost=None,
                    price=None,
                    flag=None,
                    meta={},
                )
            ],
        )
        # returns the single transaction imported from the transaction email
        return [txn]
