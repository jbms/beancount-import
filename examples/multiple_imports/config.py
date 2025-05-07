"""
This config is where you would initialize your importers with personal info
like account number or credit card last4 digit.

you may also define CONFIG:List[ImporterProtocol] for other beancount tools like
bean-identify, bean-file, and other beancount scripts to use
eg. `bean-identify _config.py ~/Downloads`
to identify the files that importers defined here can process

beancount-import should have it's own run.py where you invoke the
`beancount_import.webserver.main` but import the Importer objects from this config
"""
from beancount.ingest.importers.csv import Importer as CSVImporter, Col
from foo_bar_email_importer import FooBarTransactionEmailImporter

my_foobar_bank_importer = CSVImporter({
                        Col.DATE: 'Date',
                        Col.NARRATION1: 'Description',
                        Col.AMOUNT: 'Amount',
                        },
                       'Assets:FooBarBank', # account
                       'EUR', # currency
                        # regexps used by ImporterProtocol.identify() to identify the correct file
                       '"Date","Description","Amount"',
                       )

foobar_email_importer = FooBarTransactionEmailImporter(filing_account='Assets:FooBarBank')


my_amex_cc_importer = CSVImporter({
                        Col.DATE: 'Date',
                        Col.NARRATION1: 'Description',
                        Col.AMOUNT: 'Amount',
                        Col.BALANCE:'Balance'
                        },
                       'Liabilities:Amex-Credit-Card', # account
                       'EUR', # currency
                        # regexps used by ImporterProtocol.identify() to identify the correct file
                       ('Date,Description,Amount,Balance',
                       'Credit.*7890'
                       ),
                       skip_lines=1
                       )

# beancount's scripts use this
CONFIG = [my_foobar_bank_importer, foobar_email_importer, my_amex_cc_importer]
