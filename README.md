Beancount-import is a tool for semi-automatically importing financial data from
external data sources into the [Beancount](https://beancount.github.io/)
bookkeeping system, as well as merging and reconciling imported transactions with
each other and with existing transactions.

[![License: GPL v2](https://img.shields.io/badge/License-GPL%20v2-blue.svg)](LICENSE)
[![PyPI](https://img.shields.io/pypi/v/beancount-import)](https://pypi.org/project/beancount-import)
[![Build](https://github.com/jbms/beancount-import/workflows/Build/badge.svg)](https://github.com/jbms/beancount-import/actions?query=workflow%3ABuild)
[![Coverage Status](https://coveralls.io/repos/github/jbms/beancount-import/badge.svg?branch=master)](https://coveralls.io/github/jbms/beancount-import?branch=master)

# Key features

- Pluggable data source architecture, including existing support for OFX (cash,
  investment, and retirement accounts), Mint.com, Amazon.com, and Venmo.

- Supports [beancount importers](https://beancount.github.io/docs/importing_external_data.html) so it's easier to write your own,
  and existing beancount and fava users can hop right on with no hustle.

- Robustly associates imported transactions with the source data, to
  automatically avoid duplicates.

- Automatically predicts unknown legs of imported transactions based on a
  learned classifier (currently decision tree-based).

- Sophisticated transaction matching/merging system that can semi-automatically
  combine and reconcile both manually entered and imported transactions from
  independent sources.

- Easy-to-use, powerful web-based user interface.

# Basic operation

From the data source modules, beancount-import obtains a list of *pending*
imported transactions.  (Balance and price entries may also be provided.)
Depending on the external data source, pending transactions may fully specify
all of the Beancount accounts (e.g. an investment transaction from an OFX source
where shares of a stock are bought using cash in the same investment account),
or may have some postings to unknown accounts, indicated by the special account
name `Expenses:FIXME`.  For example, pending transactions obtained from bank
account/credit card account data (e.g. using the Mint.com data source) always
have exactly two postings, one to the known Beancount account corresponding to
the bank account from which the data was obtained, and the other to an unknown
account.

For each pending transaction, beancount-import attempts to find matches to both
existing transactions and to other pending transactions, and computes a set of
candidate merged transactions.  For each unknown account posting,
Beancount-import predicts the account based on a learned classifier.  Through a
web interface, the user can view the pending transactions, select the original
transaction or one of the merged candidates, and confirm or modify any predicted
accounts.  The web interface shows the lines in the journal that would be added
or removed for each candidate.  Once the user accepts a candidate, the candidate
is inserted or merged into the Beancount journal, and the user is then presented
with the next pending entry.

The imported transactions include metadata fields on the transaction and on the
postings that serve several purposes:
 - indicating to the data source module which entries in its external
   representation have already been imported and should not be imported again;
 - indicating which postings are *cleared*, meaning they have been confirmed by
   the authoritative source, which also constrains matching (a cleared posting
   can only match an uncleared posting);
 - providing necessary information for training the classifier used for
   predicting unknown accounts;
 - providing information to the user that may be helpful for identifying and
   understanding the transaction.

# Installation

1. Ensure you have activated a suitable Python 3 virtualenv if desired.

2. To install the most recent published package from PyPi, simply type:

   ```shell
   pip install beancount-import
   ```

   Alternatively, to install from a clone of the repository, type:
   ```shell
   pip install .
   ```

   or for development:
   ```shell
   pip install -e .
   ```

   The published PyPI package includes pre-built copy of the frontend and no
   further building is required.  When installing from the git repository, the
   frontend is built automatically by the above installation commands, but
   [Node.js](https://nodejs.org) is required.  If you don't already have it
   installed, follow the instructions in the [frontend](frontend/README.md)
   directory to install it.

# Demo

To see Beancount-import in action on test data, refer to the instructions in the
[examples](examples/) directory.

# Data sources

Data sources are defined by implementing the Source interface defined by the
[beancount_import.source](beancount_import/source/__init__.py) module.

The data sources provide a way to import and reconcile already-downloaded data.
To retrieve financial data automatically, you can use the
[finance_dl](https://github.com/jbms/finance-dl) package.  You can also use any
other mechanism, including manually downloading the data from a financial
institution's website, provided that it is in the format required by the data
source.

The currently supported set of data sources is:

- [beancount_import.source.ofx](beancount_import/source/ofx.py): most versatile
  data source, supports checking, savings, credit card, investment, and
  retirement accounts.
- [beancount_import.source.mint](beancount_import/source/mint.py): supports all
  cash account types supported by [Mint.com](https://www.mint.com).
- [beancount_import.source.venmo](beancount_import/source/venmo.py)
- [beancount_import.source.amazon](beancount_import/source/amazon.py): supports
  regular and digital order invoices.
- [beancount_import.source.healthequity](beancount_import/source/healthequity.py):
  supports [HealthEquity.com](https://healthequity.com) HSA accounts, including
  both cash and investment transactions.
- [beancount_import.source.google_purchases](beancount_import/source/google_purchases.py):
  imports purchases from that Google has [heuristically extracted from Gmail
  messages](https://myaccount.google.com/purchases).
- [beancount_import.source.paypal](beancount_import/source/paypal.py):
  imports Paypal transactions.
- [beancount_import.source.waveapps](beancount_import/source/waveapps.py):
  imports receipts from [Wave](https://waveapps.com), a free receipt-scanning
  website/mobile app.
- [beancount_import.source.stockplanconnect](beancount_import/source/stockplanconnect.py):
  imports release/trade transactions from
  [Morgan Stanley StockPlan Connect](https://stockplanconnect.com).
- [beancount_import.source.schwab_csv](beancount_import/source/schwab_csv.py):
  imports transactions and positions CSV exports from Schwab brokerage accounts.
- [beancount_import.source.ultipro_google](beancount_import/source/ultipro_google.py):
  imports Google employee Ultipro payroll statements.
- [beancount_import.source.generic_importer_source](beancount_import/source/generic_importer_source.py):
  imports from `beancount.ingest.importer.ImporterProtocol` subclass Importers. See [beancount's documentation](https://beancount.github.io/docs/importing_external_data.html#the-importing-process) on how to write one and checkout the [examples](examples/) directory for a simple csv importer

Refer to the individual data source documentation for details on configuration.

# Usage

To run Beancount-import, create a Python script that invokes the
`beancount_import.webserver.main` function.  Refer to the examples
[fresh](examples/fresh/run.py) and
[manually_entered](examples/manually_entered/run.py).

## Errors

Any errors either from Beancount itself or one of the data sources are shown in
the `Errors` tab.  It is usually wise to manually resolve any errors, either
using the built-in editor or an external editor, before proceeding, as some
errors may result in incorrect behavior.  Balance errors, however, are generally
safe to ignore.

## Viewing candidates

Select the `Candidates` tab to view the current pending imported entry, along
with all proposed matches with existing and other pending transactions.  The
original unmatched entry is always listed last, and the proposal that
includes the most matched postings is listed first.  The list with checkboxes at
the top indicates which existing or pending transactions are used in each
proposed match; the current pending transaction is always listed first.  If many
incorrect matches were found, you can deselect the checkboxes to filter the
matches.

You can select one of the proposed entries by clicking on it, or using the
up/down arrow keys.  To accept a proposed entry as is, you can press Enter or
double click it.  This immediately modifies the journal to reflect the change,
and also displays the relevant portion of the journal in the Journal tab, so
that you may easily make manual edits.

## Specifying unknown accounts

If a proposed entry includes unknown accounts, they are highlighted with a
distinctive background color and labeled with a group number.  The account shown
is the one that was automatically predicted, or `Expenses:FIXME` if automatic
prediction was not possible (e.g. because of lack of training data).  There are
several ways to correct any incorrectly-predicted accounts:
 - To change an individual account, you can Shift+click on it, type in the new
   account name, and then press Enter.  If you press Escape while typing in the
   account name, the account will be left unchanged.  A fuzzy matching algorithm
   is used for autocompletion: if you type "ex:co", for example, it will match
   any accounts for which there is a subsequence of 2 components, where the
   first starts (case-insensitively) with "ex" and the second starts with "co",
   such as an `Expenses:Drinks:Coffee` account.
 - To change all accounts within a proposed entry that share the same group
   number, you can click on one of the accounts without holding shift, or press
   the digit key corresponding to the group number.  Once you type in an account
   and press Enter, the specified account will be substituted for all postings
   in the group.
 - To change all accounts within a proposed entry, you can click the `Change
   account` button or press the `a` key.  Once you type in an account and press
   Enter, the specified account will be substituted for all unknown accounts in
   the current entry.
 - If you wish to postpone specifying the correct account, you can click the
   `Fixme later` button or press the `f` key.  This will substitute the original
   unknown account names for all unknown accounts in the current entry.  If you
   then accept this entry, the transaction including these FIXME accounts will
   be added to your journal, and the next time you start Beancount-import the
   transaction will be treated as a pending entry.

## Viewing associated source data

Data sources may indicate that additional source data is associated with
particular candidate entries, typically based on the metadata fields and/or
links that are included in the transaction.  For example, the
`beancount_import.source.amazon` data source associates the order invoice HTML
page with the transaction, and the `beancount_import.source.google_purchases`
data source associates the purchase details HTML page.  Other possible source
data types include PDF statements and receipt images.

You can view any associated source data for the currently selected candidate by
selecting the `Source data` tab.

## Changing the narration, payee, links or tags

To modify the narration of an entry, you can click on it, click the `Narration`
button, or press the `n` key.  This actually lets you modify the payee, links,
and tags as well.  If you introduce a syntax error in the first line of
transaction, the text box will be highlighted in red and focus will remain until
you either correct it or press Escape, which will revert the first line of the
transaction back to its previous value.

## Checking for uncleared postings

The `Uncleared` tab displays the list of postings to accounts for which there is
an authoritative source and which have not been cleared.  Normally, postings are
marked as cleared by adding the appropriate source-specific metadata fields that
associate it with the external data representation, such as an `ofx_fitid` field
in the case of the OFX source.

This list may be useful for finding discrepancies that need manual correction.
Typical causes of uncleared postings include:
1. The source data for the posting has not yet been downloaded.
2. The transaction is a duplicate of another transaction already in the journal,
   and needs to be manually merged/deleted.
3. The posting is from before the earliest date for which source data was
   imported, and no earlier data is available.  Such postings can be ignored by
   adding a `cleared_before: <date>` metadata field to the `open` directive for
   the account or one of its ancestor accounts.
4. The source data is missing or cannot be imported, but the posting was
   manually verified.  Such postings can be ignored by adding a `cleared: TRUE`
   metadata field to them.

## Skipping and ignoring imported entries

If you are presented with a pending entry that you don't wish to import, you
have several options:

1. You can skip past it by selecting a different transaction in the `Pending`
   tab, or can skip to the next pending entry by clicking on the button labeled
   `⏩` or pressing the `]` key.  This skips it in the current session, but it
   remains as a pending entry and will be included again if you restart
   beancount-import.

2. You can click on the button labeled `Fixme later` or press the `f` key to
   reset all unknown accounts, and then accept the candidate.  This will add the
   transaction to your journal, but with the unknown accounts left as
   `Expenses:FIXME`.  This is useful for transactions for which you don't know
   how to assign an account, or which you expect to match to another transaction
   that will be generated from data that hasn't yet been downloaded.  Any
   transactions in the journal with `Expenses:FIXME` accounts will be included
   at the end of the list of pending entries the next time you start
   beancount-import.

3. You can click on the button labeled `Ignore` or press the `i` key to add the
   selected candidate to the special "ignored" journal file.  This is useful for
   transactions that are erroneous, such as actual duplicates.  Entries that are
   ignored will not be presented again if you restart beancount-import.
   However, if you manually delete them from the "ignored" journal file, they
   will return as pending entries.

## Usage with a reverse proxy

If you want to run Beancount-import with features like TLS or authentication,
then you can run it behind a reverse proxy that provides this functionality.
For instance, an NGINX location configuration like the following can route
traffic to a local instance of Beancount-import:

```
location /some/url/prefix/ {
    proxy_pass_header Server;
    proxy_set_header Host $http_host;
    proxy_redirect off;
    proxy_set_header X-Real-IP $remote_addr;
    proxy_set_header X-Scheme $scheme;
    proxy_http_version 1.1;
    proxy_set_header Upgrade $http_upgrade;
    proxy_set_header Connection "Upgrade";
    proxy_pass http://localhost:8101/;
}
```

Replace `/some/url/prefix/` with your desired URL path (retaining the trailing
slash), or even just `/` to make Beancount-import available at the URL root.

# Use with an existing Beancount journal

If you start using Beancount-import with an existing beancount journal
containing transactions that are also referenced in the external data supplied
to a data sources, the data source will not know to skip those transactions,
because they will not have the requisite metadata indicating the association.
Therefore, they will all be presented to you as new pending imported
transactions.

However, the matching mechanism will very likely have determined the correct
match to an existing transaction, which will be presented as the default option.
Accepting these matches will simply have the effect of inserting the relevant
metadata into your journal so that the transactions are considered "cleared" and
won't be imported again next time you run Beancount-import.  It should be a
relatively quick process to do this even for a large number of transactions.

# Development

For development of this package, make sure to install Beancount-import using the
`pip install -e .` command rather than the `pip install .` command.  If you
previously ran the `pip install` command without the `-e` option, you can simply
re-run the `pip install -e .` command.

## Testing

You can run the tests using the `pytest` command.

Many of the tests are "golden" tests, which work by creating a textual
representation of some state and comparing it with the contents of a particular
file in the [testdata/](testdata/) directory.  If you change one of these tests
or add a new one, you can have the tests automatically generate the output by
setting the environment variable `BEANCOUNT_IMPORT_GENERATE_GOLDEN_TESTDATA=1`,
e.g.:

```shell
BEANCOUNT_IMPORT_GENERATE_GOLDEN_TESTDATA=1 pytest
```

Make sure to commit to at least stage any changes you've made to the relevant
`testdata` files prior to running the tests with this environment variable set.
That way you can manually verify any changes between the existing output and the
new output using `git diff`.

## Web frontend

The web frontend source code is in the [frontend/](frontend/) directory.  Refer
to the README.md file there for how to rebuild and run the frontend after making
changes.

# Basic workflow

## Simple expense transaction from Mint.com data source

Suppose the user has purchased a coffee at Starbucks on 2016-08-09 using a
credit card, and has set up Mint.com to retrieve the transaction data for this
credit card.

Given the following CSV entry:

```csv
"Date","Description","Original Description","Amount","Transaction Type","Category","Account Name","Labels","Notes"
"8/10/2016","Starbucks","STARBUCKS STORE 12345","2.45","debit","Coffee Shops","My Credit Card","",""
```

and the following open account directive:

```
1900-01-01 open Liabilities:Credit-Card  USD
  mint_id: "My Credit Card"
```

the Mint data source will generate the following pending transaction:

```
2016-08-10 * "STARBUCKS STORE 12345"
  Liabilities:Credit-Card             -2.45 USD
    date: 2016-08-10
    source_desc: "STARBUCKS STORE 12345"
  Expenses:FIXME                       2.45 USD
```

The user might manually specify that the unknown account is `Expenses:Coffee`.
The web interface will then show the updated changeset:

```
+2016-08-10 * "STARBUCKS STORE 12345"
+  Liabilities:Credit-Card             -2.45 USD
+    date: 2016-08-10
+    source_desc: "STARBUCKS STORE 12345"
+  Expenses:Coffee                      2.45 USD
```

If the `Expenses:Coffee` account does not already exist, Beancount-import will
additionally include an `open` directive in the changeset:

```
+2016-08-10 * "STARBUCKS STORE 12345"
+  Liabilities:Credit-Card             -2.45 USD
+    date: 2016-08-10
+    source_desc: "STARBUCKS STORE 12345"
+  Expenses:Coffee                      2.45 USD
+ 2016-08-10 open Expenses:Coffee USD
```

Once the user accepts this change, the changeset is applied to the journal.  The
presence of the `date` and `source_desc` metadata fields indicate to the Mint
data source that the `Liabilities:Credit-Card` posting is cleared.  The
combination of the words in the `source_desc`, the *source* account of
`Liabilities:Credit-Card`, and the *target* account of `Expenses:Coffee` serves
as a training example for the classifier.  A subsequent pending transaction with
a `source_desc` field containing the word `STARBUCKS` is likely to be
automatically classified as `Expenses:Coffee`.  Note that while in this case the
narration matches the `source_desc` field, the narration has no effect on the
automatic prediction.  The user must not delete or modify these metadata fields,
but additional metadata fields may be added.

Mint.com has its own heuristics for computing the `Description` and `Category`
fields from the `Original Description` provided by the financial institution.
However, these are ignored by the Mint data source as they are not stable (can
change if the data is re-downloaded) and not particularly reliable.

## Match to a manually entered transaction

Considering the same transaction as shown in the previous example, suppose the
user has already manually entered the transaction prior to running the import:

```
2016-08-09 * "Coffee"
  Liabilities:Credit-Card             -2.45 USD
  Expenses:Coffee
```

When running Beancount-import, the user will be presented with two candidates:

```
 2016-08-09 * "Coffee"
   Liabilities:Credit-Card             -2.45 USD
+    date: 2016-08-10
+    source_desc: "STARBUCKS STORE 12345"
   Expenses:Coffee


+2016-08-10 * "STARBUCKS STORE 12345"
+  Liabilities:Credit-Card             -2.45 USD
+    date: 2016-08-10
+    source_desc: "STARBUCKS STORE 12345"
+  Expenses:FIXME                       2.45 USD
```

The user should select the first one; selecting the second one would yield a
duplicate transaction (but which could later be diagnosed as an uncleared
transaction).  The `Expenses:FIXME` account in the second candidate would in
general actually be some other, possibly incorrect, predicted account, but which
is clearly indicated as an prediction that can be changed.

As is typically the case, the date on the manually entered transaction (likely
the date on which the transaction actually occurred) is not exactly the same as
the date provided by the bank.  To handle this discrepancy, Beancount-import
allows matches between postings that are up to 5 days apart.  The `date`
metadata field allows the posting to be reliably matched to the corresponding
entry in the CSV file, even though the overall transaction date differs.

Note that even though this transaction was manually entered, once it is matched
with the pending transaction and the `source_desc` and `date` metadata fields
are added, it functions as a training example exactly the same as in the
previous example.

## Credit card payment transaction

Suppose the user pays the balance of a credit card using a bank account, and
Mint.com is set up to retrieve the transactions from both the bank account and
the credit card.

Given the following CSV entries:

```csv
"Date","Description","Original Description","Amount","Transaction Type","Category","Account Name","Labels","Notes"
"11/27/2013","Transfer from My Checking","CR CARD PAYMENT ALEXANDRIA VA","66.88","credit","Credit Card Payment","My Credit Card","",""
"12/02/2013","National Federal Des","NATIONAL FEDERAL DES:TRNSFR","66.88","debit","Transfer","My Checking","",""
```

and the following open account directives:

```
1900-01-01 open Liabilities:Credit-Card  USD
  mint_id: "My Credit Card"

1900-01-01 open Assets:Checking  USD
  mint_id: "My Checking"
```

the Mint data source will generate 2 pending transactions, and for the first one
will present two candidates:

```
+2013-11-27 * "CR CARD PAYMENT ALEXANDRIA VA"
+  Liabilities:Credit-Card             66.88 USD
+    date: 2013-11-27
+    source_desc: "CR CARD PAYMENT ALEXANDRIA VA"
+  Assets:Checking                    -66.88 USD
+    date: 2013-12-02
+    source_desc: "NATIONAL FEDERAL DES:TRNSFR"


+2013-11-27 * "CR CARD PAYMENT ALEXANDRIA VA"
+  Liabilities:Credit-Card             66.88 USD
+    date: 2013-11-27
+    source_desc: "CR CARD PAYMENT ALEXANDRIA VA"
+  Expenses:FIXME                     -66.88 USD
```

Note that the `Expenses:FIXME` account in the second transaction will actually
be whichever account was predicted automatically.  If there have been prior
similar transactions, it is likely to be correct predicted as `Assets:Checking`.

The user should accept the first candidate to import both transactions at once.
In that case, both postings are considered cleared, and the new transaction will
result in two training examples for automatic prediction, corresponding to each
of the two combinations of `source_desc`, source account, and target account.

However, if the user accepts the second candidate (perhaps because the
transaction hasn't yet been posted to the checking account and the pending
transaction derived from the checking account data is not yet available), and
either leaves the account as `Expenses:FIXME`, manually specifies
`Assets:Checking`, or relies on the automatic prediction to choose
`Assets:Checking`, then when importing the transaction from the checking
account, the user will be presented with the following candidates and will have
another chance to accept the match:


```
 2013-11-27 * "CR CARD PAYMENT ALEXANDRIA VA"
   Liabilities:Credit-Card             66.88 USD
     date: 2013-11-27
     source_desc: "CR CARD PAYMENT ALEXANDRIA VA"
   Assets:Checking                    -66.88 USD
+    date: 2013-12-02
+    source_desc: "NATIONAL FEDERAL DES:TRNSFR"


+2013-12-02 * "NATIONAL FEDERAL DES:TRNSFR"
+  Assets:Checking                    -66.88 USD
+    date: 2013-12-02
+    source_desc: "NATIONAL FEDERAL DES:TRNSFR"
+  Expenses:FIXME                      66.88 USD
```

License
==

Copyright (C) 2014-2018 Jeremy Maitin-Shepard.

Distributed under the GNU General Public License, Version 2.0 only.
See [LICENSE](LICENSE) file for details.
