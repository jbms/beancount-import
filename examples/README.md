To run these example, first ensure you have installed Beancount-import into your
current Python 3 environment (a virtualenv is recommended).

Then change your current directory to one of the following example directories,
and type `./run.py` or `python3 ./run.py` and open the URL that is printed in a
web browser.

Examples:

  - `fresh`: Example of importing transactions starting with an empty journal.
  - `manually_entered`: Example of importing transactions corresponding to
    existing, manually-entered transactions.
  - `multiple_imports`: Example of importing same transactions from multiple
    importers, eg. you receive transaction emails same day while the monthly
    statement is received at the end of the month. here, the transaction is
    imported from email but not cleared (by setting `account=None` in run.py)
    and is cleared only at the end of the month by monthly statement.
