# Deterministic preprocessing

Rules handle routine imports before you review anything. This example uses
four invented Mint transactions and three rules from [`run.py`](run.py):

| Import | Rule | Result |
| --- | --- | --- |
| `EXAMPLE MARKET` | `record` to `Expenses:Groceries` | Written to `journal.beancount` with its `open` directive. Never shown in the UI. |
| `EXAMPLE CAFE` | `modify` to `Expenses:Dining` | Shown in the UI with the account filled in, for you to accept. |
| `CARD VERIFICATION HOLD` | `ignore` | Written to `ignored.beancount`. Never offered again. |
| `EXAMPLE HARDWARE` | none | Normal review, with the account left for you or the classifier. |

Copy this directory somewhere writable (the example edits its own journal
files), then run `python3 run.py` with Beancount-import installed and open the
printed URL. Restart it: nothing is recorded or ignored twice.

To use rules with your own setup, pass `preprocessor=Rules` to
`beancount_import.webserver.main`, as `run.py` does.

## Writing rules

```python
RULES = [
    PreprocessingRule(
        transaction_narration=re.compile('EXAMPLE MARKET'),
        target_account='Expenses:Groceries',      # action='record' by default
    ),
    PreprocessingRule(
        source_name='mint',
        transaction_narration=re.compile('CARD VERIFICATION'),
        posting_amount='-1.00 USD',
        action='ignore',
    ),
]

class Rules(TransactionRuleMixin, BasePreprocessor):
    def preprocess(self) -> None:
        self.apply_rules(RULES)
```

- **Which entries:** only pending entries holding one transaction with exactly
  two postings, one known account and one `Expenses:FIXME`, in either order.
  Anything else is left for review, or for a [custom callback](#custom-callbacks).
- **Order:** rules are tried in order, and the first match wins.
- **Criteria:** every criterion you set must match. A string matches exactly.
  A compiled regex uses `re.match`, so it is anchored at the start; write
  `re.compile('.*CAFE')` to match anywhere.
- **Actions:** `record` (the default), `modify` or `ignore`. Any other value
  raises an error. `target_account` replaces the `Expenses:FIXME` account for
  `record` and `modify`.

| Criterion | Matched against |
| --- | --- |
| `source_name` | The data source's name, e.g. `'mint'`, `'ofx'`, `'venmo'` |
| `posting_account` | The known account, e.g. `'Assets:Checking'` |
| `posting_amount` | The known posting's units as text, sign included: `'-1.00 USD'` |
| `transaction_narration`, `transaction_payee` | The transaction's narration or payee |
| `transaction_flag`, `posting_flag` | The flag, e.g. `'*'` or `'!'` |
| `posting_meta`, `transaction_meta` | A dict of key → pattern; every key must be present and match |
| `transaction_tags`, `transaction_links` | A pattern, or a list of patterns that each must match some tag or link |

To see which narration and metadata a source produces, look at a pending entry
in the web UI, or at an entry it has already written to your journal.

## Custom callbacks

For split transactions, or logic that looks at several imports together,
override `preprocess()` and use the `BasePreprocessor` helpers directly:

- `find_pending_entries(predicate)` iterates a snapshot, so the matching
  entries can be recorded or ignored during iteration.
- `record_pending_entry(old, new)` writes all replacement directives in one
  staged operation, opens missing accounts, and removes the pending entry only
  after the write succeeds. It indexes the written entries for later matching.
- `ignore_pending_entry(entry)` writes to the configured ignore journal and
  removes the pending entry from review and matching.
- `modify_pending_entry(old, new)` updates the pending entry and its display,
  identity and matching index without writing it. It keeps an explicit link to
  journal-backed originals, which are reserved for the replacement. Accepting,
  recording and ignoring update those originals directly, including after
  repeated modifications; they do not rely on fuzzy matching. A replacement
  with several directives is accepted as a whole.
- `get_fixme_postings(entry)` and `get_non_fixme_postings(entry)` split an
  entry's postings by whether the account is still unknown.

`pending_data` is the reconciler's current pending list, including after it has
been replaced, and assigning it updates that list. Editing the list directly
does not update the matching index or write files; use the helpers for that.
Writing to several files follows JournalEditor's existing behavior, so it is
not atomic across files.

### Lifecycle

The `preprocessor` option accepts any callable taking the `LoadedReconciler`;
`BasePreprocessor` subclasses qualify. It runs on initial load and on every
reload, after source matching and before classifier training. The journal
editor, pending entries, matching index, source and account mappings, and
uncleared postings are available; the classifier is not initialized yet.
Without the option, the import flow is unchanged.
