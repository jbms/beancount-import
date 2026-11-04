# Deterministic preprocessing

This example uses invented transactions. Copy this directory to a working
location and run `python3 run.py` with Beancount-import installed. Startup
records the Example Market payment to `Expenses:Groceries`, creating its Open
directive. The unmatched cafe payment remains for review in the web UI.
Restarting the example does not import the recorded payment again.

Add `preprocessor=Rules` to an existing webserver configuration to use the
pattern in `run.py` with your own sources.

## Callback lifecycle

The option accepts a callable receiving the `LoadedReconciler`. It runs on
initial load and reload, after source matching and before classifier training.
The journal editor, pending entries, matching index, source/account mappings,
and uncleared postings are available; the classifier is not initialized yet.
Omitting the option leaves the normal import flow unchanged.

`BasePreprocessor` calls your `preprocess()` method and provides these helpers:

- `record_pending_entry(old, new)` writes all replacement directives in one
  staged operation, opens missing accounts, and removes the pending entry only
  after the write succeeds. It indexes the persisted entries for later matching.
- `ignore_pending_entry(entry)` writes to the configured ignore journal and
  removes the pending entry from review and matching.
- `modify_pending_entry(old, new)` updates the pending entry and its display,
  identity, and matching index without writing it. It retains an explicit link
  to journal-backed originals, which are reserved for the replacement. Acceptance,
  recording, and ignoring update those originals directly, including after
  repeated modifications; they do not rely on fuzzy matching. A replacement with
  several directives is accepted as a whole.
- `find_pending_entries(predicate)` iterates a snapshot, allowing the matching
  entries to be recorded or ignored during iteration.

`pending_data` accesses the reconciler's current list, including when it has
been replaced. Assigning it also updates that list. Direct list edits do not
update the matching index or write files; use the helpers for those operations.
Writing to multiple files follows JournalEditor's existing behavior; it is not
an atomic transaction across files.

## Simple rules

Combine `TransactionRuleMixin` with `BasePreprocessor` as shown in `run.py`.
Rules are tried in order; the first matching rule wins. They handle entries
containing one transaction with exactly two postings: one known account and
one `Expenses:FIXME` account, in either order. Other entries remain for review
or can be handled in a custom callback.

String criteria match literally, and compiled regular expressions use `match`
(from the start of the string). All specified criteria must match. Supported
criteria cover the source name, account, amount, transaction narration/payee,
flags, posting/transaction metadata, tags, and links. Metadata criteria match
each specified key. Lists of tag/link patterns require every pattern to match
at least one value.

The actions are `record` (the default), `modify`, and `ignore`. `target_account`
replaces the unknown posting's account for `record` and `modify`. Invalid action
names raise an error rather than accidentally recording an entry.

Use a custom callback for split transactions or logic involving several imports.
