import collections
import datetime
import re
from typing import List, Optional, Union, Callable, Dict, Mapping, Tuple, Any, Iterable, Set, NamedTuple
import argparse
import os
import tempfile
import hashlib
import string
import random
import pickle

from beancount.core.data import Transaction, Posting, Balance, Open, Close, Price, Directive, Entries, Amount
from beancount.core.flags import FLAG_PADDING
from beancount.core.number import MISSING, Decimal, ZERO
import beancount.parser.printer

from . import training
from . import matching
from . import journal_editor
from .source import ImportResult, load_source, SourceResults, Source, LogFunction, AssociatedData, InvalidSourceReference, invalid_source_reference_sort_key
from .posting_date import get_posting_date

from .thread_helpers import call_in_new_thread

from .matching import FIXME_ACCOUNT, is_unknown_account, CLEARED_KEY

display_prediction_explanation = False

classifier_cache_version_number = 1

PendingEntry = NamedTuple('PendingEntry', [
    ('date', datetime.date),
    ('entries', Entries),
    ('source', Optional[Source]),
    ('info', Optional[Mapping[str, Any]]),
    ('formatted', str),
    ('id', str),
])

AcceptCandidateResult = NamedTuple('AcceptCandidateResult', [
    ('new_entries', Entries),
    ('modified_filenames', List[str]),
])


def is_account_unknown(posting: Posting) -> bool:
    return is_unknown_account(posting.account)


def include_import_transaction(transaction: Transaction,
                               account_pattern: str) -> bool:
    for posting in transaction.postings:
        if not is_unknown_account(posting.account) and re.fullmatch(
                account_pattern, posting.account):
            return True
    return False


def include_import_result(import_result: ImportResult,
                          account_pattern: Optional[str]) -> bool:
    if account_pattern is None:
        return True
    for entry in import_result.entries:
        if isinstance(entry, Transaction):
            if include_import_transaction(entry, account_pattern):
                return True
        elif isinstance(entry, Balance):
            if re.fullmatch(account_pattern, entry.account):
                return True
            # else:
            #     print('excluding balance entry %r' % (entry,))
        else:
            return True
    return False


def _get_transaction_with_substitutions(transaction: Transaction,
                                        new_accounts: List[str]) -> Transaction:
    new_postings = []
    new_account_i = 0
    for posting in transaction.postings:
        if is_unknown_account(posting.account):
            posting = posting._replace(account=new_accounts[new_account_i])
            new_account_i += 1
        new_postings.append(posting)
    assert new_account_i == len(new_accounts)
    return transaction._replace(postings=new_postings)


def _replace_transaction_properties(transaction: Transaction,
                                    changes: dict) -> Transaction:
    if 'links' in changes:
        links = changes['links']
        if links is None:
            links = []
        assert isinstance(links, list) and all(
            isinstance(x, str) for x in links)
        links = frozenset(links)
    else:
        links = transaction.links

    if 'tags' in changes:
        tags = changes['tags']
        if tags is None:
            tags = []
        assert isinstance(tags, list) and all(isinstance(x, str) for x in tags)
        tags = frozenset(tags)
    else:
        tags = transaction.tags

    narration = changes.get('narration', transaction.narration)
    payee = changes.get('payee', transaction.payee)
    if narration is None:
        if payee is not None:
            narration = payee
            payee = None
        else:
            narration = ''
    return transaction._replace(
        links=links, tags=tags, narration=narration, payee=payee)


unique_id_characters = string.ascii_uppercase + string.ascii_lowercase


def _get_unique_id_for_account(account: str) -> str:
    length = max(20, len(account))
    return ''.join(random.choice(unique_id_characters) for _ in range(length))


def get_prediction_explanation(classifier, features: Dict[str, bool]):
    tree = classifier._clf.tree_

    lines = []

    converted_features = classifier._vectorizer.transform([features])
    class_names = classifier._encoder.classes_
    feature_names = classifier._vectorizer.get_feature_names_out()

    node_id = 0
    while True:
        if tree.children_left[node_id] < 0:
            # Leaf node
            class_index = tree.value[node_id].argmax()
            class_count = tree.value[node_id].max()
            lines.append('return %s (%g counts)' % (class_names[class_index],
                                                    class_count))
            break
        else:
            feature_index = tree.feature[node_id]
            feature_value = converted_features[0, feature_index]
            feature_threshold = tree.threshold[node_id]
            if feature_value <= feature_threshold:
                relation = '<='
                node_id = tree.children_left[node_id]
            else:
                relation = '> '
                node_id = tree.children_right[node_id]
            lines.append(
                '%s = %r %s %r' % (feature_names[feature_index], feature_value,
                                   relation, feature_threshold))
    return lines


AccountSubstitution = collections.namedtuple('AccountSubstitution', [
    'unique_name', 'account_name', 'group_number', 'unknown_account_name',
    'predicted_account_name'
])


class Candidate(object):
    def __init__(
            self,
            staged_changes: journal_editor.StagedChanges,
            staged_changes_with_unique_account_names: journal_editor.
            StagedChanges,
            used_import_results: List[Union[Transaction, ImportResult]],
            used_transactions: List[Transaction],
            substituted_accounts: Optional[List[AccountSubstitution]] = None,
            original_transaction_properties: Optional[dict] = None,
            substitute: Optional[Callable[[Dict[str, Any]], 'Candidate']] = None,
    ) -> None:
        self.staged_changes = staged_changes
        self.staged_changes_with_unique_account_names = staged_changes_with_unique_account_names
        self.used_import_results = used_import_results
        self.used_transactions = used_transactions

        # If not None, list of (unique_id, account_name, group_number)
        self.substituted_accounts = substituted_accounts

        self.original_transaction_properties = original_transaction_properties

        # If not None, Function that when called with list of account names (of same length as substituted_accounts) returns a new candidate.
        self.substitute = substitute

        self.used_transaction_ids = None  # type: Optional[List[int]]
        self.associated_data = []  # type: List[AssociatedData]

    def update_associated_data(self, sources: List[Source]) -> None:
        self.associated_data = []
        diff = self.staged_changes.get_diff()
        for entry in diff.new_entries:
            for source in sources:
                results = source.get_associated_data(entry)
                if results is not None:
                    self.associated_data.extend(results)


class Candidates(object):
    def __init__(
            self,
            candidates: List[Candidate],
            pending_data: List[PendingEntry],
            sources: List[Source],
            date: Optional[datetime.date] = None,
            number: Optional[Decimal] = None,
    ) -> None:
        self.candidates = candidates
        self.date = date
        self.number = number
        self.pending_data = pending_data
        self.sources = sources

        used_transaction_ids = collections.OrderedDict(
        )  # type: Dict[int, Tuple[Transaction, int]]
        for candidate in candidates:
            candidate.used_transaction_ids = [
                used_transaction_ids.setdefault(
                    id(transaction),
                    (transaction, len(used_transaction_ids)))[1]
                for transaction in candidate.used_transactions
            ]
            candidate.update_associated_data(self.sources)
        used_transaction_id_to_pending_index = {
            id(pending.entries[0]): index
            for index, pending in enumerate(pending_data)
        }
        self.used_transactions = [(transaction,
                                   used_transaction_id_to_pending_index.get(
                                       id_value, None)) for id_value,
                                  (transaction, _) in
                                  used_transaction_ids.items()]

    def change_transaction(self, candidate_index: int, changes: Dict[str, Any]):
        candidate = self.candidates[candidate_index]
        new_candidate = candidate.substitute(changes)  # type: ignore
        new_candidate.used_transaction_ids = candidate.used_transaction_ids
        new_candidate.update_associated_data(self.sources)
        self.candidates[candidate_index] = new_candidate


def with_metadata(x, new_meta):
    meta = collections.OrderedDict()
    if x.meta is not None:
        meta.update(x.meta)
    for k, v in new_meta.items():
        if k in journal_editor.META_IGNORE:
            continue
        meta[k] = v
    return x._replace(meta=meta)


def get_filename_from_map(account_map: List[Tuple[str, str]], account_name: str, default_output: str) -> str:
    if account_map is not None:
        for pattern, filename in account_map:
            if re.match(pattern, account_name):
                return filename
    return default_output


class EntryFileSelector(object):
    def __init__(self, default_map, open_map, balance_map, price_output,
                 default_output):
        self.default_map = default_map
        self.open_map = open_map
        self.balance_map = balance_map
        self.default_output = default_output
        if price_output is None:
            price_output = default_output
        self.price_output = price_output

    def __call__(self, entry):
        if isinstance(entry, Open) or isinstance(entry, Close):
            return get_filename_from_map(self.open_map, entry.account,
                                         self.default_output)
        if isinstance(entry, Transaction):
            for posting in entry.postings:
                result = get_filename_from_map(self.default_map,
                                               posting.account, None)
                if result is not None:
                    return result
            return self.default_output
        if isinstance(entry, Balance):
            return get_filename_from_map(self.balance_map, entry.account,
                                         self.default_output)
        if isinstance(entry, Price):
            return self.price_output
        return self.default_output

    @staticmethod
    def from_args(options):
        return EntryFileSelector(
            default_map=options['transaction_output_map'],
            price_output=options['price_output'],
            open_map=options['open_account_output_map'],
            default_output=options['default_output'],
            balance_map=options['balance_account_output_map'])


def get_entry_file_selector_argparser(kwargs):
    ap = argparse.ArgumentParser(add_help=False)
    ap.add_argument(
        '--default_output',
        help='Beancount output file to which new transactions will be appended',
        required=kwargs.get('default_output') is None)
    ap.add_argument(
        '--price_output',
        help='Beancount output file to which price entries will be appended')
    ap.add_argument(
        '--open_account_output_map',
        help='Beancount output file to which matching accounts will be appended',
        nargs=2,
        action='append')
    ap.add_argument(
        '--balance_account_output_map',
        help='Beancount output file to which balance entries will be appended',
        nargs=2,
        action='append')
    ap.add_argument(
        '--transaction_output_map',
        help=
        'Beancount output file to which matching transactions will be appended',
        nargs=2,
        action='append')
    return ap


def stage_missing_accounts(stage, entry_file_selector, account_map=None):
    """Stages Open directives for any missing accounts referenced in new entries."""
    for account, date, currencies in stage.get_missing_accounts(
            account_map=account_map):
        open_entry = Open(
            date=date,
            account=account,
            currencies=sorted(list(currencies)),
            meta=None,
            booking=None)
        stage.add_entry(open_entry, entry_file_selector(open_entry))


def make_pending_entry(import_result: ImportResult, source: Optional[Source]):
    printer = beancount.parser.printer.EntryPrinter()
    formatted = '\n'.join(printer(e) for e in import_result.entries)
    identifier = hashlib.sha256(formatted.encode()).hexdigest()
    return PendingEntry(
        date=import_result.date,
        entries=import_result.entries,
        source=source,
        info=import_result.info,
        formatted=formatted,
        id=identifier,
    )


class LoadedReconciler(object):
    """Represents the loaded reconciler state."""

    def __init__(self, reconciler, sources=None, classifier=None) -> None:
        self.reconciler = reconciler
        reconciler.log_status('Loading journal')
        self.editor = journal_editor.JournalEditor(reconciler.journal_path,
                                                   reconciler.ignore_path)
        self.errors = [('error', e[1], e[0]) for e in self.editor.errors]

        if sources is not None:
            self.sources = sources
        else:
            # Load sources
            self._load_sources()

        self.posting_db = matching.PostingDatabase(
            fuzzy_match_days=reconciler.options['fuzzy_match_days'],
            fuzzy_match_amount=reconciler.options['fuzzy_match_amount'],
            is_cleared=self.is_posting_cleared,
            metadata_keys=frozenset([matching.CHECK_KEY]),
        )

        # Set of ids of transactions pending import.  Used to determine whether a transaction found
        # in the posting_db is an existing or pending transaction.
        self.pending_transaction_ids = set()  # type: Set[int]

        self.balance_entries = dict(
        )  # type: Dict[Tuple[datetime.date, str, str], Decimal]
        self.price_values = set()  # type: Set[Tuple[datetime.date, str, Amount]]
        all_source_results = self._prepare_sources()
        self._preprocess_entries()
        self._match_sources(all_source_results)
        Preprocessor = self.reconciler.options.get('preprocessor')
        if Preprocessor:
            Preprocessor(self)
        all_skip_accounts = set()
        for src in all_source_results:
            all_skip_accounts.update(src.skip_training_accounts)
        self._feature_extractor = training.FeatureExtractor(
            account_source_map=self.account_source_map,
            ignore_account_pattern=reconciler.options[
                'ignore_account_for_classification_pattern'],
            sources=self.sources,
            skip_accounts=all_skip_accounts,
        )
        self.training_examples = training.TrainingExamples()
        self._extract_training_examples(self.editor.entries)

        self.classifier = classifier
        if self.classifier is None:
            classifier_cache_path = self.reconciler.options['classifier_cache']
            if classifier_cache_path is not None and os.path.exists(
                    classifier_cache_path):
                try:
                    with open(classifier_cache_path, 'rb') as cache_f:
                        cache_data = pickle.load(cache_f)
                        version = cache_data['version']
                        if version != classifier_cache_version_number:
                            raise RuntimeError('invalid version')
                        self.classifier = cache_data['classifier']
                except:
                    import traceback
                    traceback.print_exc()
                    print('Not using classifier cache due to above error')

        if self.classifier is None:
            self._maybe_train_classifier()

    def _extract_training_examples(self, entries: Entries) -> None:
        self._feature_extractor.extract_examples(entries,
                                                 self.training_examples)

    def _load_sources(self):
        sources = self.sources = [
            load_source(spec, log_status=self.reconciler.log_status)
            for spec in self.reconciler.options['data_sources']
        ]

    def _preprocess_entries(self):
        posting_db = self.posting_db
        for entry in self.editor.entries:
            if isinstance(entry, Transaction):
                posting_db.add_transaction(entry)
        for entry in self.editor.all_entries:
            if isinstance(entry, Price):
                self.price_values.add((entry.date, entry.currency,
                                       entry.amount))
            elif isinstance(entry, Balance):
                key = (entry.date, entry.account, entry.amount.currency)
                self.balance_entries[key] = entry.amount.number

    def is_posting_cleared(self, posting: Posting) -> bool:
        source = self.account_source_map.get(posting.account)
        if source is None: return False
        return source.is_posting_cleared(posting)

    def retrain(self):
        self._maybe_train_classifier()
        return self

    def _maybe_train_classifier(self):
        training_examples = [
            x for x in self.training_examples.training_examples
            if x[1] != FIXME_ACCOUNT
        ]
        if len(training_examples) > 0:
            self.reconciler.log_status(
                'Training classifier with %d examples' % len(training_examples))
            import nltk
            import sklearn.tree

            self.classifier = nltk.classify.scikitlearn.SklearnClassifier(
                estimator=sklearn.tree.DecisionTreeClassifier())
            self.classifier.train(training_examples)
            self.reconciler.log_status(
                'Trained classifier with %d examples.' % len(training_examples))
            classifier_cache_path = self.reconciler.options['classifier_cache']
            if classifier_cache_path is None:
                return
            renamed = False
            cache_data = {
                'version': classifier_cache_version_number,
                'classifier': self.classifier
            }
            with tempfile.NamedTemporaryFile(
                    mode='wb',
                    dir=os.path.dirname(classifier_cache_path),
                    prefix='.' + os.path.basename(classifier_cache_path),
                    suffix='.tmp',
                    delete=False) as cache_f:
                try:
                    pickle.dump(cache_data, cache_f)
                    os.rename(cache_f.name, classifier_cache_path)
                    renamed = True
                finally:
                    if not renamed:
                        os.remove(cache_f.name)
            # sklearn.tree.export_graphviz(self.classifier._clf,
            #                              feature_names=self.classifier._vectorizer.get_feature_names(),
            #                              class_names=self.classifier._encoder.classes_,
            #                              out_file='/tmp/tree.dot')
            # print('Evaluating accuracy of classifier')
            # errors = 0
            # for features, label in training_examples:
            #     if self.classifier.classify(features) != label:
            #         errors += 1
            # print('Classifier accuracy: %.4f', 1 - float(errors) / len(training_examples))

    def _prepare_sources(self) -> List[SourceResults]:
        self.reconciler.log_status('Matching source data')
        self.account_source_map = dict()  # type: Dict[str, Source]
        invalid_references = [
        ]  # type: List[Tuple[Source, InvalidSourceReference]]
        all_source_results = []  # type: List[SourceResults]
        for source in self.sources:
            source_results = SourceResults()
            source.prepare(self.editor, source_results)
            for account in source_results.accounts:
                self.account_source_map[account] = source
            for message in source_results.messages:
                message_source = {'source': source.name}
                meta = message[2]
                if meta is not None:
                    for k in ('filename', 'lineno'):
                        if k in meta:
                            message_source[k] = meta[k]
                self.errors.append((message[0], message[1], message_source))
            invalid_references.extend(
                (source, r) for r in source_results.invalid_references)
            all_source_results.append(source_results)
        invalid_references.sort(
            key=lambda x: invalid_source_reference_sort_key(x[1]))
        self.invalid_references = invalid_references
        return all_source_results

    def _match_sources(self, all_source_results: List[SourceResults]):
        source_balance_and_price_entries = collections.OrderedDict(
        )  # type: Dict[Source, List[Directive]]

        import_results = []
        for source, source_results in zip(self.sources, all_source_results):
            filtered_import_results, balance_and_price_entries = self._filter_import_results(
                source, source_results.pending)
            if balance_and_price_entries:
                balance_and_price_entries.sort(key=lambda x: x.date)
                source_balance_and_price_entries[
                    source] = balance_and_price_entries
            import_results.extend(filtered_import_results)
        import_results.sort(key=lambda x: x.date)
        self.errors.sort(key=lambda x: x[0] == 'warning')

        # Produce final candidates with pending balance and price entries.
        for source, balance_and_price_entries in source_balance_and_price_entries.items(
        ):
            import_results.append(
                make_pending_entry(
                    ImportResult(
                        date=balance_and_price_entries[0].date,
                        entries=balance_and_price_entries,
                        info=None),
                    source=source))

        # Add FIXME transactions
        fixme_transactions = self._get_fixme_transactions()
        fixme_transactions.sort(key=lambda x: x.date)
        for entry in fixme_transactions:
            import_results.append(
                make_pending_entry(
                    ImportResult(date=entry.date, entries=(entry, ), info=None),
                    None))

        self.uncleared_postings = []  # type: List[Tuple[Transaction, Posting]]
        self._get_uncleared_postings()

        self.pending_data = import_results
        self.reconciler.log_status('Done loading')

    def _get_fixme_transactions(self):
        output = []
        for entry in self.editor.entries:
            if isinstance(entry, Transaction):
                if any(
                        is_unknown_account(posting.account)
                        for posting in entry.postings):
                    output.append(entry)
        return output

    def _add_uncleared_postings_from(self,
                                     entries: Iterable[Directive]) -> None:
        cleared_dates = self.cleared_dates
        uncleared = self.uncleared_postings
        account_source_map = self.account_source_map
        default_cleared = (datetime.date.min, datetime.date.max)
        for entry in entries:
            if not isinstance(entry, Transaction): continue
            if entry.flag == FLAG_PADDING: continue
            for posting in entry.postings:
                if posting.meta and posting.meta.get(CLEARED_KEY) == True:
                    continue
                if posting.units is not MISSING and posting.units.number == ZERO:
                    continue
                source = account_source_map.get(posting.account)
                if source is None: continue
                if source.is_posting_cleared(posting): continue
                cleared_before, cleared_after = cleared_dates.get(
                    posting.account, default_cleared)
                d = get_posting_date(entry, posting)
                if d < cleared_before or d > cleared_after:
                    continue
                uncleared.append((entry, posting))

    def _get_uncleared_postings(self):
        cleared_dates = dict(
        )  # type: Dict[str, Tuple[datetime.date,datetime.date]]
        for account_name in sorted(self.editor.accounts):
            account = self.editor.accounts[account_name]
            meta = account.meta
            if meta is None: continue
            parts = account_name.split(':')
            cleared_before = datetime.date.min
            cleared_after = datetime.date.max
            for part_i in range(1, len(parts)):
                ancestor_account_name = ':'.join(parts[:part_i])
                ancestor_cleared_dates = cleared_dates.get(ancestor_account_name)
                if ancestor_cleared_dates is None: continue
                (ancestor_cleared_before,
                 ancestor_cleared_after) = ancestor_cleared_dates
                cleared_before = max(cleared_before, ancestor_cleared_before)
                cleared_after = min(cleared_after, ancestor_cleared_after)

            cur_cleared_before = meta.get('cleared_before', datetime.date.min)
            if not isinstance(cur_cleared_before, datetime.date):
                self.errors.append(
                    ('error', '%s: Expected cleared_before value to be a date' %
                     (account_name, ), meta))
                cur_cleared_before = datetime.date.min
            cleared_before = max(cleared_before, cur_cleared_before)
            cur_cleared_after = meta.get('cleared_after', datetime.date.max)
            if not isinstance(cur_cleared_after, datetime.date):
                self.errors.append(
                    ('error', '%s: Expected cleared_after value to be a date' %
                     (account_name, ), meta))
                cur_cleared_after = datetime.date.max
            cleared_after = min(cleared_after, cur_cleared_after)
            if cleared_before != datetime.date.min or cleared_after != datetime.date.max:
                cleared_dates[account_name] = (cleared_before, cleared_after)
        self.cleared_dates = cleared_dates
        self._add_uncleared_postings_from(self.editor.entries)

    def _filter_import_results(self, source: Source,
                               import_results: List[ImportResult]
                               ) -> Tuple[List[PendingEntry], List[Directive]]:
        account_pattern = self.reconciler.options['account_pattern']
        output = []
        balance_and_price_entries = []  # type: List[Directive]
        posting_db = self.posting_db
        pending_transaction_ids = self.pending_transaction_ids
        for import_result in import_results:
            if not include_import_result(import_result, account_pattern):
                continue
            filtered_entries = []
            only_balance_or_price = True
            for entry in import_result.entries:
                if isinstance(entry, Price):
                    key = (entry.date, entry.currency, entry.amount)
                    if key in self.price_values:
                        continue
                    self.price_values.add(key)
                elif isinstance(entry, Balance):
                    key = (entry.date, entry.account, entry.amount.currency)
                    if key in self.balance_entries:
                        continue
                    self.balance_entries[key] = entry.amount.number
                else:
                    pending_transaction_ids.add(id(entry))
                    posting_db.add_transaction(entry)
                    only_balance_or_price = False
                filtered_entries.append(entry)
            if only_balance_or_price:
                balance_and_price_entries.extend(filtered_entries)
                continue
            elif not filtered_entries:
                continue
            import_result = import_result._replace(entries=filtered_entries)
            output.append(make_pending_entry(import_result, source))
        return output, balance_and_price_entries

    @property
    def num_pending(self) -> int:
        return len(self.pending_data)

    def predict_account(
            self, prediction_input: Optional[training.PredictionInput]) -> str:
        if self.classifier is None or prediction_input is None:
            return FIXME_ACCOUNT
        features = training.get_features(prediction_input)
        explanation = get_prediction_explanation(self.classifier, features)
        predicted_account = self.classifier.classify(features)
        if display_prediction_explanation:
            print('\n'.join(explanation))
            print('predicted account = %r' % (predicted_account, ))
        return predicted_account

    def _get_generic_stage(self, entries: Entries):
        stage = self.editor.stage_changes()
        for entry in entries:
            output_filename = self.reconciler.entry_file_selector(entry)
            stage.add_entry(entry, output_filename)
        stage_missing_accounts(stage, self.reconciler.entry_file_selector)
        return stage

    def _get_primary_transaction_amount_number(self, transaction: Transaction):
        num_unknown_accounts = sum(
            is_account_unknown(p) for p in transaction.postings)
        non_ignored_postings = self._feature_extractor.get_postings_for_automatic_classification(
            transaction.postings)

        if len(non_ignored_postings) == 2 and num_unknown_accounts == 1:
            source_posting = (non_ignored_postings[0]
                              if is_account_unknown(non_ignored_postings[1])
                              else non_ignored_postings[0])
            if source_posting.units is not None and source_posting.units is not MISSING:
                return -source_posting.units.number
        return None

    def _get_unknown_account_predictions(self,
                                         transaction: Transaction) -> List[str]:
        group_prediction_inputs = self._feature_extractor.extract_unknown_account_group_features(
            transaction)
        group_predictions = [
            self.predict_account(prediction_input)
            for prediction_input in group_prediction_inputs
        ]
        group_numbers = training.get_unknown_account_group_numbers(transaction)
        return [
            group_predictions[group_number] for group_number in group_numbers
        ]

    def _make_candidate_with_substitutions(self,
                                           transaction: Transaction,
                                           used_transactions: List[Transaction],
                                           predicted_accounts: List[str],
                                           changes: dict = {}):
        assert isinstance(changes, dict)
        new_accounts = changes.get('accounts')
        if new_accounts is None:
            new_accounts = predicted_accounts
        else:
            assert isinstance(new_accounts, list) and all(
                isinstance(x, str) for x in new_accounts)
        unique_ids = [
            _get_unique_id_for_account(account) for account in new_accounts
        ]
        account_map = {
            unique_id: account
            for unique_id, account in zip(unique_ids, new_accounts)
        }
        group_numbers = training.get_unknown_account_group_numbers(transaction)
        unknown_names = training.get_unknown_account_names(transaction)
        substitutions = [
            AccountSubstitution(
                unique_name=unique_id,
                account_name=new_account,
                group_number=group_number,
                unknown_account_name=unknown_account,
                predicted_account_name=predicted_account)
            for unique_id, new_account, group_number, unknown_account,
            predicted_account in zip(unique_ids, new_accounts, group_numbers,
                                     unknown_names, predicted_accounts)
        ]

        def substitute(changes: dict):
            return self._make_candidate_with_substitutions(
                transaction,
                used_transactions,
                changes=changes,
                predicted_accounts=predicted_accounts)

        new_transaction = _replace_transaction_properties(transaction, changes)
        real_transaction = _get_transaction_with_substitutions(
            new_transaction, new_accounts)
        transaction_with_unique_account_names = _get_transaction_with_substitutions(
            new_transaction, unique_ids)
        existing_used_transactions = [
            t for t in used_transactions
            if id(t) not in self.pending_transaction_ids
        ]

        def make_stage(new_transaction, account_map):
            stage = self.editor.stage_changes()
            if existing_used_transactions:
                stage.change_entry(existing_used_transactions[0],
                                   new_transaction)
                for old_entry in existing_used_transactions[1:]:
                    stage.remove_entry(old_entry)
            else:
                stage.add_entry(
                    new_transaction,
                    self.reconciler.entry_file_selector(new_transaction))
            stage_missing_accounts(stage, self.reconciler.entry_file_selector,
                                   account_map)
            return stage

        real_stage = make_stage(real_transaction, account_map=None)
        stage_with_unique_account_names = make_stage(
            transaction_with_unique_account_names, account_map=account_map)
        return Candidate(
            staged_changes=real_stage,
            staged_changes_with_unique_account_names=
            stage_with_unique_account_names,
            used_import_results=used_transactions,
            used_transactions=used_transactions,
            substituted_accounts=substitutions,
            original_transaction_properties=dict(
                tags=transaction.tags,
                links=transaction.links,
                payee=transaction.payee,
                narration=transaction.narration,
            ),
            substitute=substitute)

    def _make_candidates_from_import_result(self, next_pending):
        if len(next_pending.entries) == 1 and isinstance(
                next_pending.entries[0], Transaction):
            next_entry = next_pending.entries[0]
            candidates = []
            match_results = matching.get_extended_transactions(
                next_entry, posting_db=self.posting_db)
            # Always include the original transaction.
            match_results.append((next_entry, [next_entry]))
            for transaction, used_transactions in match_results:
                predicted_accounts = self._get_unknown_account_predictions(
                    transaction)
                candidates.append(
                    self._make_candidate_with_substitutions(
                        transaction,
                        used_transactions,
                        predicted_accounts=predicted_accounts))
            result = Candidates(
                candidates=candidates,
                date=next_entry.date,
                number=self._get_primary_transaction_amount_number(next_entry),
                pending_data=self.pending_data,
                sources=self.sources,
            )
        else:
            assert next_pending.source is not None
            stage = self._get_generic_stage(next_pending.entries)
            result = Candidates(
                candidates=[
                    Candidate(
                        staged_changes=stage,
                        staged_changes_with_unique_account_names=stage,
                        used_import_results=[next_pending],
                        used_transactions=[])
                ],
                date=next_pending.date,
                pending_data=self.pending_data,
                sources=self.sources,
            )
        return result

    def get_next_candidates(self, skip_ids: Optional[Dict[str, int]] = None):
        if self.pending_data:
            if skip_ids is None:
                skip_ids = collections.Counter()
            new_skip_ids = collections.Counter()  # type: Dict[str, int]
            for i, pending in enumerate(self.pending_data):
                existing_count = skip_ids[pending.id]
                if existing_count > 0:
                    new_skip_ids[pending.id] += 1
                    skip_ids[pending.id] -= 1
                else:
                    break
            return self._make_candidates_from_import_result(
                pending), i, new_skip_ids
        return None, None, collections.Counter()

    def get_skip_ids_by_index(self, index: int):
        skip_ids = collections.Counter()  # type: Dict[str, int]
        for i, pending in enumerate(self.pending_data):
            if i >= index:
                break
            skip_ids[pending.id] += 1
        return skip_ids

    def accept_candidate(self, candidate: Candidate, ignore=False) -> AcceptCandidateResult:
        ignored_path = self.editor.ignored_path
        if ignored_path is None:
            raise RuntimeError(
                'Cannot ignore candidate without an "ignored" journal having been specified.'
            )
        staged_changes = candidate.staged_changes
        if ignore:
            staged_changes = staged_changes.make_with_new_output_filename(
                ignored_path)
        result = staged_changes.apply()
        old_entries = result.old_entries
        new_entries = result.new_entries

        for entry in old_entries:
            if isinstance(entry, Transaction):
                self.posting_db.remove_transaction(entry)

        old_entry_ids = set(id(x) for x in old_entries)
        self.uncleared_postings = [
            x for x in self.uncleared_postings if id(x[0]) not in old_entry_ids
        ]
        for import_result in candidate.used_import_results:
            if isinstance(import_result, Transaction):
                if id(import_result) in self.pending_transaction_ids:
                    self.pending_transaction_ids.remove(id(import_result))
                    self.posting_db.remove_transaction(import_result)

        self._add_uncleared_postings_from(new_entries)
        self.uncleared_postings.sort(key=lambda x: x[0].date)
        for entry in new_entries:
            if isinstance(entry, Transaction):
                self.posting_db.add_transaction(entry)

        self._extract_training_examples(new_entries)

        used_import_result_ids = frozenset(
            map(id, candidate.used_import_results))
        self.pending_data = [
            e for e in self.pending_data
            if id(e) not in used_import_result_ids and
            id(e.entries[0]) not in used_import_result_ids
        ]
        return AcceptCandidateResult(
            new_entries=new_entries + result.new_ignored_entries,
            modified_filenames=staged_changes.get_modified_filenames(),
        )


class Reconciler(object):
    """Holds the reconciler configuration and asynchronously loads a reconciler."""

    def __init__(self, journal_path: str, log_status: LogFunction,
                 ignore_path: str, options: dict) -> None:
        self.options = options
        self.journal_path = journal_path
        self.ignore_path = ignore_path
        self.log_status = log_status
        self.entry_file_selector = EntryFileSelector.from_args(options)
        self.loaded_future = call_in_new_thread(
            LoadedReconciler, reconciler=self, classifier=None)

    def reload_journal(self):
        assert self.loaded_future.done()
        loaded_reconciler = self.loaded_future.result()
        classifier = loaded_reconciler.classifier
        existing_sources = loaded_reconciler.sources
        self.loaded_future = call_in_new_thread(
            LoadedReconciler,
            reconciler=self,
            classifier=classifier,
            sources=existing_sources)

    def retrain(self):
        assert self.loaded_future.done()
        loaded_reconciler = self.loaded_future.result()
        self.loaded_future = call_in_new_thread(loaded_reconciler.retrain)
