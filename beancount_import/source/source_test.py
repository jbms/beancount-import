from typing import List, Optional, Union, Tuple, Dict, Any

import collections
import io
import os
import json

import beancount.parser.parser
import beancount.parser.printer
from beancount.core.data import Posting, Transaction, Meta, Directive, Entries

from ..journal_editor import JournalEditor
from . import load_source as _load_source, ImportResult, SourceResults, SourceSpec, InvalidSourceReference, PredictionInput, Source, invalid_source_reference_sort_key
from .. import training
from .. import test_util


def log_status(x):
    print(x)


def load_source(source_spec: SourceSpec):
    return _load_source(source_spec, log_status=log_status)


def _json_encode_prediction_input(x: Optional[PredictionInput]) -> Any:
    if x is None: return None
    result = x._asdict()
    result['amount'] = str(result['amount'])
    result['date'] = result['date'].strftime('%Y-%m-%d')
    return result


def _format_import_results(import_results: List[ImportResult],
                           extractor: training.FeatureExtractor,
                           source: Source) -> str:
    out = io.StringIO()
    for import_result in import_results:
        out.write(';; date: %s\n' % import_result.date.strftime('%Y-%m-%d'))
        out.write(
            ';; info: %s\n\n' % json.dumps(import_result.info, sort_keys=True))
        for entry in import_result.entries:
            if isinstance(entry, Transaction):
                entry = entry._replace(
                    meta=collections.OrderedDict(entry.meta or {}),
                    postings=[
                        posting._replace(
                            meta=collections.OrderedDict(posting.meta or {}))
                        for posting in entry.postings
                    ],
                )
                features = extractor.extract_unknown_account_group_features(
                    entry)
                if features is not None:
                    features_json = json.dumps(
                        [_json_encode_prediction_input(x) for x in features],
                        sort_keys=True,
                        indent='  ')
                    prefix0 = '; features: '
                    prefix1 = ';           '
                    prefix = prefix0
                    for line in features_json.split('\n'):
                        out.write(prefix + line + '\n')
                        prefix = prefix1
                associated_data = source.get_associated_data(entry) or []
                for i, data in enumerate(associated_data):
                    data_rep = dict(vars(data))
                    del data_rep['posting']
                    for key in [k for k, v in data_rep.items() if v is None]:
                        del data_rep[key]
                    data_json = json.dumps(data_rep, sort_keys=True)
                    meta_key = 'associated_data%d' % i
                    if data.posting is not None:
                        data.posting.meta[meta_key] = data_json
                    else:
                        entry.meta[meta_key] = data_json

            out.write(test_util.format_entries([entry]).strip() + '\n\n')
    return out.getvalue().strip() + '\n'


def _add_invalid_reference_and_cleared_metadata(
        editor: JournalEditor, source: Source,
        invalid_references: List[InvalidSourceReference]) -> Dict[str, str]:
    id_to_invalid_keys = {}  # type: Dict[int, List[Tuple[str, str]]]
    for i, ref in enumerate(invalid_references):
        invalid_pair = ('invalid%d' % (i, ), '%d extra' % (ref.num_extras, ))
        for transaction, posting in ref.transaction_posting_pairs:
            if posting is None:
                id_to_invalid_keys.setdefault(id(transaction),
                                              []).append(invalid_pair)
            else:
                id_to_invalid_keys.setdefault(id(posting),
                                              []).append(invalid_pair)

    def _adjust_meta(
            directive: Union[Directive, Posting]) -> Union[Directive, Posting]:
        meta = collections.OrderedDict(
            sorted((key, value)
                   for key, value in (directive.meta or {}).items()
                   if not key.startswith('invalid') and key != 'cleared'))
        if isinstance(directive,
                      Posting) and source.is_posting_cleared(directive):
            meta['cleared'] = True
        for key, value in id_to_invalid_keys.get(id(directive), []):
            meta[key] = value
        if dict(meta) == directive.meta:
            return directive
        return directive._replace(meta=meta)

    def _adjust_entry(entry: Directive) -> Directive:
        if not isinstance(entry, Transaction): return entry
        new_entry = _adjust_meta(entry)
        modified = new_entry is not entry
        new_postings = []
        for posting in entry.postings:
            new_posting = _adjust_meta(posting)
            if new_posting is not posting:
                modified = True
            new_postings.append(new_posting)
        new_entry = new_entry._replace(postings=new_postings)
        if modified:
            return new_entry
        return entry

    stage = editor.stage_changes()
    for entry in editor.all_entries:
        new_entry = _adjust_entry(entry)
        if new_entry is not entry:
            stage.change_entry(entry, new_entry)
    return {
        filename: result.new_contents
        for filename, result in editor.get_file_change_results(
            stage.get_diff().change_sets).items()
    }


def check_source_example(example_dir: str,
                         source_spec: SourceSpec,
                         replacements: List[Tuple[str, str]],
                         write: Optional[bool] = None):
    journal_path = os.path.join(example_dir, 'journal.beancount')
    editor = JournalEditor(journal_path)
    assert editor.errors == []
    source = load_source(source_spec)
    results = SourceResults()
    source.prepare(editor, results)
    results.pending.sort(key=lambda x: x.date)
    account_source_map = {account: source for account in results.accounts}
    sources = [source]
    extractor = training.FeatureExtractor(
        sources=sources, account_source_map=account_source_map)
    for filename, new_contents in _add_invalid_reference_and_cleared_metadata(
            editor, source,
            sorted(
                results.invalid_references,
                key=invalid_source_reference_sort_key)).items():
        test_util.check_golden_contents(
            filename,
            new_contents,
            replacements=[],
            write=write,
        )
    test_util.check_golden_contents(
        os.path.join(example_dir, 'import_results.beancount'),
        _format_import_results(
            results.pending, extractor=extractor, source=source),
        replacements=replacements,
        write=write,
    )
    test_util.check_golden_contents(
        os.path.join(example_dir, 'accounts.txt'),
        ''.join(account + '\n' for account in sorted(results.accounts)),
        replacements=replacements,
        write=write,
    )

    training_examples = training.MockTrainingExamples()
    extractor.extract_examples(editor.entries, training_examples)

    test_util.check_golden_contents(
        os.path.join(example_dir, 'training_examples.json'),
        json.dumps(
            [[_json_encode_prediction_input(prediction_input), target]
             for prediction_input, target in training_examples.examples],
            indent='  ',
            sort_keys=True),
        replacements=replacements,
        write=write,
    )
