"""Cache lifecycle tests use invented invoice objects and parser outcomes."""
import os
import pickle

import pytest

from . import amazon, SourceResults
from .amazon_test import synthetic_card_invoice
from ..journal_editor import JournalEditor


@pytest.fixture
def cache_case(tmp_path, monkeypatch, synthetic_card_invoice):
    inputs = tmp_path / 'invoices'
    inputs.mkdir()
    cache = tmp_path / 'cache'
    cache.mkdir()
    path = inputs / '111-0000001-0000001.html'
    path.write_text('<html>Invented test input</html>')
    journal_path = tmp_path / 'journal.beancount'
    journal_path.write_text('')
    journal = JournalEditor(str(journal_path))
    calls = []

    def make_source(outcome='invoice', locale='en_US', persistent=True):
        def parse(*args, **kwargs):
            calls.append(path)
            if isinstance(outcome, Exception):
                raise outcome
            return synthetic_card_invoice if outcome == 'invoice' else None

        monkeypatch.setattr(amazon, 'parse_invoice', parse)
        return amazon.AmazonSource(
            directory=str(inputs), pickle_dir=str(cache) if persistent else None,
            amazon_account='example@example.com', locale=locale,
            log_status=lambda message: None)

    def prepare(source):
        results = SourceResults()
        source.prepare(journal, results)
        return results

    return path, cache, calls, make_source, prepare


@pytest.mark.parametrize('outcome', ['invoice', None, ValueError('Invented invalid invoice')])
def test_outcomes_reused_across_source_reload(cache_case, outcome):
    path, cache, calls, make_source, prepare = cache_case
    first = prepare(make_source(outcome))
    second = prepare(make_source(outcome))
    assert len(calls) == 1
    assert len(first.pending) == len(second.pending) == (1 if outcome == 'invoice' else 0)
    if isinstance(outcome, Exception):
        assert len(first.messages) == len(second.messages) == 1
        assert all('Invented invalid invoice' in results.messages[0][1]
                   for results in (first, second))
    else:
        assert first.messages == second.messages == []


@pytest.mark.parametrize('outcome', ['invoice', None, ValueError('Invented invalid invoice')])
def test_changed_input_retries_in_memory_and_on_disk(cache_case, outcome):
    path, cache, calls, make_source, prepare = cache_case
    source = make_source(outcome)
    prepare(source)
    before = path.stat()
    # Same-size edit with an older timestamp must also invalidate the cache.
    path.write_text('<html>Modified test input</html>')
    os.utime(path, ns=(before.st_atime_ns, before.st_mtime_ns - 1000000000))
    prepare(source)
    assert len(calls) == 2
    prepare(make_source(outcome))
    assert len(calls) == 2


@pytest.mark.parametrize('change', ['locale', 'parser', 'path'])
def test_changed_parser_locale_or_input_directory_retries(cache_case, change, tmp_path):
    path, cache, calls, make_source, prepare = cache_case
    prepare(make_source(ValueError('Invented invalid invoice')))
    source = make_source('invoice', locale='de_DE' if change == 'locale' else 'en_US')
    if change == 'parser':
        source.pickler.parser_fingerprint = 'new-parser-version'
    elif change == 'path':
        other = tmp_path / 'other-invoices'
        other.mkdir()
        new_path = other / path.name
        new_path.write_bytes(path.read_bytes())
        before = path.stat()
        os.utime(new_path, ns=(before.st_atime_ns, before.st_mtime_ns))
        source.directory = str(other)
    results = prepare(source)
    assert len(calls) == 2
    assert len(results.pending) == 1
    assert results.messages == []


def test_disabled_persistent_cache_reparses_after_reload(cache_case):
    path, cache, calls, make_source, prepare = cache_case
    for _ in range(2):
        prepare(make_source(None, persistent=False))
    assert len(calls) == 2
    assert list(cache.iterdir()) == []


def test_transient_io_failure_retried(cache_case):
    path, cache, calls, make_source, prepare = cache_case
    source = make_source(PermissionError('Invented transient failure'))
    assert len(prepare(source).messages) == 1
    assert len(prepare(source).messages) == 1
    assert len(calls) == 2
    assert list(cache.iterdir()) == []


@pytest.mark.parametrize('cached', ['corrupt', 'legacy'])
def test_unusable_cache_reparsed(cache_case, synthetic_card_invoice, cached):
    path, cache, calls, make_source, prepare = cache_case
    data = b'not a pickle' if cached == 'corrupt' else pickle.dumps(synthetic_card_invoice)
    (cache / '111-0000001-0000001.order.p').write_bytes(data)
    results = prepare(make_source())
    assert len(calls) == len(results.pending) == 1
    assert len(results.messages) == (1 if cached == 'corrupt' else 0)


def test_cache_write_failure_does_not_prevent_import(cache_case, monkeypatch):
    path, cache, calls, make_source, prepare = cache_case

    def fail(*args):
        raise PermissionError('Invented cache write failure')

    monkeypatch.setattr(amazon.os, 'replace', fail)
    results = prepare(make_source())
    assert len(results.pending) == 1
    assert len(results.messages) == 1
    assert 'Failed to save pickled invoice' in results.messages[0][1]
    assert list(cache.iterdir()) == []


def test_file_changed_during_parse_is_not_cached(cache_case, monkeypatch, synthetic_card_invoice):
    path, cache, calls, make_source, prepare = cache_case
    source = make_source()

    def parse(*args, **kwargs):
        calls.append(path)
        path.write_text(path.read_text() + ' changed')
        return synthetic_card_invoice

    monkeypatch.setattr(amazon, 'parse_invoice', parse)
    prepare(source)
    prepare(source)
    assert len(calls) == 2
    assert list(cache.iterdir()) == []
