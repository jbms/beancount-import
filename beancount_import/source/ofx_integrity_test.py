from datetime import date
from decimal import Decimal
import pickle

import pytest
from beancount import loader
from beancount.core.data import Balance, Transaction

from beancount_import.journal_editor import JournalEditor
from beancount_import.source import SourceResults, ofx


JOURNAL = '''2026-06-01 open Assets:Fixture AUD
  ofx_org: "AU-DEMO"
  ofx_broker_id: ""
  account_id: "000100"
  ofx_account_type: "cash_only"
2026-06-01 open Equity:Fixture AUD
2026-06-30 * "Opening"
  Assets:Fixture  100.00 AUD
  Equity:Fixture -100.00 AUD
2026-07-01 * "Deposit"
  Assets:Fixture  0.05 AUD
  Equity:Fixture -0.05 AUD
'''


def write_statement(path, currency_element='<CURDEF>AUD\n',
                    as_of='20260701235959[+10:AEST]'):
    path.write_text(f'''<OFX><SONRS><FI><ORG>AU-DEMO\n</FI></SONRS>
<BANKMSGSRSV1><STMTTRNRS><STMTRS>{currency_element}<BANKACCTFROM><ACCTID>000100\n</BANKACCTFROM>
<BANKTRANLIST><STMTTRN><TRNTYPE>OTHER\n<DTPOSTED>20260701001500[+10:AEST]
<TRNAMT>0.05\n<FITID>FIXTURE-1\n<NAME>CROFT MANOR FIXTURE\n</STMTTRN></BANKTRANLIST>
<LEDGERBAL><BALAMT>100.05\n<DTASOF>{as_of}\n</LEDGERBAL>
</STMTRS></STMTTRNRS></BANKMSGSRSV1></OFX>''', encoding='ascii')
    return path


def import_statements(directory, *files, cache=None):
    ledger = directory / 'test.beancount'
    ledger.write_text(JOURNAL, encoding='utf-8')
    spec = {'ofx_filenames': [str(path) for path in files]}
    if cache is not None:
        spec['cache_filename'] = str(cache)
    source = ofx.load(spec, log_status=lambda message: None)
    results = SourceResults()
    source.prepare(JournalEditor(str(ledger)), results)
    return source, [entry for pending in results.pending for entry in pending.entries], results.messages


@pytest.mark.parametrize('as_of', ['20260701235959[+10:AEST]',
                                    '20260701235959.500[+10:AEST]'])
def test_closing_balance_is_asserted_at_start_of_next_local_day(tmp_path, as_of):
    statement = write_statement(tmp_path / 'close.ofx', as_of=as_of)
    _, entries, messages = import_statements(tmp_path, statement)
    assert not messages
    [balance] = [entry for entry in entries if isinstance(entry, Balance)]
    assert balance.date == date(2026, 7, 2)
    _, errors, _ = loader.load_string(
        JOURNAL + f'{balance.date} balance {balance.account} {balance.amount}\n')
    assert not errors


@pytest.mark.parametrize('as_of', ['20260701', '20260701000000[+10:AEST]',
                                     '20260701163000[+10:AEST]'])
def test_other_balance_timestamps_keep_the_local_date(tmp_path, as_of):
    statement = write_statement(tmp_path / 'balance.ofx', as_of=as_of)
    _, entries, messages = import_statements(tmp_path, statement)
    assert not messages
    [balance] = [entry for entry in entries if isinstance(entry, Balance)]
    assert balance.date == date(2026, 7, 1)


@pytest.mark.parametrize('currency_element', ['', '<CURDEF>   \n', '<CURDEF>'])
def test_missing_statement_currency_is_reported_without_consuming_fitid(tmp_path, currency_element):
    invalid = write_statement(tmp_path / 'invalid.ofx', currency_element)
    valid = write_statement(tmp_path / 'valid.ofx')
    duplicate = write_statement(tmp_path / 'duplicate.ofx')
    source, entries, messages = import_statements(tmp_path, invalid)
    assert not entries
    assert not source.source_fitids
    [(severity, message, location)] = messages
    assert severity == 'error' and 'CURDEF' in message
    assert location['filename'] == str(invalid)

    source, entries, repeated_messages = import_statements(tmp_path, invalid, valid, duplicate)
    assert repeated_messages == messages
    assert len(source.source_fitids) == 1
    [transaction] = [entry for entry in entries if isinstance(entry, Transaction)]
    [posting] = [posting for posting in transaction.postings if posting.account == 'Assets:Fixture']
    assert transaction.date == date(2026, 7, 1)
    assert posting.units.number == Decimal('0.05')
    assert posting.units.currency == 'AUD'


@pytest.mark.parametrize('cache_version', [4, 5])
def test_old_cache_reparses_closing_balance(tmp_path, monkeypatch, cache_version):
    path = tmp_path / 'balance.ofx'
    cache = tmp_path / 'ofx.cache'
    with monkeypatch.context() as old_cache:
        old_cache.setattr(ofx, 'cache_version_number', cache_version)
        statement = write_statement(path, as_of='20260701')
        _, entries, _ = import_statements(tmp_path, statement, cache=cache)
        [balance] = [entry for entry in entries if isinstance(entry, Balance)]
        assert balance.date == date(2026, 7, 1)
    assert cache.is_file()

    statement = write_statement(path)
    _, entries, messages = import_statements(tmp_path, statement, cache=cache)
    assert not messages
    [balance] = [entry for entry in entries if isinstance(entry, Balance)]
    assert balance.date == date(2026, 7, 2)
    [transaction] = [entry for entry in entries if isinstance(entry, Transaction)]
    [posting] = [p for p in transaction.postings if p.account == 'Assets:Fixture']
    assert posting.units.number == Decimal('0.05')
    assert posting.units.currency == 'AUD'


@pytest.mark.parametrize('rejection', ['removed_input', 'invalid_fitids', 'invalid_parsed_file'])
def test_rejected_cache_reparses_remaining_input(tmp_path, rejection):
    first = write_statement(tmp_path / 'first.ofx')
    remaining = write_statement(tmp_path / 'remaining.ofx')
    cache = tmp_path / 'ofx.cache'
    import_statements(tmp_path, first, remaining, cache=cache)
    requested = (remaining,) if rejection == 'removed_input' else (first, remaining)
    if rejection != 'removed_input':
        with cache.open('rb') as cache_file:
            cached = pickle.load(cache_file)
        if rejection == 'invalid_fitids':
            cached['source_fitids'] = [[]]
        else:
            cached['parsed_files'].insert(1, None)
        with cache.open('wb') as cache_file:
            pickle.dump(cached, cache_file)

    source, entries, messages = import_statements(tmp_path, *requested, cache=cache)
    assert not messages
    assert len(source.parsed_files) == len(requested)
    assert len(source.source_fitids) == 1
    [transaction] = [entry for entry in entries if isinstance(entry, Transaction)]
    [posting] = [p for p in transaction.postings if p.account == 'Assets:Fixture']
    assert posting.units.number == Decimal('0.05')
    assert posting.units.currency == 'AUD'


def test_warm_cache_keeps_valid_overlapping_statement_after_invalid_currency(tmp_path):
    invalid = write_statement(tmp_path / 'invalid.ofx', '<CURDEF>')
    valid = write_statement(tmp_path / 'valid.ofx')
    cache = tmp_path / 'ofx.cache'
    first_source, first_entries, first_messages = import_statements(tmp_path, invalid, valid, cache=cache)
    source, entries, messages = import_statements(tmp_path, invalid, valid, cache=cache)
    assert len(first_source.source_fitids) == len(source.source_fitids) == 1
    assert entries == first_entries
    assert messages == first_messages
    [(severity, message, location)] = messages
    assert severity == 'error' and 'CURDEF' in message
    assert location['filename'] == str(invalid)
    [transaction] = [entry for entry in entries if isinstance(entry, Transaction)]
    [posting] = [p for p in transaction.postings if p.account == 'Assets:Fixture']
    assert posting.units.number == Decimal('0.05')
    assert posting.units.currency == 'AUD'
