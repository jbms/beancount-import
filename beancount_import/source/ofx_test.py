import os

import bs4
import pytest
from beancount.core.amount import Amount
from beancount.core.number import D

from ..journal_editor import JournalEditor
from . import SourceResults, ofx
from .source_test import check_source_example

testdata_dir = os.path.realpath(
    os.path.join(
        os.path.dirname(__file__), '..', '..', 'testdata', 'source', 'ofx'))

examples = [
    ('test_vanguard_basic', 'vanguard.ofx'),
    ('test_vanguard_matching', 'vanguard.ofx'),
    ('test_vanguard_invalid', 'vanguard.ofx'),
    ('test_vanguard_with_cash_account', 'vanguard.ofx'),
    ('test_vanguard_with_cash_account_matching_missing_transfer',
     'vanguard.ofx'),
    ('test_vanguard_with_cash_account_matching_missing_primary',
     'vanguard.ofx'),
    ('test_vanguard401k', 'vanguard401k.ofx'),
    ('test_vanguard_401k_matching', 'vanguard401k.ofx'),
    ('test_vanguard_xfer_in', 'vanguard_xfer_in.ofx'),
    ('test_fidelity_savings', 'fidelity-savings.ofx'),
    ('test_fidelity_treasury', 'fidelity_treasury.ofx'),
    ('test_suncorp', 'suncorp.ofx'),
    ('test_checking', 'checking.ofx'),
    ('test_checking_emptyledgerbal', 'checking-emptyledgerbal.ofx'),
    ('test_td_ameritrade', 'td_ameritrade.ofx'),
    ('test_anzcc', 'anzcc.ofx'),
    ('test_multiple_accounts', 'multiple_accounts.ofx'),
    ('test_bank_medium', 'bank_medium.ofx'),
    ('test_investment_401k', 'investment_401k.ofx'),
    ('test_investment_buy_sell_income', 'investment_buy_sell_income.ofx'),
    ('test_vanguard_roth_ira', 'vanguard_roth_ira.ofx'),
    ('test_vanguard_roth_ira_matching', 'vanguard_roth_ira.ofx'),
    ('test_checking2', 'checking2.ofx'),
    ('test_checking2_matching', 'checking2.ofx'),
    ('test_amex', 'amex.ofx'),
    ('test_fidelity', 'fidelity.ofx'),
    ('test_fidelity_ira', 'fidelity-ira.ofx'),
    ('test_non_default_capital_gains', 'vanguard401k.ofx'),
]


@pytest.mark.parametrize('name,ofx_filename', examples)
def test_source(name: str, ofx_filename: str):
    check_source_example(
        example_dir=os.path.join(testdata_dir, name),
        source_spec={
            'module': 'beancount_import.source.ofx',
            'ofx_filenames': [os.path.join(testdata_dir, ofx_filename)],
        },
        replacements=[(testdata_dir, '<testdata>')])

def test_find_ofx_id_for_account():
    ofx_ids = {
        'Assets:Vanguard:401k': 1,
    }
    for (account, want) in [
        ('Assets:Vanguard:401k:PreTax:VGI1', 1),
        ('Assets:Vanguard:401k:PreTax', 1),
        ('Assets:Vanguard:401k:VG1', 1),
        ('Assets:Vanguard:401k', 1),
        ('Assets:Vanguard:Unknown', None),
        ('Assets:Vanguard:401k:PreTax:Excessive:VGI1', None),
    ]:
        assert ofx.find_ofx_id_for_account(account, ofx_ids) == want, account


_OFX_HEADER = (b'OFXHEADER:100\r\nDATA:OFXSGML\r\nVERSION:102\r\n'
               b'SECURITY:NONE\r\nENCODING:USASCII\r\nCHARSET:1252\r\n'
               b'COMPRESSION:NONE\r\nOLDFILEUID:NONE\r\nNEWFILEUID:NONE\r\n\r\n')


def test_decode_ofx_zero_balances_are_not_utf7():
    # Several signed zero amounts made BeautifulSoup's encoding detection
    # pick UTF-7, garbling every value.
    contents = _OFX_HEADER + (
        b'<OFX><INVPOS><MKTVAL>+00000000000.00<DTPRICEASOF>20200131</INVPOS>'
        b'<INVBAL><AVAILCASH>+00000000000.00<MARGINBALANCE>+00000000000.00'
        b'<SHORTBALANCE>+00000000000.00<BUYPOWER>+00000000000.00<BALLIST>'
        b'</INVBAL></OFX>')
    soup = bs4.BeautifulSoup(ofx.decode_ofx(contents), 'html.parser')
    assert ofx.find_child(soup, 'mktval') == '+00000000000.00'
    assert ofx.find_child(soup, 'buypower') == '+00000000000.00'


def test_decode_ofx_windows_1252():
    assert ofx.decode_ofx(b'<NAME>Caf\xe9 Rouge') == '<NAME>Caf\xe9 Rouge'
    assert ofx.decode_ofx('<NAME>Caf\xe9'.encode('utf-8')) == '<NAME>Caf\xe9'


@pytest.mark.parametrize('second_amount', ['-100.0', '-3.25'])
def test_same_fitid_bank_transactions(tmp_path, second_amount):
    statement = tmp_path / 'card.ofx'
    statement.write_text(
        '<OFX><ORG>TEST<CCSTMTRS><CURDEF>AUD<ACCTID>TEST-CARD'
        '<BANKTRANLIST>'
        '<STMTTRN><TRNTYPE>DEBIT<DTPOSTED>20260630<TRNAMT>-100.00'
        '<FITID>test-1<NAME>Purchase</STMTTRN>\n'
        f'<STMTTRN><TRNTYPE>DEBIT<DTPOSTED>20260630<TRNAMT>{second_amount}'
        '<FITID>test-1<NAME>Purchase</STMTTRN>'
        '</BANKTRANLIST></CCSTMTRS></OFX>', encoding='ascii')
    journal_path = tmp_path / 'journal.beancount'
    journal_path.write_text(
        '2026-01-01 open Liabilities:TestCard AUD\n'
        '  ofx_org: "TEST"\n'
        '  ofx_broker_id: ""\n'
        '  account_id: "TEST-CARD"\n'
        '  ofx_account_type: "cash_only"\n', encoding='ascii')
    journal = JournalEditor(str(journal_path))
    assert not journal.errors
    cache = tmp_path / 'ofx.cache'

    for cached in (False, True):
        status = []
        source = ofx.OfxSource([str(statement)], log_status=status.append,
                               cache_filename=str(cache))
        assert any(message.startswith('ofx: loading') for message in status) != cached
        results = SourceResults()
        source.prepare(journal, results)
        assert len(results.pending) == 1
        [transaction] = results.pending[0].entries
        [card_posting] = [posting for posting in transaction.postings
                          if posting.account == 'Liabilities:TestCard']
        assert card_posting.units == Amount(D('-100.00'), 'AUD')
        if second_amount == '-100.0':
            assert not results.messages
        else:
            assert len(results.messages) == 1
            [(severity, message, location)] = results.messages
            assert severity == 'error'
            assert 'test-1' in message
            assert '-100.00 AUD' in message and '-3.25 AUD' in message
            assert location == {'filename': str(statement), 'lineno': 2}
