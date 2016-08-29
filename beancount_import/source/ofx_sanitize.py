"""Attempts to remove identifying information from an OFX file.

This is useful for generating test data that may be publicly shared.

The result is pretty printed to make it easier to inspect.

WARNING: Inspect the output carefully before sharing publicly, as this tool is
not perfect and the result may need to be edited manually.

"""

from typing import FrozenSet, Set, List, Callable, Dict
import random
import re
import io

from beancount.core.number import D

def make_random_digits_replacement(x: str):
    return re.sub('[0-9]', lambda x: str(random.randint(0, 9)), x)


def make_random_ticker(x: str, retained_tickers: FrozenSet[str]):
    if x in retained_tickers:
        return x
    return ''.join(chr(ord('A') + random.randint(0, 25)) for _ in range(5))


def make_random_hex_replacement(x: str):
    if x == 'NONE':
        return x
    return re.sub('[0-9a-zA-Z]', lambda x: '%x' % random.randint(0, 15), x)


def make_random_replacement(x: str):
    return re.sub('[0-9a-zA-Z]', lambda x: '%x' % random.randint(0, 15), x)


def replace_tag_value(contents: str, tag: str,
                      replacement: Callable[[str], str]) -> str:
    return re.sub(
        r'(<' + tag + '>)([^<]*)',
        lambda m: m.group(1) + replacement(m.group(2)),
        contents,
        flags=re.IGNORECASE)


def get_balanced_tags(contents: str, tag_pattern=r'[a-zA-Z][a-zA-Z0-9]*'):
    pattern = r'<(' + tag_pattern + r')>((?:[^<]|<(?!/\1>))*)</\1>'
    while contents:
        m = re.search(pattern, contents, flags=re.IGNORECASE)
        if m is None:
            if contents:
                yield None, contents
            return
        prefix = contents[:m.start()]
        if prefix:
            yield None, prefix
        yield m.group(1), m.group(2)
        contents = contents[m.end():]

def replace_number(x: str) -> str:
    if not x.strip():
        return x
    m = re.match(r'^(\s*[-+]?\s*)([0-9]+)((?:\.(?:[0-9]*[1-9])?)?)(0*\s*)$', x)
    assert m is not None
    num_part = str(random.randint(0, 99))
    frac_part = make_random_digits_replacement(m.group(3))
    return '%s%s%s%s' % (m.group(1), num_part, frac_part, m.group(4))


def do_generic_number_replacements(contents: str) -> str:
    for tag in [
            'TOTAL',
            'UNITS',
            'UNITPRICE',
            'TRNAMT',
            'FEES',
            'COMMISSION',
    ]:
        contents = replace_tag_value(contents, tag, replace_number)
    return contents


def replace_transactions(contents: str) -> str:
    out = io.StringIO()
    for tag, inner in get_balanced_tags(contents, 'buymf|sellmf|reinvest|buystock|sellstock|buyopt|sellopt|transfer|income|invbanktran|stmttrn'):
        inner = do_generic_number_replacements(inner)
        if tag is None:
            # Do generic replacements
            out.write(inner)
        else:
            numbers = []
            units = get_tag_values(inner, 'UNITS')
            unitprice = get_tag_values(inner, 'UNITPRICE')
            if len(units) == 1 and len(unitprice) == 1:
                numbers.append(D(units[0]) * D(unitprice[0]))
            for inner_tag in ['FEES', 'COMMISSION']:
                for value in get_tag_values(inner, inner_tag):
                    if value:
                        numbers.append(D(value))
            if numbers:
                total_number = sum(numbers, D('0'))
                inner = replace_tag_value(inner, 'TOTAL', lambda _: str(-total_number))
            out.write('<' + tag + '>' + inner + '</' + tag + '>')
    return out.getvalue()

def replace_tag_contents(contents: str, tag: str,
                         replacement: Callable[[str], str]) -> str:
    return re.sub(
        r'(<' + tag + '>)((?:[^<]|<(?!/' + tag + r'>))*)(</' + tag + r'>)',
        lambda m: m.group(1) + replacement(m.group(2)) + m.group(3),
        contents,
        flags=re.IGNORECASE)


def get_tag_values(contents: str, tag: str) -> List[str]:
    values = []  # type: List[str]

    def replacer(x):
        values.append(x)
        return x

    replace_tag_value(contents, tag, replacer)
    return values


def pretty_print(contents: str) -> str:
    out = io.StringIO()

    def recurse(level: int, s: str):
        indent = '  ' * level
        for tag, inner in get_balanced_tags(s):
            inner = inner.lstrip()
            if tag is None:
                for line in inner.replace('<', '\n<').lstrip().split('\n'):
                    out.write(indent + line + '\n')
                continue
            out.write(indent + '<' + tag + '>\n')
            recurse(level + 1, inner)
            out.write(indent + '</' + tag + '>\n')

    recurse(0, contents)
    return out.getvalue()


def get_strings(contents: str) -> List[str]:
    return [
        x.strip() for x in re.split('\n|</?[a-zA-Z][a-zA-Z0-9]*>', contents)
    ]


class MemoizedReplacer(object):
    def __init__(self, replacer: Callable[[str], str]) -> None:
        self.replacer = replacer
        self.replacements = dict()  # type: Dict[str, str]

    def __call__(self, x: str) -> str:
        return self.replacements.setdefault(x, self.replacer(x))


def sanitize_ofx(input_path: str, output_path: str, account_id: str,
                 broker_id: str, org: str, retained_tickers: FrozenSet[str]):
    with open(input_path, 'r') as f:
        contents = f.read()
    orig_strings = set(get_strings(contents))

    contents = re.sub(
        r'^([A-Z]*FILEUID:)(.*)$',
        lambda m: m.group(1) + make_random_hex_replacement(m.group(2)),
        contents,
        flags=re.MULTILINE)
    contents = replace_tag_value(contents, 'SESSCOOKIE',
                                 make_random_replacement)
    contents = replace_tag_value(contents, 'CLTCOOKIE',
                                 make_random_replacement)
    contents = replace_tag_value(contents, 'TRNUID',
                                 make_random_hex_replacement)
    contents = replace_tag_value(contents, 'ORG', lambda _: org)
    contents = replace_tag_value(contents, 'ACCTID', lambda _: account_id)
    contents = replace_tag_value(contents, 'BROKERID', lambda _: broker_id)
    contents = replace_tag_value(contents, 'FITID', make_random_replacement)
    contents = replace_tag_value(
        contents, 'UNIQUEID',
        MemoizedReplacer(lambda x: make_random_replacement(x).upper()))
    contents = replace_tag_value(contents, 'FIID',
                                 make_random_digits_replacement)
    contents = replace_tag_value(contents, 'FID',
                                 make_random_digits_replacement)
    contents = replace_tag_value(
        contents, 'TICKER',
        MemoizedReplacer(lambda x: make_random_ticker(x, retained_tickers)))
    contents = replace_tag_value(contents, 'SECNAME', lambda _: 'Security')

    allowed_strings = set([
        'Y',
        'N',
        'CUSIP',
        'BUY',
        'DIV',
        'INTEREST',
        'LONG',
        'SHORT',
        'SELL',
        'SUCCESS',
        'DEBIT',
        'CASH',
        'MATCH',
        'PRETAX',
        'AFTERTAX',
        'PERCENT',
        '0',
        'DOLLAR',
        'OTHER',
        'OPENEND',
        'CLOSEEND',
        'OLDFILEUID:NONE',
        'Price as of date based on closing price',
    ])

    # Allow dates
    for tag in [
            'DTASOF', 'DTPRICEASOF', 'DTTRADE', 'DTSTART', 'DTEND', 'DTSERVER',
            'DTPOSTED', 'DTPROFUP',
    ]:
        allowed_strings.update(get_tag_values(contents, tag))

    # Allow currencies
    allowed_strings.update(get_tag_values(contents, 'CURDEF'))

    for tag in ['BALTYPE', 'LANGUAGE', 'SEVERITY', 'MESSAGE']:
        allowed_strings.update(get_tag_values(contents, tag))

    # Allow BAL DESC and NAME values
    for tag, inner in get_balanced_tags(contents, 'BAL'):
        if tag is None: continue
        allowed_strings.update(get_tag_values(inner, 'DESC'))
        allowed_strings.update(get_tag_values(inner, 'NAME'))

    contents = replace_transactions(contents)

    for tag in [
            'MKTVAL',
            'BUYPOWER',
            'VALUE',
            'AVAILCASH',
            'MARGINBALANCE',
            'SHORTBALANCE',
    ]:
        contents = replace_tag_value(contents, tag, replace_number)
    retained_strings = orig_strings & set(
        get_strings(contents)) - allowed_strings

    allowed_strings_pattern = '^(CHARSET|COMPRESSION|DATA|ENCODING|OFXHEADER|SECURITY|VERSION):.*|^$'
    retained_strings = {
        x
        for x in retained_strings
        if re.match(allowed_strings_pattern, x) is None
    }

    with open(output_path, 'w') as f:
        f.write(pretty_print(contents))

    print('RETAINED STRINGS:\n')
    for s in sorted(retained_strings):
        print(s)


def main():
    import argparse
    ap = argparse.ArgumentParser()
    ap = argparse.ArgumentParser()
    ap.add_argument('input')
    ap.add_argument('output')
    ap.add_argument('--account-id', default='123456789')
    ap.add_argument('--broker-id', default='MyBank')
    ap.add_argument('--retained-tickers', nargs='*', default=[])
    ap.add_argument('--org', default='MyBank')
    args = ap.parse_args()
    sanitize_ofx(
        args.input,
        args.output,
        account_id=args.account_id,
        broker_id=args.broker_id,
        org=args.org,
        retained_tickers=frozenset(args.retained_tickers),
    )


if __name__ == '__main__':
    main()
