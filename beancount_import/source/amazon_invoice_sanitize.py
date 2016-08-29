"""Strips identifying information from an Amazon.com order details HTML file.

This is useful for creating test files.

WARNING: This may not always strip all identifying information.  You should
always manually inspect the output.
"""

from typing import Optional, Dict, Tuple
import os
import random
import re

import bs4

order_id_re = '[0-9]+(-[0-9]+)+'


def make_random_number_replacement(x: str):
    return re.sub('[0-9]', lambda x: str(random.randint(0, 9)), x)


def sanitize_order_ids(contents: str,
                       replacements: Optional[Dict[str, str]] = None
                       ) -> Tuple[str, Dict[str, str]]:
    if replacements is None:
        reps = dict()  # type: Dict[str, str]
    else:
        reps = replacements

    def get_replacement(m):
        if m.group(0) in reps:
            return reps[m.group(0)]
        r = make_random_number_replacement(m.group(0))
        reps[m.group(0)] = r
        return r

    return re.sub(order_id_re, get_replacement, contents), reps


def sanitize_other_ids(contents: str):
    pattern = '[0-9A-Z][0-9A-Z][0-9A-Z][0-9A-Z][0-9A-Z]+'

    def get_replacement(m):
        s = m.group(0)
        if re.search('[A-Z]', s) and re.search('[0-9]', s):
            return 'X' * len(m.group(0))
        return s

    return re.sub(pattern, get_replacement, contents)


def sanitize_credit_card(contents: str, new_digits: str):
    contents = re.sub(r'(ending in\s+)[0-9]{4}',
                      lambda m: m.group(1) + new_digits, contents)
    contents = re.sub(r'(Last (?:[a-zA-Z0-9\s]*)digits:\s*)[0-9]{4}',
                      lambda m: m.group(1) + new_digits, contents)
    return contents


def sanitize_address(contents: str):
    return re.sub(
        '^.*address.*$', '', contents, flags=re.IGNORECASE | re.MULTILINE)


def remove_tag(soup: bs4.BeautifulSoup, tag: str):
    for x in soup.find_all(tag):
        x.extract()


def sanitize_invoice(input_path: str, output_path: str,
                     credit_card_digits: str):
    with open(input_path, 'r') as f:
        soup = bs4.BeautifulSoup(f.read(), 'lxml')
    comments = soup.find_all(text=lambda text: isinstance(text, bs4.Comment))
    remove_tag(soup, 'script')
    remove_tag(soup, 'style')
    remove_tag(soup, 'link')
    remove_tag(soup, 'noscript')
    remove_tag(soup, 'img')
    remove_tag(soup, 'input')
    for x in soup.find_all('a'):
        if 'href' in x.attrs and '/dp/' not in x.attrs['href']:
            del x['href']
    for x in comments:
        x.extract()

    new_output, order_id_replacements = sanitize_order_ids(str(soup))
    # new_output = sanitize_other_ids(new_output)
    new_output = sanitize_credit_card(new_output, credit_card_digits)
    new_output = sanitize_address(new_output)
    if os.path.isdir(output_path):
        output_name, _ = sanitize_order_ids(
            os.path.basename(input_path), order_id_replacements)
        output_path = os.path.join(output_path, output_name)
    with open(output_path, 'w') as f:
        f.write(new_output)


def main():
    import argparse
    ap = argparse.ArgumentParser()
    ap.add_argument('invoice')
    ap.add_argument('output')
    ap.add_argument('--credit-card-digits', default='1234')
    args = ap.parse_args()
    sanitize_invoice(
        args.invoice, args.output, credit_card_digits=args.credit_card_digits)


if __name__ == '__main__':
    main()
