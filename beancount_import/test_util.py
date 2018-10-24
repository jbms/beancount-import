from typing import List, Optional, Union, Tuple, Dict
import os

import pytest
import py

import beancount.parser.parser
import beancount.parser.printer
from beancount.core.data import Directive, Entries, Posting, Transaction, Meta


def parse(text: str) -> Entries:
    entries, errors, options = beancount.parser.parser.parse_string(
        text, dedent=True)
    assert errors == []
    return entries


def format_entries(entries: Entries, indent=0):
    result = '\n\n'.join(
        beancount.parser.printer.format_entry(e) for e in entries)
    indent_str = ' ' * indent
    return '\n'.join(indent_str + x.rstrip() for x in result.split('\n'))


def normalize_metadata(meta: Optional[Meta]) -> Optional[Meta]:
    if meta is None:
        return None
    meta = dict(meta)
    meta.pop('filename', None)
    meta.pop('lineno', None)
    if not meta:
        meta = None
    return meta


def normalize_entry(entry: Union[Directive, Posting]) -> Directive:
    entry = entry._replace(meta=normalize_metadata(entry.meta))
    if isinstance(entry, Transaction):
        entry = entry._replace(
            postings=[normalize_entry(p) for p in entry.postings])
    return entry


def check_golden_contents(path: str,
                          expected_contents: str,
                          replacements: List[Tuple[str, str]] = [],
                          write: Optional[bool] = None) -> None:
    if write is None:
        write = os.getenv('BEANCOUNT_IMPORT_GENERATE_GOLDEN_TESTDATA',
                          None) == '1'

    for old, new in replacements:
        expected_contents = expected_contents.replace(old, new)
    if write:
        dir_name = os.path.dirname(path)
        if not os.path.exists(dir_name):
            os.makedirs(dir_name)
        with open(path, 'w') as f:
            f.write(expected_contents)
    else:
        with open(path, 'r') as f:
            contents = f.read()
        assert contents == expected_contents
