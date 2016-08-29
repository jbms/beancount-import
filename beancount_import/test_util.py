from typing import List, Optional, Union, Tuple, Dict

import pytest
import py

import beancount.parser.parser
import beancount.parser.printer
from beancount.core.data import ALL_DIRECTIVES, Posting, Transaction, Meta

Directive = Union[ALL_DIRECTIVES]
Entries = List[Directive]


def parse(text: str) -> Entries:
    entries, errors, options = beancount.parser.parser.parse_string(
        text, dedent=True)
    assert errors == []
    return entries


def format_entries(entries: Entries, indent=0):
    result = '\n\n'.join(
        beancount.parser.printer.format_entry(e) for e in entries)
    if indent:
        indent_str = ' ' * indent
        result = '\n'.join(indent_str + x for x in result.split('\n'))
    return result


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
