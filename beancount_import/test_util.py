from typing import List, Optional, Union, Tuple, Dict
import json
import re
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
    """Check that the contents of the file at `path` matches `expected_contents`.

    Before comparing the contents of the file, the replacements indicated by the
    The `replacements` parameter specifies a sequence of `(old, new)` used to
    normalize `expected_contents`.  Any line or quoted string or doubly-quoted
    string that starts with one of the `old` values is assumed to indicate a
    filename: the `old` prefix is replaced with `new`, and any backslashes in
    the remainder of the filename are replaced with forward slashes.

    If `write == True`, instead of matching the existing contents of `path`, the
    value of `expected_contents` after applying `replacements` is written to
    `path`.

    """
    if write is None:
        write = os.getenv('BEANCOUNT_IMPORT_GENERATE_GOLDEN_TESTDATA',
                          None) == '1'

    for old, new in replacements:
        # Replace filenames specified as doubly-quoted strings.
        doubly_quoted_pattern = ('\\\\"' + re.escape(
            json.dumps(json.dumps(old)[1:-1])[1:-1]) + '[^"]*\\\\"')

        def get_doubly_quoted_replacement(m) -> str:
            suffix = json.loads(json.loads('"' + m.group(0) + '"'))[len(old):]
            return json.dumps(json.dumps(new + suffix.replace('\\', '/')))[1:-1]

        expected_contents = re.sub(doubly_quoted_pattern,
                                   get_doubly_quoted_replacement,
                                   expected_contents)

        # Replace filenames specified as quoted strings.
        quoted_pattern = '"' + re.escape(json.dumps(old)[1:-1]) + '[^"]*"'

        def get_quoted_replacement(m) -> str:
            suffix = json.loads(m.group(0))[len(old):]
            return json.dumps(new + suffix.replace('\\', '/'))

        expected_contents = re.sub(quoted_pattern, get_quoted_replacement,
                                   expected_contents)

        # Rpelace filenames specified as single lines.
        unquoted_pattern = '^' + re.escape(old) + '(.*)$'

        def get_unquoted_replacement(m) -> str:
            suffix = m.group(1)
            return new + suffix.replace('\\', '/')

        expected_contents = re.sub(
            unquoted_pattern,
            get_unquoted_replacement,
            expected_contents,
            flags=re.MULTILINE)

    if write:
        dir_name = os.path.dirname(path)
        if not os.path.exists(dir_name):
            os.makedirs(dir_name)
        with open(path, 'w', encoding='utf-8', newline='\n') as f:
            f.write(expected_contents)
    else:
        with open(path, 'r', encoding='utf-8', newline='\n') as f:
            contents = f.read()
        assert contents == expected_contents
