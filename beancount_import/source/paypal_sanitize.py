"""Strips identifying information from a Paypal transaction JSON file.

This is useful for creating test files.

WARNING: This may not always strip all identifying information.  You should
always manually inspect the output.

Currently, this only replaces the transaction id.
"""

import os
import random


def sanitize(input_path: str, output_directory: str):
    txn_id = os.path.splitext(os.path.basename(input_path))[0]
    with open(input_path, 'r') as f:
        content = f.read()
    base_36_chars = '0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ'
    new_txn_id = ''.join(base_36_chars[random.randint(0, 35)] for _ in txn_id)
    content = content.replace(txn_id, new_txn_id)
    output_path = os.path.join(output_directory, new_txn_id + '.json')
    with open(output_path, 'w') as f:
        f.write(content)
    print('Wrote: %s' % output_path)


def main():
    import argparse
    ap = argparse.ArgumentParser()
    ap.add_argument('input')
    ap.add_argument('output_directory')
    args = ap.parse_args()
    sanitize(
        args.input,
        args.output_directory,
    )


if __name__ == '__main__':
    main()
