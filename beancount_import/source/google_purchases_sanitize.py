"""Strips identifying information from a Google Purchases JSON file.

This is useful for creating test files.
"""

import os
import re
import random
import json


def make_random_number_replacement(x: str):
    return re.sub('[0-9]', lambda x: str(random.randint(0, 9)), x)


def sanitize(input_path: str, output_directory: str):
    with open(input_path, 'r') as f:
        data = json.load(f)
    purchase_id = data['id'] = make_random_number_replacement(data['id'])
    with open(os.path.join(output_directory, purchase_id + '.json'), 'w') as f:
        f.write(json.dumps(data, indent='  '))


def main():
    import argparse
    ap = argparse.ArgumentParser()
    ap.add_argument('input', help='Input JSON file.')
    ap.add_argument('output_directory', help='Output directory.')
    args = ap.parse_args()
    sanitize(args.input, args.output_directory)

if __name__ == '__main__':
    main()
