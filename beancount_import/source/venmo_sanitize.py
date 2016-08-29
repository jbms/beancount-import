import csv
from typing import Dict
import io
import random
import sys


def _sanitize_id(x: str) -> str:
    return ''.join(str(random.randint(0, 9)) for _ in range(len(x)))


first_names = [
    'Tom', 'Mary', 'Patricia', 'Linda', 'John', 'Brian', 'Jim', 'Nick', 'Sally',
    'Cindy', 'Kelly', 'Kim', 'Maria'
]
last_names = [
    'Smith', 'Johnson', 'Williams', 'Jones', 'Brown', 'Davis', 'Miller',
    'Wilson', 'Moore', 'Taylor', 'Anderson', 'Thomas', 'Jackson', 'White',
    'Harris', 'Martin', 'Thompson'
]


def _make_random_name():
    return random.choice(first_names) + ' ' + random.choice(last_names)


def _make_random_account():
    return 'Visa Debit *' + ''.join(str(random.randint(0, 9)) for _ in range(4))


class EntrySanitizer(object):
    def __init__(self):
        self._name_map = {'': ''}  # Dict[str, str]
        self._account_map = {
            '': '',
            'Venmo balance': 'Venmo balance'
        }  # Dict[str, str]

    def _sanitize_name(self, name: str) -> str:
        return self._name_map.setdefault(name, _make_random_name())

    def _sanitize_account(self, x: str) -> str:
        return self._account_map.setdefault(x, _make_random_account())

    def sanitize(self, row: Dict[str, str]) -> Dict[str, str]:
        new_row = row.copy()
        new_row[' ID'] = _sanitize_id(row[' ID'])
        for key in ['From', 'To']:
            new_row[key] = self._sanitize_name(row[key])
        for key in ['Funding Source', 'Destination']:
            new_row[key] = self._sanitize_account(row[key])
        return new_row


def sanitize_transactions_data(contents: str) -> str:
    reader = csv.DictReader(io.StringIO(contents))
    entry_santizer = EntrySanitizer()

    output = io.StringIO()
    writer = csv.DictWriter(output, reader.fieldnames, dialect='unix', quoting=csv.QUOTE_ALL)
    writer.writeheader()
    for line in reader:
        writer.writerow(entry_santizer.sanitize(line))
    return output.getvalue()


if __name__ == '__main__':
    print(sanitize_transactions_data(sys.stdin.read()))
