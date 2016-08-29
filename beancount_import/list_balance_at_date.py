import argparse
import beancount.loader
import dateutil.parser
from beancount.core.number import D
from beancount.core.data import Open, Transaction, Balance, Commodity
from beancount.core.inventory import Inventory
from beancount.core.position import get_position, Position, Cost
from beancount.core.convert import get_units, get_cost
from beancount.core.amount import Amount


def ignore_errors(_):
    pass


class BalanceLister(object):
    def __init__(self, journal_path):

        self.entries, self.errors, self.options_map = beancount.loader.load_file(
            journal_path, log_errors=ignore_errors)

    def get_inventory(self, account, date):
        inventory = Inventory()
        for entry in self.entries:
            if date is not None and entry.date > date:
                break
            if not isinstance(entry, Transaction):
                continue
            for posting in entry.postings:
                if posting.account != account:
                    continue
                inventory.add_position(get_position(posting))
        return inventory

def get_digits(x):
    s = str(x)
    k =  s.find('.')
    if k == -1:
        return 0
    return len(s) - k - 1

if __name__ == '__main__':
    ap = argparse.ArgumentParser()
    ap.add_argument('journal')
    ap.add_argument('account')
    ap.add_argument('-d' ,'--date', type=lambda x: dateutil.parser.parse(x).date(), help='End date')
    ap.add_argument('--transfer-to')

    ap.add_argument('--convert-to')
    ap.add_argument('--new-quantity', type=D)
    ap.add_argument('--new-to-old-ratio', type=D)
    args = ap.parse_args()
    balance_lister = BalanceLister(args.journal)
    inventory = balance_lister.get_inventory(args.account, args.date)
    total = inventory.reduce(get_units)
    print('Original units: ', total)
    print('Original cost:', inventory.reduce(get_cost))

    if args.convert_to:
        new_currency = args.convert_to
        if len(total) != 1:
            raise ValueError
        if args.new_quantity:
            old_quantity = total[0].units.number
            new_to_old_ratio = args.new_quantity / old_quantity
        else:
            new_to_old_ratio = args.new_to_old_ratio

        new_inventory = Inventory()
        do_round = True
        for position in inventory:
            # units_digits = get_digits(position.units.number)
            # cost_digits = get_digits(position.cost.number)
            units_digits = 4
            cost_digits = 4
            new_units_number = position.units.number * new_to_old_ratio
            new_cost_number = position.cost.number / new_to_old_ratio
            if do_round:
                new_units_number = round(new_units_number, units_digits)
                new_cost_number = round(new_cost_number, cost_digits)
            new_position = Position(
                units=Amount(new_units_number, new_currency),
                cost=Cost(
                    number=new_cost_number,
                    currency=position.cost.currency,
                    date=position.cost.date,
                    label=position.cost.label))
            new_inventory.add_position(new_position)
            print('  %s %s' % (args.transfer_to, new_position))
            print('  %s %s' % (args.account, -position))
        print('New units: ', new_inventory.reduce(get_units))
        print('New cost: ', new_inventory.reduce(get_cost))
    elif args.transfer_to:
        for position in inventory:
            print('  %s %s' % (args.transfer_to, position))
            print('  %s %s' % (args.account, -position))
    else:
        for position in inventory:
            print('  %s %s' % (args.account, position))
