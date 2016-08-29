import os

from .amazon_invoice_test import testdata_dir
from .source_test import check_source, import_result
from ..training import PredictionInput
import datetime
from beancount.core.amount import Amount
from beancount.core.number import D


def test_basic(tmpdir):
    check_source(
        tmpdir,
        source_spec={
            'module': 'beancount_import.source.amazon',
            'directory': testdata_dir,
            'amazon_account': 'name@domain.com',
            'posttax_adjustment_accounts': {
                'Gift Card Amount': 'Assets:Gift-Cards:Amazon',
                'Rewards Points': 'Income:Amazon:Cashback',
            },
        },
        pending=[
            import_result(
                info={
                    'type':
                    'text/html',
                    'filename':
                    os.path.join(testdata_dir, '166-7926740-5141621.html'),
                },
                entries=r"""
                2016-02-07 * "Amazon.com" "Order"
                  amazon_account: "name@domain.com"
                  amazon_order_id: "166-7926740-5141621"
                  Expenses:FIXME:A   11.87 USD
                    amazon_item_condition: "New"
                    amazon_item_description: "Casio Men's W916-8AV Alarm Chronograph Watch, classic"
                    amazon_item_quantity: 1
                    amazon_seller: "Amazon.com LLC"
                    shipped_date: 2016-02-08
                  Expenses:FIXME:A    1.13 USD
                    amazon_invoice_description: "Sales Tax"
                  Income:Amazon:Cashback     -1.06 USD
                    amazon_posttax_adjustment: "Rewards Points"
                  Expenses:FIXME    -11.94 USD
                    amazon_credit_card_description: "Amazon.com Visa Signature ending in 1234"
                    transaction_date: 2016-02-08
                
            """,
            ),
            import_result(
                info={
                    'type':
                    'text/html',
                    'filename':
                    os.path.join(testdata_dir, 'D56-5204779-4181560.html')
                },
                entries=r"""
                2016-06-30 * "Amazon.com" "Order"
                  amazon_account: "name@domain.com"
                  amazon_order_id: "D56-5204779-4181560"
                  Expenses:FIXME:A   14.99 USD
                    amazon_item_by: "Robert Zubrin, Arthur C. Clarke, Richard Wagner"
                    amazon_item_description: "Case for Mars"
                    amazon_item_url: "https://www.amazon.com/dp/B004G8QU6U/ref=docs-os-doi_0"
                    amazon_seller: "Simon and Schuster Digital Sales Inc"
                  Expenses:FIXME    -14.99 USD
                    amazon_credit_card_description: "VISA ending in 1234"
                    transaction_date: 2016-06-30
                
                """,
            ),
            import_result(
                info={
                    'type':
                    'text/html',
                    'filename':
                    os.path.join(testdata_dir, '781-8429198-6057878.html'),
                },
                entries=r"""
                2016-08-30 * "Amazon.com" "Order"
                  amazon_account: "name@domain.com"
                  amazon_order_id: "781-8429198-6057878"
                  Expenses:FIXME:A   14.99 USD
                    amazon_item_condition: "New"
                    amazon_item_description: "Car Mount, Anker CD Slot Universal Phone Holder for iPhone 6s/6/6s plus/6 plus, Samsung S7/S6/edge, Samsung Note 7/5, LG G5, Nexus 5X/6/6P, Moto, HTC,"
                    amazon_item_quantity: 1
                    amazon_seller: "AnkerDirect"
                    shipped_date: 2016-08-30
                  Expenses:FIXME:A    5.99 USD
                    amazon_invoice_description: "Shipping & Handling"
                  Expenses:FIXME:A    1.99 USD
                    amazon_invoice_description: "Sales Tax"
                  Expenses:FIXME    -22.97 USD
                    amazon_credit_card_description: "Amazon.com Visa Signature ending in 1234"
                    transaction_date: 2016-08-30
                
            """,
            ),
            import_result(
                info={
                    'type':
                    'text/html',
                    'filename':
                    os.path.join(testdata_dir, '277-5312419-9119541.html'),
                },
                entries=r"""
                2017-03-23 * "Amazon.com" "Order"
                  amazon_account: "name@domain.com"
                  amazon_order_id: "277-5312419-9119541"
                  Expenses:FIXME:A   10.80 USD
                    amazon_item_condition: "New"
                    amazon_item_description: "C.R. Gibson 9-Count Coloring File Folders, 3 of Each Design, 10 Adhesive Labels, Measures 11.5 x 9.5 - Gold"
                    amazon_item_quantity: 1
                    amazon_seller: "Amazon.com LLC"
                    shipped_date: 2017-03-24
                  Expenses:FIXME:A   12.60 USD
                    amazon_item_condition: "New"
                    amazon_item_description: "MUJI Gel Ink Ballpoint Pens 0.38mm 9-colors Pack"
                    amazon_item_quantity: 1
                    amazon_seller: "hidamarifarm"
                    shipped_date: 2017-03-24
                  Expenses:FIXME:A   16.50 USD
                    amazon_item_condition: "New"
                    amazon_item_description: "Cynthia Rowley File Folders, 3 Tab, 6 Folders per package, Leaves Assorted Patterns"
                    amazon_item_quantity: 1
                    amazon_seller: "Sherry Pappas"
                    shipped_date: 2017-03-24
                  Expenses:FIXME:A    8.99 USD
                    amazon_item_condition: "New"
                    amazon_item_description: "V&A William Morris Garden File Folder, Galison"
                    amazon_item_quantity: 1
                    amazon_seller: "Amazon.com LLC"
                    shipped_date: 2017-03-24
                  Expenses:FIXME:A   23.95 USD
                    amazon_item_condition: "New"
                    amazon_item_description: "Bloom Daily Planners All In One Planner, Calendar, Notebook, To-Do List Book, Sketch Book, Coloring Book and More! 9 x 11 Do More of What Makes You Happy"
                    amazon_item_quantity: 1
                    amazon_seller: "bloom daily planners"
                    shipped_date: 2017-03-24
                  Expenses:FIXME:A    8.99 USD
                    amazon_item_condition: "New"
                    amazon_item_description: "Skydue Floral Printed Accordion Document File Folder Expanding Letter Organizer (Pink)"
                    amazon_item_quantity: 1
                    amazon_seller: "Skydue"
                    shipped_date: 2017-03-24
                  Expenses:FIXME:A    1.83 USD
                    amazon_invoice_description: "Sales Tax"
                  Expenses:FIXME:B    7.99 USD
                    amazon_item_condition: "New"
                    amazon_item_description: "Skydue Letter A4 Paper Expanding File Folder Pockets Accordion Document Organizer (Jade)"
                    amazon_item_quantity: 1
                    amazon_seller: "Skydue"
                    shipped_date: 2017-03-27
                  Expenses:FIXME:C   14.84 USD
                    amazon_item_condition: "New"
                    amazon_item_description: "Design Ideas 8758616-DI 8758616-DI Cabo LetterHolder-Copper,Copper,"
                    amazon_item_quantity: 1
                    amazon_seller: "Quidsi Retail LLC"
                    shipped_date: 2017-03-24
                  Expenses:FIXME:C    1.37 USD
                    amazon_invoice_description: "Sales Tax"
                  Expenses:FIXME:D   17.95 USD
                    amazon_item_condition: "New"
                    amazon_item_description: "Cynthia Rowley File Folders, 3 Tab, 6 folders per package, Gold Assorted Patterns"
                    amazon_item_quantity: 1
                    amazon_seller: "Cailler's LLC"
                    shipped_date: 2017-03-25
                  Expenses:FIXME:D   14.99 USD
                    amazon_item_condition: "New"
                    amazon_item_description: "Suncatchers Colorful Bird Stained Glass Effect Resin Mobile - Beautiful Window Hanging - Home Decoration"
                    amazon_item_quantity: 1
                    amazon_seller: "That Internet Shop USA"
                    shipped_date: 2017-03-25
                  Expenses:FIXME:D    1.39 USD
                    amazon_invoice_description: "Sales Tax"
                  Expenses:FIXME:E   12.00 USD
                    amazon_item_condition: "New"
                    amazon_item_description: "Rifle Paper Co. Jardin Weekly Desk Planner Notepad"
                    amazon_item_quantity: 1
                    amazon_seller: "Our Pampered Home"
                    shipped_date: 2017-03-24
                  Expenses:FIXME    -71.06 USD
                    amazon_credit_card_description: "Amazon.com Store Card ending in 1234"
                    transaction_date: 2017-03-25
                  Expenses:FIXME    -16.21 USD
                    amazon_credit_card_description: "Amazon.com Store Card ending in 1234"
                    transaction_date: 2017-03-25
                  Expenses:FIXME    -12.60 USD
                    amazon_credit_card_description: "Amazon.com Store Card ending in 1234"
                    transaction_date: 2017-03-27
                  Expenses:FIXME    -34.33 USD
                    amazon_credit_card_description: "Amazon.com Store Card ending in 1234"
                    transaction_date: 2017-03-27
                  Expenses:FIXME    -12.00 USD
                    amazon_credit_card_description: "Amazon.com Store Card ending in 1234"
                    transaction_date: 2017-03-27
                  Expenses:FIXME     -7.99 USD
                    amazon_credit_card_description: "Amazon.com Store Card ending in 1234"
                    transaction_date: 2017-03-28
                
            """,
            ),
        ],
        training_examples=[])


def test_credit_card_transactions(tmpdir):
    check_source(
        tmpdir,
        source_spec={
            'module': 'beancount_import.source.amazon',
            'directory': testdata_dir,
            'amazon_account': 'name@domain.com',
            'posttax_adjustment_accounts': {
                'Gift Card Amount': 'Assets:Gift-Cards:Amazon',
                'Rewards Points': 'Income:Amazon:Cashback',
            },
        },
        journal_contents=r"""
        1900-01-01 open Liabilities:Credit-Card
          credit_card_last_4_digits: "1234"
        """,
        pending=[
            import_result(
                info={
                    'type':
                    'text/html',
                    'filename':
                    os.path.join(testdata_dir, '166-7926740-5141621.html'),
                },
                entries=r"""
                2016-02-07 * "Amazon.com" "Order"
                  amazon_account: "name@domain.com"
                  amazon_order_id: "166-7926740-5141621"
                  Expenses:FIXME:A   11.87 USD
                    amazon_item_condition: "New"
                    amazon_item_description: "Casio Men's W916-8AV Alarm Chronograph Watch, classic"
                    amazon_item_quantity: 1
                    amazon_seller: "Amazon.com LLC"
                    shipped_date: 2016-02-08
                  Expenses:FIXME:A    1.13 USD
                    amazon_invoice_description: "Sales Tax"
                  Income:Amazon:Cashback     -1.06 USD
                    amazon_posttax_adjustment: "Rewards Points"
                  Liabilities:Credit-Card    -11.94 USD
                    amazon_credit_card_description: "Amazon.com Visa Signature ending in 1234"
                    transaction_date: 2016-02-08
                
                """,
            ),
            import_result(
                info={
                    'type':
                    'text/html',
                    'filename':
                    os.path.join(testdata_dir, 'D56-5204779-4181560.html')
                },
                entries=r"""
                2016-06-30 * "Amazon.com" "Order"
                  amazon_account: "name@domain.com"
                  amazon_order_id: "D56-5204779-4181560"
                  Expenses:FIXME:A   14.99 USD
                    amazon_item_by: "Robert Zubrin, Arthur C. Clarke, Richard Wagner"
                    amazon_item_description: "Case for Mars"
                    amazon_item_url: "https://www.amazon.com/dp/B004G8QU6U/ref=docs-os-doi_0"
                    amazon_seller: "Simon and Schuster Digital Sales Inc"
                  Liabilities:Credit-Card    -14.99 USD
                    amazon_credit_card_description: "VISA ending in 1234"
                    transaction_date: 2016-06-30
                
                """,
            ),
            import_result(
                info={
                    'type':
                    'text/html',
                    'filename':
                    os.path.join(testdata_dir, '781-8429198-6057878.html'),
                },
                entries=r"""
                2016-08-30 * "Amazon.com" "Order"
                  amazon_account: "name@domain.com"
                  amazon_order_id: "781-8429198-6057878"
                  Expenses:FIXME:A   14.99 USD
                    amazon_item_condition: "New"
                    amazon_item_description: "Car Mount, Anker CD Slot Universal Phone Holder for iPhone 6s/6/6s plus/6 plus, Samsung S7/S6/edge, Samsung Note 7/5, LG G5, Nexus 5X/6/6P, Moto, HTC,"
                    amazon_item_quantity: 1
                    amazon_seller: "AnkerDirect"
                    shipped_date: 2016-08-30
                  Expenses:FIXME:A    5.99 USD
                    amazon_invoice_description: "Shipping & Handling"
                  Expenses:FIXME:A    1.99 USD
                    amazon_invoice_description: "Sales Tax"
                  Liabilities:Credit-Card    -22.97 USD
                    amazon_credit_card_description: "Amazon.com Visa Signature ending in 1234"
                    transaction_date: 2016-08-30
                
            """,
            ),
            import_result(
                info={
                    'type':
                    'text/html',
                    'filename':
                    os.path.join(testdata_dir, '277-5312419-9119541.html'),
                },
                entries=r"""
                2017-03-23 * "Amazon.com" "Order"
                  amazon_account: "name@domain.com"
                  amazon_order_id: "277-5312419-9119541"
                  Expenses:FIXME:A   10.80 USD
                    amazon_item_condition: "New"
                    amazon_item_description: "C.R. Gibson 9-Count Coloring File Folders, 3 of Each Design, 10 Adhesive Labels, Measures 11.5 x 9.5 - Gold"
                    amazon_item_quantity: 1
                    amazon_seller: "Amazon.com LLC"
                    shipped_date: 2017-03-24
                  Expenses:FIXME:A   12.60 USD
                    amazon_item_condition: "New"
                    amazon_item_description: "MUJI Gel Ink Ballpoint Pens 0.38mm 9-colors Pack"
                    amazon_item_quantity: 1
                    amazon_seller: "hidamarifarm"
                    shipped_date: 2017-03-24
                  Expenses:FIXME:A   16.50 USD
                    amazon_item_condition: "New"
                    amazon_item_description: "Cynthia Rowley File Folders, 3 Tab, 6 Folders per package, Leaves Assorted Patterns"
                    amazon_item_quantity: 1
                    amazon_seller: "Sherry Pappas"
                    shipped_date: 2017-03-24
                  Expenses:FIXME:A    8.99 USD
                    amazon_item_condition: "New"
                    amazon_item_description: "V&A William Morris Garden File Folder, Galison"
                    amazon_item_quantity: 1
                    amazon_seller: "Amazon.com LLC"
                    shipped_date: 2017-03-24
                  Expenses:FIXME:A   23.95 USD
                    amazon_item_condition: "New"
                    amazon_item_description: "Bloom Daily Planners All In One Planner, Calendar, Notebook, To-Do List Book, Sketch Book, Coloring Book and More! 9 x 11 Do More of What Makes You Happy"
                    amazon_item_quantity: 1
                    amazon_seller: "bloom daily planners"
                    shipped_date: 2017-03-24
                  Expenses:FIXME:A    8.99 USD
                    amazon_item_condition: "New"
                    amazon_item_description: "Skydue Floral Printed Accordion Document File Folder Expanding Letter Organizer (Pink)"
                    amazon_item_quantity: 1
                    amazon_seller: "Skydue"
                    shipped_date: 2017-03-24
                  Expenses:FIXME:A    1.83 USD
                    amazon_invoice_description: "Sales Tax"
                  Expenses:FIXME:B    7.99 USD
                    amazon_item_condition: "New"
                    amazon_item_description: "Skydue Letter A4 Paper Expanding File Folder Pockets Accordion Document Organizer (Jade)"
                    amazon_item_quantity: 1
                    amazon_seller: "Skydue"
                    shipped_date: 2017-03-27
                  Expenses:FIXME:C   14.84 USD
                    amazon_item_condition: "New"
                    amazon_item_description: "Design Ideas 8758616-DI 8758616-DI Cabo LetterHolder-Copper,Copper,"
                    amazon_item_quantity: 1
                    amazon_seller: "Quidsi Retail LLC"
                    shipped_date: 2017-03-24
                  Expenses:FIXME:C    1.37 USD
                    amazon_invoice_description: "Sales Tax"
                  Expenses:FIXME:D   17.95 USD
                    amazon_item_condition: "New"
                    amazon_item_description: "Cynthia Rowley File Folders, 3 Tab, 6 folders per package, Gold Assorted Patterns"
                    amazon_item_quantity: 1
                    amazon_seller: "Cailler's LLC"
                    shipped_date: 2017-03-25
                  Expenses:FIXME:D   14.99 USD
                    amazon_item_condition: "New"
                    amazon_item_description: "Suncatchers Colorful Bird Stained Glass Effect Resin Mobile - Beautiful Window Hanging - Home Decoration"
                    amazon_item_quantity: 1
                    amazon_seller: "That Internet Shop USA"
                    shipped_date: 2017-03-25
                  Expenses:FIXME:D    1.39 USD
                    amazon_invoice_description: "Sales Tax"
                  Expenses:FIXME:E   12.00 USD
                    amazon_item_condition: "New"
                    amazon_item_description: "Rifle Paper Co. Jardin Weekly Desk Planner Notepad"
                    amazon_item_quantity: 1
                    amazon_seller: "Our Pampered Home"
                    shipped_date: 2017-03-24
                  Liabilities:Credit-Card    -71.06 USD
                    amazon_credit_card_description: "Amazon.com Store Card ending in 1234"
                    transaction_date: 2017-03-25
                  Liabilities:Credit-Card    -16.21 USD
                    amazon_credit_card_description: "Amazon.com Store Card ending in 1234"
                    transaction_date: 2017-03-25
                  Liabilities:Credit-Card    -12.60 USD
                    amazon_credit_card_description: "Amazon.com Store Card ending in 1234"
                    transaction_date: 2017-03-27
                  Liabilities:Credit-Card    -34.33 USD
                    amazon_credit_card_description: "Amazon.com Store Card ending in 1234"
                    transaction_date: 2017-03-27
                  Liabilities:Credit-Card    -12.00 USD
                    amazon_credit_card_description: "Amazon.com Store Card ending in 1234"
                    transaction_date: 2017-03-27
                  Liabilities:Credit-Card     -7.99 USD
                    amazon_credit_card_description: "Amazon.com Store Card ending in 1234"
                    transaction_date: 2017-03-28
                
                """,
            ),
        ])


def test_cleared_and_invalid(tmpdir):
    check_source(
        tmpdir,
        source_spec={
            'module': 'beancount_import.source.amazon',
            'directory': testdata_dir,
            'amazon_account': 'name@domain.com',
            'posttax_adjustment_accounts': {
                'Gift Card Amount': 'Assets:Gift-Cards:Amazon',
                'Rewards Points': 'Income:Amazon:Cashback',
            },
        },
        journal_contents=r"""
        plugin "beancount.plugins.auto_accounts"

        1900-01-01 open Liabilities:Credit-Card
          credit_card_last_4_digits: "1234"

        2017-03-23 * "Amazon.com" "Order"
          amazon_account: "name@domain.com"
          amazon_order_id: "277-5312419-9119541"
          Expenses:FIXME:A   10.80 USD
            amazon_item_condition: "New"
            amazon_item_description: "C.R. Gibson 9-Count Coloring File Folders, 3 of Each Design, 10 Adhesive Labels, Measures 11.5 x 9.5 - Gold"
            amazon_item_quantity: 1
            amazon_seller: "Amazon.com LLC"
            shipped_date: 2017-03-24
          Expenses:FIXME:A   12.60 USD
            amazon_item_condition: "New"
            amazon_item_description: "MUJI Gel Ink Ballpoint Pens 0.38mm 9-colors Pack"
            amazon_item_quantity: 1
            amazon_seller: "hidamarifarm"
            shipped_date: 2017-03-24
          Expenses:FIXME:A   16.50 USD
            amazon_item_condition: "New"
            amazon_item_description: "Cynthia Rowley File Folders, 3 Tab, 6 Folders per package, Leaves Assorted Patterns"
            amazon_item_quantity: 1
            amazon_seller: "Sherry Pappas"
            shipped_date: 2017-03-24
          Expenses:FIXME:A    8.99 USD
            amazon_item_condition: "New"
            amazon_item_description: "V&A William Morris Garden File Folder, Galison"
            amazon_item_quantity: 1
            amazon_seller: "Amazon.com LLC"
            shipped_date: 2017-03-24
          Expenses:FIXME:A   23.95 USD
            amazon_item_condition: "New"
            amazon_item_description: "Bloom Daily Planners All In One Planner, Calendar, Notebook, To-Do List Book, Sketch Book, Coloring Book and More! 9 x 11 Do More of What Makes You Happy"
            amazon_item_quantity: 1
            amazon_seller: "bloom daily planners"
            shipped_date: 2017-03-24
          Expenses:FIXME:A    8.99 USD
            amazon_item_condition: "New"
            amazon_item_description: "Skydue Floral Printed Accordion Document File Folder Expanding Letter Organizer (Pink)"
            amazon_item_quantity: 1
            amazon_seller: "Skydue"
            shipped_date: 2017-03-24
          Expenses:FIXME:A    1.83 USD
            amazon_invoice_description: "Sales Tax"
          Expenses:FIXME:B    7.99 USD
            amazon_item_condition: "New"
            amazon_item_description: "Skydue Letter A4 Paper Expanding File Folder Pockets Accordion Document Organizer (Jade)"
            amazon_item_quantity: 1
            amazon_seller: "Skydue"
            shipped_date: 2017-03-27
          Expenses:FIXME:C   14.84 USD
            amazon_item_condition: "New"
            amazon_item_description: "Design Ideas 8758616-DI 8758616-DI Cabo LetterHolder-Copper,Copper,"
            amazon_item_quantity: 1
            amazon_seller: "Quidsi Retail LLC"
            shipped_date: 2017-03-24
          Expenses:FIXME:C    1.37 USD
            amazon_invoice_description: "Sales Tax"
          Expenses:FIXME:D   17.95 USD
            amazon_item_condition: "New"
            amazon_item_description: "Cynthia Rowley File Folders, 3 Tab, 6 folders per package, Gold Assorted Patterns"
            amazon_item_quantity: 1
            amazon_seller: "Cailler's LLC"
            shipped_date: 2017-03-25
          Expenses:FIXME:D   14.99 USD
            amazon_item_condition: "New"
            amazon_item_description: "Suncatchers Colorful Bird Stained Glass Effect Resin Mobile - Beautiful Window Hanging - Home Decoration"
            amazon_item_quantity: 1
            amazon_seller: "That Internet Shop USA"
            shipped_date: 2017-03-25
          Expenses:FIXME:D    1.39 USD
            amazon_invoice_description: "Sales Tax"
          Expenses:FIXME:E   12.00 USD
            amazon_item_condition: "New"
            amazon_item_description: "Rifle Paper Co. Jardin Weekly Desk Planner Notepad"
            amazon_item_quantity: 1
            amazon_seller: "Our Pampered Home"
            shipped_date: 2017-03-24
          Liabilities:Credit-Card    -71.06 USD
            amazon_credit_card_description: "Amazon.com Store Card ending in 1234"
            transaction_date: 2017-03-25
          Liabilities:Credit-Card    -16.21 USD
            amazon_credit_card_description: "Amazon.com Store Card ending in 1234"
            transaction_date: 2017-03-25
          Liabilities:Credit-Card    -12.60 USD
            amazon_credit_card_description: "Amazon.com Store Card ending in 1234"
            transaction_date: 2017-03-27
          Liabilities:Credit-Card    -34.33 USD
            amazon_credit_card_description: "Amazon.com Store Card ending in 1234"
            transaction_date: 2017-03-27
          Liabilities:Credit-Card    -12.00 USD
            amazon_credit_card_description: "Amazon.com Store Card ending in 1234"
            transaction_date: 2017-03-27
          Liabilities:Credit-Card     -7.99 USD
            amazon_credit_card_description: "Amazon.com Store Card ending in 1234"
            transaction_date: 2017-03-28

        2016-08-30 * "Amazon.com" "Order"
          amazon_account: "name@domain.com"
          amazon_order_id: "781-8429198-6057878"
          invalid_id: "a"
          Expenses:FIXME:A   14.99 USD
            amazon_item_condition: "New"
            amazon_item_description: "Car Mount, Anker CD Slot Universal Phone Holder for iPhone 6s/6/6s plus/6 plus, Samsung S7/S6/edge, Samsung Note 7/5, LG G5, Nexus 5X/6/6P, Moto, HTC,"
            amazon_item_quantity: 1
            amazon_seller: "AnkerDirect"
            shipped_date: 2016-08-30
          Expenses:FIXME:A    5.99 USD
            amazon_invoice_description: "Shipping & Handling"
          Expenses:FIXME:A    1.99 USD
            amazon_invoice_description: "Sales Tax"
          Liabilities:Credit-Card    -22.97 USD
            amazon_credit_card_description: "Amazon.com Visa Signature ending in 1234"
            transaction_date: 2016-08-30


        2016-08-30 * "Amazon.com" "Order"
          amazon_account: "name@domain.com"
          amazon_order_id: "781-8429198-6057878"
          invalid_id: "b"
          Expenses:FIXME:A   14.99 USD
            amazon_item_condition: "New"
            amazon_item_description: "Car Mount, Anker CD Slot Universal Phone Holder for iPhone 6s/6/6s plus/6 plus, Samsung S7/S6/edge, Samsung Note 7/5, LG G5, Nexus 5X/6/6P, Moto, HTC,"
            amazon_item_quantity: 1
            amazon_seller: "AnkerDirect"
            shipped_date: 2016-08-30
          Expenses:FIXME:A    5.99 USD
            amazon_invoice_description: "Shipping & Handling"
          Expenses:FIXME:A    1.99 USD
            amazon_invoice_description: "Sales Tax"
          Liabilities:Credit-Card    -22.97 USD
            amazon_credit_card_description: "Amazon.com Visa Signature ending in 1234"
            transaction_date: 2016-08-30

        """,
        pending=[
            import_result(
                info={
                    'type':
                    'text/html',
                    'filename':
                    os.path.join(testdata_dir, '166-7926740-5141621.html'),
                },
                entries=r"""
                2016-02-07 * "Amazon.com" "Order"
                  amazon_account: "name@domain.com"
                  amazon_order_id: "166-7926740-5141621"
                  Expenses:FIXME:A   11.87 USD
                    amazon_item_condition: "New"
                    amazon_item_description: "Casio Men's W916-8AV Alarm Chronograph Watch, classic"
                    amazon_item_quantity: 1
                    amazon_seller: "Amazon.com LLC"
                    shipped_date: 2016-02-08
                  Expenses:FIXME:A    1.13 USD
                    amazon_invoice_description: "Sales Tax"
                  Income:Amazon:Cashback     -1.06 USD
                    amazon_posttax_adjustment: "Rewards Points"
                  Liabilities:Credit-Card    -11.94 USD
                    amazon_credit_card_description: "Amazon.com Visa Signature ending in 1234"
                    transaction_date: 2016-02-08
                
                """,
            ),
            import_result(
                info={
                    'type':
                    'text/html',
                    'filename':
                    os.path.join(testdata_dir, 'D56-5204779-4181560.html')
                },
                entries=r"""
                2016-06-30 * "Amazon.com" "Order"
                  amazon_account: "name@domain.com"
                  amazon_order_id: "D56-5204779-4181560"
                  Expenses:FIXME:A   14.99 USD
                    amazon_item_by: "Robert Zubrin, Arthur C. Clarke, Richard Wagner"
                    amazon_item_description: "Case for Mars"
                    amazon_item_url: "https://www.amazon.com/dp/B004G8QU6U/ref=docs-os-doi_0"
                    amazon_seller: "Simon and Schuster Digital Sales Inc"
                  Liabilities:Credit-Card    -14.99 USD
                    amazon_credit_card_description: "VISA ending in 1234"
                    transaction_date: 2016-06-30
                
                """,
            ),
        ],
        invalid_references=[(1, [('a', None), ('b', None)])],
    )


def test_prediction(tmpdir):
    check_source(
        tmpdir,
        source_spec={
            'module': 'beancount_import.source.amazon',
            'directory': testdata_dir,
            'amazon_account': 'name@domain.com',
        },
        journal_contents=r"""
        plugin "beancount.plugins.auto_accounts"

        1900-01-01 open Liabilities:Credit-Card
          credit_card_last_4_digits: "1234"

        2017-03-23 * "Amazon.com" "Order"
          amazon_account: "name@domain.com"
          amazon_order_id: "277-5312419-9119541"
          Expenses:FIXME:A   10.80 USD
            amazon_item_condition: "New"
            amazon_item_description: "C.R. Gibson 9-Count Coloring File Folders, 3 of Each Design, 10 Adhesive Labels, Measures 11.5 x 9.5 - Gold"
            amazon_item_quantity: 1
            amazon_seller: "Amazon.com LLC"
            shipped_date: 2017-03-24
          Expenses:FIXME:A   12.60 USD
            amazon_item_condition: "New"
            amazon_item_description: "MUJI Gel Ink Ballpoint Pens 0.38mm 9-colors Pack"
            amazon_item_quantity: 1
            amazon_seller: "hidamarifarm"
            shipped_date: 2017-03-24
          Expenses:FIXME:A   16.50 USD
            amazon_item_condition: "New"
            amazon_item_description: "Cynthia Rowley File Folders, 3 Tab, 6 Folders per package, Leaves Assorted Patterns"
            amazon_item_quantity: 1
            amazon_seller: "Sherry Pappas"
            shipped_date: 2017-03-24
          Expenses:FIXME:A    8.99 USD
            amazon_item_condition: "New"
            amazon_item_description: "V&A William Morris Garden File Folder, Galison"
            amazon_item_quantity: 1
            amazon_seller: "Amazon.com LLC"
            shipped_date: 2017-03-24
          Expenses:FIXME:A   23.95 USD
            amazon_item_condition: "New"
            amazon_item_description: "Bloom Daily Planners All In One Planner, Calendar, Notebook, To-Do List Book, Sketch Book, Coloring Book and More! 9 x 11 Do More of What Makes You Happy"
            amazon_item_quantity: 1
            amazon_seller: "bloom daily planners"
            shipped_date: 2017-03-24
          Expenses:FIXME:A    8.99 USD
            amazon_item_condition: "New"
            amazon_item_description: "Skydue Floral Printed Accordion Document File Folder Expanding Letter Organizer (Pink)"
            amazon_item_quantity: 1
            amazon_seller: "Skydue"
            shipped_date: 2017-03-24
          Expenses:FIXME:A    1.83 USD
            amazon_invoice_description: "Sales Tax"
          Expenses:Office-Supplies    7.99 USD
            amazon_item_condition: "New"
            amazon_item_description: "Skydue Letter A4 Paper Expanding File Folder Pockets Accordion Document Organizer (Jade)"
            amazon_item_quantity: 1
            amazon_seller: "Skydue"
            shipped_date: 2017-03-27
          Expenses:FIXME:C   14.84 USD
            amazon_item_condition: "New"
            amazon_item_description: "Design Ideas 8758616-DI 8758616-DI Cabo LetterHolder-Copper,Copper,"
            amazon_item_quantity: 1
            amazon_seller: "Quidsi Retail LLC"
            shipped_date: 2017-03-24
          Expenses:FIXME:C    1.37 USD
            amazon_invoice_description: "Sales Tax"
          Expenses:FIXME:D   17.95 USD
            amazon_item_condition: "New"
            amazon_item_description: "Cynthia Rowley File Folders, 3 Tab, 6 folders per package, Gold Assorted Patterns"
            amazon_item_quantity: 1
            amazon_seller: "Cailler's LLC"
            shipped_date: 2017-03-25
          Expenses:FIXME:D   14.99 USD
            amazon_item_condition: "New"
            amazon_item_description: "Suncatchers Colorful Bird Stained Glass Effect Resin Mobile - Beautiful Window Hanging - Home Decoration"
            amazon_item_quantity: 1
            amazon_seller: "That Internet Shop USA"
            shipped_date: 2017-03-25
          Expenses:FIXME:D    1.39 USD
            amazon_invoice_description: "Sales Tax"
          Expenses:FIXME:E   12.00 USD
            amazon_item_condition: "New"
            amazon_item_description: "Rifle Paper Co. Jardin Weekly Desk Planner Notepad"
            amazon_item_quantity: 1
            amazon_seller: "Our Pampered Home"
            shipped_date: 2017-03-24
          Liabilities:Credit-Card    -71.06 USD
            amazon_credit_card_description: "Amazon.com Store Card ending in 1234"
            transaction_date: 2017-03-25
          Liabilities:Credit-Card    -16.21 USD
            amazon_credit_card_description: "Amazon.com Store Card ending in 1234"
            transaction_date: 2017-03-25
          Liabilities:Credit-Card    -12.60 USD
            amazon_credit_card_description: "Amazon.com Store Card ending in 1234"
            transaction_date: 2017-03-27
          Liabilities:Credit-Card    -34.33 USD
            amazon_credit_card_description: "Amazon.com Store Card ending in 1234"
            transaction_date: 2017-03-27
          Liabilities:Credit-Card    -12.00 USD
            amazon_credit_card_description: "Amazon.com Store Card ending in 1234"
            transaction_date: 2017-03-27
          Liabilities:Credit-Card     -7.99 USD
            amazon_credit_card_description: "Amazon.com Store Card ending in 1234"
            transaction_date: 2017-03-28

        2016-08-30 * "Amazon.com" "Order"
          amazon_account: "name@domain.com"
          amazon_order_id: "781-8429198-6057878"
          invalid_id: "b"
          Expenses:Miscellaneous   14.99 USD
            amazon_item_condition: "New"
            amazon_item_description: "Car Mount, Anker CD Slot Universal Phone Holder for iPhone 6s/6/6s plus/6 plus, Samsung S7/S6/edge, Samsung Note 7/5, LG G5, Nexus 5X/6/6P, Moto, HTC,"
            amazon_item_quantity: 1
            amazon_seller: "AnkerDirect"
            shipped_date: 2016-08-30
          Expenses:Miscellaneous    5.99 USD
            amazon_invoice_description: "Shipping & Handling"
          Expenses:Miscellaneous    1.99 USD
            amazon_invoice_description: "Sales Tax"
          Liabilities:Credit-Card    -22.97 USD
            amazon_credit_card_description: "Amazon.com Visa Signature ending in 1234"
            transaction_date: 2016-08-30

        """,
        pending=[
            import_result(
                info={
                    'type':
                    'text/html',
                    'filename':
                    os.path.join(testdata_dir, '166-7926740-5141621.html'),
                },
                entries=r"""
                2016-02-07 * "Amazon.com" "Order"
                  amazon_account: "name@domain.com"
                  amazon_order_id: "166-7926740-5141621"
                  Expenses:FIXME:A   11.87 USD
                    amazon_item_condition: "New"
                    amazon_item_description: "Casio Men's W916-8AV Alarm Chronograph Watch, classic"
                    amazon_item_quantity: 1
                    amazon_seller: "Amazon.com LLC"
                    shipped_date: 2016-02-08
                  Expenses:FIXME:A    1.13 USD
                    amazon_invoice_description: "Sales Tax"
                  Expenses:FIXME     -1.06 USD
                    amazon_posttax_adjustment: "Rewards Points"
                  Liabilities:Credit-Card    -11.94 USD
                    amazon_credit_card_description: "Amazon.com Visa Signature ending in 1234"
                    transaction_date: 2016-02-08
                
                """,
                unknown_account_prediction_inputs=[
                    PredictionInput(
                        source_account='',
                        amount=Amount(D('1.13'), 'USD'),
                        date=datetime.date(2016, 2, 7),
                        key_value_pairs={
                            'amazon_item_description': [
                                "Casio Men's W916-8AV Alarm Chronograph Watch, classic"
                            ]
                        }),
                    PredictionInput(
                        source_account='',
                        amount=Amount(D('-1.06'), 'USD'),
                        date=datetime.date(2016, 2, 7),
                        key_value_pairs={
                            'amazon_posttax_adjustment': ['Rewards Points']
                        }),
                ],
            ),
            import_result(
                info={
                    'type':
                    'text/html',
                    'filename':
                    os.path.join(testdata_dir, 'D56-5204779-4181560.html')
                },
                entries=r"""
                2016-06-30 * "Amazon.com" "Order"
                  amazon_account: "name@domain.com"
                  amazon_order_id: "D56-5204779-4181560"
                  Expenses:FIXME:A   14.99 USD
                    amazon_item_by: "Robert Zubrin, Arthur C. Clarke, Richard Wagner"
                    amazon_item_description: "Case for Mars"
                    amazon_item_url: "https://www.amazon.com/dp/B004G8QU6U/ref=docs-os-doi_0"
                    amazon_seller: "Simon and Schuster Digital Sales Inc"
                  Liabilities:Credit-Card    -14.99 USD
                    amazon_credit_card_description: "VISA ending in 1234"
                    transaction_date: 2016-06-30
                
                """,
            ),
        ],
        training_examples=[
            (PredictionInput(
                source_account='',
                amount=Amount(D('14.99'), 'USD'),
                date=datetime.date(2016, 8, 30),
                key_value_pairs={
                    'amazon_account': ['name@domain.com'],
                    'amazon_item_description': [
                        'Car Mount, Anker CD Slot Universal Phone Holder for iPhone 6s/6/6s plus/6 plus, Samsung S7/S6/edge, Samsung Note 7/5, LG G5, Nexus 5X/6/6P, Moto, HTC,'
                    ]
                }), 'Expenses:Miscellaneous'),
            (PredictionInput(
                source_account='',
                amount=Amount(D('-22.97'), 'USD'),
                date=datetime.date(2016, 8, 30),
                key_value_pairs={
                    'amazon_account': ['name@domain.com'],
                    'amazon_credit_card_description':
                    ['Amazon.com Visa Signature ending in 1234']
                }), 'Liabilities:Credit-Card'),
            (PredictionInput(
                source_account='',
                amount=Amount(D('7.99'), 'USD'),
                date=datetime.date(2017, 3, 23),
                key_value_pairs={
                    'amazon_account': ['name@domain.com'],
                    'amazon_item_description': [
                        'Skydue Letter A4 Paper Expanding File Folder Pockets Accordion Document Organizer (Jade)'
                    ]
                }), 'Expenses:Office-Supplies'),
            (PredictionInput(
                source_account='',
                amount=Amount(D('-71.06'), 'USD'),
                date=datetime.date(2017, 3, 23),
                key_value_pairs={
                    'amazon_account': ['name@domain.com'],
                    'amazon_credit_card_description':
                    ['Amazon.com Store Card ending in 1234']
                }), 'Liabilities:Credit-Card'),
            (PredictionInput(
                source_account='',
                amount=Amount(D('-16.21'), 'USD'),
                date=datetime.date(2017, 3, 23),
                key_value_pairs={
                    'amazon_account': ['name@domain.com'],
                    'amazon_credit_card_description':
                    ['Amazon.com Store Card ending in 1234']
                }), 'Liabilities:Credit-Card'),
            (PredictionInput(
                source_account='',
                amount=Amount(D('-12.60'), 'USD'),
                date=datetime.date(2017, 3, 23),
                key_value_pairs={
                    'amazon_account': ['name@domain.com'],
                    'amazon_credit_card_description':
                    ['Amazon.com Store Card ending in 1234']
                }), 'Liabilities:Credit-Card'),
            (PredictionInput(
                source_account='',
                amount=Amount(D('-34.33'), 'USD'),
                date=datetime.date(2017, 3, 23),
                key_value_pairs={
                    'amazon_account': ['name@domain.com'],
                    'amazon_credit_card_description':
                    ['Amazon.com Store Card ending in 1234']
                }), 'Liabilities:Credit-Card'),
            (PredictionInput(
                source_account='',
                amount=Amount(D('-12.00'), 'USD'),
                date=datetime.date(2017, 3, 23),
                key_value_pairs={
                    'amazon_account': ['name@domain.com'],
                    'amazon_credit_card_description':
                    ['Amazon.com Store Card ending in 1234']
                }), 'Liabilities:Credit-Card'),
            (PredictionInput(
                source_account='',
                amount=Amount(D('-7.99'), 'USD'),
                date=datetime.date(2017, 3, 23),
                key_value_pairs={
                    'amazon_account': ['name@domain.com'],
                    'amazon_credit_card_description':
                    ['Amazon.com Store Card ending in 1234']
                }), 'Liabilities:Credit-Card'),
        ],
    )
