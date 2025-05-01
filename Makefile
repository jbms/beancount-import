.RECIPEPREFIX = -

mypy-onefile:
- mypy beancount_import/source/amazon_invoice.py --disable-error-code=type-abstract --no-implicit-reexport

mypy:
- mypy beancount_import --disable-error-code=type-abstract

test:
- pipenv run coverage run -m pytest -vv