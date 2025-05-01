.RECIPEPREFIX = -

mypy-onefile:
- pipenv run mypy beancount_import/source/amazon_invoice.py --disable-error-code=type-abstract --no-implicit-reexport

mypy:
- pipenv run mypy beancount_import --disable-error-code=type-abstract

test:
- pipenv run coverage run -m pytest -vv


install:
- pipenv install --dev

rmvenv:
- rm -rf .venv

clean: rmvenv install

bs_local:
- pip install -e ../beancount-stubs/
