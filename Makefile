.RECIPEPREFIX = -

# mypy-onefile:
# - pipenv run mypy beancount_import/source/amazon_invoice.py --disable-error-code=type-abstract --no-implicit-reexport

default: mypy

ci: test mypy

mypy:
- pipenv run mypy beancount_import --disable-error-code=type-abstract

test:
- pipenv run coverage run -m pytest -vv


install:
- pipenv install --dev

rmvenv:
- rm -rf .venv

lock:
- pipenv lock

clean-dependencies: rmvenv lock install

bs_local:
- pip install -e ../beancount-stubs/
