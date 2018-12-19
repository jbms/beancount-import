import os
from setuptools import setup

with open(os.path.join(os.path.dirname(__file__), 'README.md'), 'r') as f:
    long_description = f.read()

setup(
    name='beancount-import',
    description='Semi-automatic importing of external data into beancount.',
    long_description=long_description,
    long_description_content_type='text/markdown',
    url='https://github.com/jbms/beancount-import',
    version='1.0.7',
    author='Jeremy Maitin-Shepard',
    author_email="jeremy@jeremyms.com",
    license='GPLv2',
    packages=[
        "beancount_import",
        "beancount_import.source",
    ],
    package_data={
        'beancount_import': ['frontend_dist/prod/index.html'],
    },
    python_requires='>=3.5',
    install_requires=[
        'beancount>=2.1.3',
        'tornado',
        'numpy',
        'scipy',
        'scikit-learn',
        'nltk',
        'python-dateutil',
        'atomicwrites',
        'jsonschema',
    ],
    test_requirements=[
        'pytest',
        'coverage',
    ],
)
