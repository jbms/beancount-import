import os
from setuptools import setup

import glob
import sys

def get_egg_file(module_name):
    def f(packages):
        return glob.glob(
            os.path.join(os.path.dirname(os.path.dirname(sys.executable)),
                         'lib', 'python*', packages, module_name + '.egg-link'))

    return f('site-packages') or f('dist-packages')

egg_file = get_egg_file('beancount_import')
if egg_file:
    os.remove(egg_file[0])
    
with open(
        os.path.join(os.path.dirname(__file__), 'README.md'),
        'r',
        newline='\n',
        encoding='utf-8') as f:
    long_description = f.read()

setup(
    name='beancount-import',
    description='Semi-automatic importing of external data into beancount.',
    long_description=long_description,
    long_description_content_type='text/markdown',
    url='https://github.com/jbms/beancount-import',
    version='1.4.0',
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
        'atomicwrites>=1.3.0',
        'jsonschema',
        'watchdog',
        'ofxstatement>=0.6.5',
    ],
    test_requirements=[
        'pytest',
        'coverage',
    ],
)
