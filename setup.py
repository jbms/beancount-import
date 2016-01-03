from setuptools import setup

setup(name='beancount-import',
      description='Automatic importing of bank/credit card transactions into beancount.',
      url='https://github.com/jbms/beancount-import',
      author='Jeremy Maitin-Shepard',
      author_email="jeremy@jeremyms.com",
      license='GPLv2',
      packages=["beancount_import"],
      entry_points={
          'console_scripts': [
              'beancount-import = beancount_import.cli:main',
              ],
          },
      install_requires=[
          'beancount',
          'npyscreen',
          'numpy',
          'scikit-learn',
          'nltk',
          ],
      )
