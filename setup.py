#!/usr/bin/env python

from setuptools import setup

setup(name='tap-salesforce',
      version='1.6.0',
      description='Singer.io tap for extracting data from the Salesforce API',
      author='Stitch',
      url='https://singer.io',
      classifiers=['Programming Language :: Python :: 3 :: Only'],
      py_modules=['tap_salesforce'],
      install_requires=[
          'requests>=2.20.0,<=2.29.0',
          'pipelinewise-singer-python==1.2.0',
          'xmltodict==0.11.0',
          'openpyxl==3.1.3',
          'hotglue-singer-sdk>=1.0.15,<2.0.0',
          'hotglue-etl-exceptions>=0.1.0'
      ],
      extras_require={
          'dev': [
              'pytest>=7.0.0',
              'pre-commit>=2.17.0',
              'ruff>=0.1.0',
              'tox>=4.0.0',
          ]
      },
      entry_points='''
          [console_scripts]
          tap-salesforce=tap_salesforce:main
      ''',
      packages=['tap_salesforce', 'tap_salesforce.salesforce'],
      package_data = {
          'tap_salesforce/schemas': [
              # add schema.json filenames here
          ]
      },
      include_package_data=True,
)
