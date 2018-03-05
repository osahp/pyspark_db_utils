"""
Just a regular `setup.py` file.
@author: Vladimir Smelov
"""


import os
from setuptools import setup

current_dir = os.path.abspath(os.path.dirname(__file__))


with open(os.path.join(current_dir, 'README.md'), encoding='utf-8') as f:
    long_description = f.read()

setup(name='pyspark_db_utils',
      version='0.0.3',
      description='Usefull functions for working with Database in PySpark (PostgreSQL, ClickHouse)',
      url='https://github.com/osahp/pyspark_db_utils',
      author='Vladimir Smelov',
      author_email='vladimirfol@gmail.com',
      packages=['pyspark_db_utils'],
      classifiers=[
        'Development Status :: 3 - Alpha',
        'Programming Language :: Python :: 3',
        'Topic :: Database',
        'Topic :: Utilities',
      ],
      keywords='postgres postgresql clickhouse sql pyspark spark',
      zip_safe=False,
      license='MIT',
      install_requires=[
            'pyspark',
            'jaydebeapi',
      ],
      test_suite='nose.collector',
      tests_require=[
          'psycopg2',
          'testing.postgresql',
      ],
      include_package_data=True,
      long_description=long_description)
