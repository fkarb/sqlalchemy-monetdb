#!/usr/bin/env python
"""
Setup for SQLAlchemy backend for MonetDB
"""
from setuptools import find_packages, setup
import os
readme = os.path.join(os.path.dirname(__file__), 'README.rst')


setup_params = dict(
    name="sqlalchemy_monetdb",
    version="0.1",
    description="SQLAlchemy dialect for MonetDB",
    long_description=open(readme).read(),
    classifiers=[
      'Development Status :: 4 - Beta',
      'Environment :: Console',
      'Intended Audience :: Developers',
      'Programming Language :: Python',
      'Programming Language :: Python :: 3',
      'Programming Language :: Python :: Implementation :: CPython',
      'Programming Language :: Python :: Implementation :: PyPy',
      'Topic :: Database :: Front-Ends',
      ],
    keywords='MonetDB  SQLAlchemy',
    packages=find_packages(),
    include_package_data=True,
    tests_require=['nose >= 0.11'],
    test_suite="run_tests.setup_py_test",
    zip_safe=False,
    entry_points={
        "sqlalchemy.dialects": ["monetdb = sqlalchemy_monetdb.base:MDBDialect"]
    },
    license="MIT",
)

if __name__ == '__main__':
    setup(**setup_params)

