MonetDB dialect for SQLAlchemy
==============================

This is the MonetDB dialect driver for SQLAlchemy. It has support for Python 2.7, 3.3+ and even PyPy. It supports
SQLalchemy 1.0, 1.1 and 1.2.


Installation
------------

To install this dialect run::

    $ pip install sqlalchemy_monetdb

or from the source folder::

    $ pip install .


Usage
-----

To start using this dialect::

    from sqlalchemy import create_engine
    engine = create_engine('monetdb:///demo:', echo=True)


More info
---------

 * http://www.sqlalchemy.org/
 * http://www.monetdb.org/


Development
-----------

.. image:: https://travis-ci.org/gijzelaerr/sqlalchemy-monetdb.png?branch=master
  :target: https://travis-ci.org/gijzelaerr/sqlalchemy-monetdb


