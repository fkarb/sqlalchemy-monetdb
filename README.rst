MonetDB dialect for SQLAlchemy
==============================

This is the MonetDB dialect driver for SQLAlchemy. It has support for Python from 3.3 onwards and even PyPy. It supports
SQLalchemy 1.0, 1.1, 1.2, 1.3 & 1.4.


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


MonetDB Lite (experimental)
---------------------------

We now have experimental support for MonetDB Lite! Note that this is experimental, and will probably break.

First you need to manually install MonetDB Lite: https://github.com/hannesmuehleisen/MonetDBLite-Python

To connect to a MonetDBLite database use this syntax::

    from sqlalchemy import create_engine
    engine = create_engine('monetdb+lite:////tmp/monetdb_lite')


More info
---------

 * http://www.sqlalchemy.org/
 * http://www.monetdb.org/


Development
-----------

.. image:: https://travis-ci.org/gijzelaerr/sqlalchemy-monetdb.png?branch=master
  :target: https://travis-ci.org/gijzelaerr/sqlalchemy-monetdb


