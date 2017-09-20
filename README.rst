MonetDB dialect for SQLAlchemy
==============================
This is the MonetDB dialect driver for SQLAlchemy.


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

We also have experimental support for MonetDB Lite!

For this install this dialect with an extra option::

    $ pip install sqlalchemy_monetdb[lite]


Now can start using MonetDB Lite::

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


