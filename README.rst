MonetDB dialect for SQLAlchemy
==============================

.. image:: https://travis-ci.org/gijzelaerr/sqlalchemy-monetdb.png?branch=master
  :target: https://travis-ci.org/gijzelaerr/sqlalchemy-monetdb

.. image:: https://badges.gitter.im/Join Chat.svg
  :target: https://gitter.im/gijzelaerr/sqlalchemy-monetdb?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge&utm_content=badge

This is the MonetDB dialect driver for SQLAlchemy.


installation
------------

To install this dialect run::

    $ pip install sqlalchemy_monetdb

or from source::

    $ python setup.py install


usage
-----

To start using this dialect::

    from sqlalchemy import create_engine
    engine = create_engine('monetdb:///demo:', echo=True)


testing
-------

create a database test::

    $ monetdb create test && monetdb release test

Create a test schema::

    $ echo "create schema test_schema;" | mclient test
    $ echo "create schema test_schema2;" | mclient test
    $ echo "alter user monetdb set schema test_schema2;" | mclient test

Run the test suite::

    $ python setup.py test



more info
---------

 * http://www.sqlalchemy.org/
 * http://www.monetdb.org/


Authors
-------

 * Matt Harrison
 * Pete Hollobon
 * Gijs Molenaar
