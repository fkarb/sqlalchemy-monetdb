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

or from the source folder::

    $ pip install .


usage
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


more info
---------

 * http://www.sqlalchemy.org/
 * http://www.monetdb.org/


Authors
-------

 * Matt Harrison
 * Pete Hollobon
 * Gijs Molenaar
