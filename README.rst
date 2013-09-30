MonetDB dialect for SQLAlchemy
==============================

installation
------------

to install this dialect run::

    $ pip install -r ./requirements.txt
    $ python ./setup.py install

usage
-----

To start using this dialect::

    from sqlalchemy import create_engine
    engine = create_engine('monetdb:///demo:', echo=True)

If you don't want to install this library (for example during development) add
this folder to your PYTHONPATH and register this dialect with SQLAlchemy::

    from sqlalchemy.dialects import registry
    registry.register("monetdb", "sqlalchemy_monetdb", "MDBDialect")

testing
-------

you need to have nose and mock installed::

    $ pip install nose mock

create a database test::

    $ monetdb create test && monetdb release test

Create a test schema::

    $ echo "create schema test_schema;" | mclient test
    $ echo "create schema test_schema2;" | mclient test
    $ echo "alter user monetdb set schema test_schema2;" | mclient test

Run the test suite::

    $ ./test_suite.py



more info
---------

 * http://www.sqlalchemy.org/
 * http://www.monetdb.org/


Authors
-------

 * Matt Harrison
 * Pete Hollobon
 * Gijs Molenaar
