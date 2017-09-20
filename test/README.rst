testing
-------

First you need to set up a test database.

create a database test::

    $ monetdb create test && monetdb release test

Create a test schema::

    $ echo "create schema test_schema;" | mclient test
    $ echo "create schema test_schema2;" | mclient test
    $ echo "alter user monetdb set schema test_schema2;" | mclient test

Now you can run the test suite::

    $ python setup.py test


If you want to test the MonetDBlite dialect backend you need to use pytest::

    $ pytest --db=monetdblite


The ``--db`` flag selects one of the preconfigured database URLs defined in setup.cfg.
