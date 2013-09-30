#!/bin/sh

DATABASE="test"
monetdb stop $DATABASE
monetdb destroy $DATABASE
monetdb create $DATABASE
monetdb release $DATABASE
echo "create schema test_schema;" | mclient $DATABASE
echo "create schema test_schema2;" | mclient $DATABASE
echo "alter user monetdb set schema test_schema2;" | mclient $DATABASE

