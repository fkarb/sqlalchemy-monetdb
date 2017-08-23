#!/bin/bash -ve

DATABASE="test"
FARM="/tmp/monetdb"
USER="monetdb"
PASSWORD=${USER}

echo -en "user=${USER}\npassword=${PASSWORD}\n" > ~/.monetdb

monetdbd stop ${FARM} || true
monetdbd create ${FARM} || true
monetdbd start ${FARM}

monetdb stop $DATABASE || true
monetdb destroy -f $DATABASE || true
monetdb create $DATABASE
monetdb release $DATABASE
echo "create schema test_schema;" | mclient -d $DATABASE
echo "create schema test_schema2;" | mclient -d $DATABASE
echo "alter user monetdb set schema test_schema2;" | mclient -d $DATABASE

