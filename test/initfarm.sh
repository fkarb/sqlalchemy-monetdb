#!/usr/bin/env bash -ve

FARM="/tmp/monetdb"


monetdbd stop ${FARM} || true
monetdbd create ${FARM} || true
monetdbd start ${FARM}