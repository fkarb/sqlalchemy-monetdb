#!/usr/bin/env python

from sqlalchemy.dialects import registry
from sqlalchemy.testing import runner


registry.register("monetdb", "sqlalchemy_monetdb.dialect", "MonetDialect")


def setup_py_test():
    runner.setup_py_test()


if __name__ == '__main__':
    runner.main()
