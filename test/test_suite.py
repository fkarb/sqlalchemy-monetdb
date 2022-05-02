import pytest
from sqlalchemy.testing.suite import *
from sqlalchemy.testing.suite.test_ddl import FutureTableDDLTest, TableDDLTest
from sqlalchemy.testing.suite.test_select import FetchLimitOffsetTest

class TableDDLTest(TableDDLTest):
    @pytest.mark.skip(reason="column names of level >= 3")
    def test_create_table_schema(*args, **kwargs):
        pass

class FutureTableDDLTest(FutureTableDDLTest):
    @pytest.mark.skip(reason="column names of level >= 3")
    def test_create_table_schema(*args, **kwargs):
        pass

class FetchLimitOffsetTest(FetchLimitOffsetTest):
    @pytest.mark.skip(reason="This test does some LIMIT statements in the middle of the query, this is not supported.")
    def test_limit_render_multiple_times(*args, **kwargs):
        """
        This test does some LIMIT statements in the middle of the query,
        this is not supported.
        """
        pass

