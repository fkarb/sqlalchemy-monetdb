import pytest
from sqlalchemy.testing.suite import *
from sqlalchemy.testing.suite.test_ddl import FutureTableDDLTest, TableDDLTest
from sqlalchemy.testing.suite.test_dialect import DifficultParametersTest
from sqlalchemy.testing.suite.test_select import FetchLimitOffsetTest

class DifficultParametersTest(DifficultParametersTest):
    @pytest.mark.skip(reason="Monet does not support identifiers with an '%' in them.")
    def test_round_trip(*args, **kwargs):
        pass


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

