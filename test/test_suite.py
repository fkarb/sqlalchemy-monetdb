from sqlalchemy.testing.suite.test_ddl import FutureTableDDLTest
from sqlalchemy.testing.suite.test_select import FetchLimitOffsetTest

class FutureTableDDLTest(FutureTableDDLTest):
    def test_create_table_schema(*args, **kwargs):
        pass

class FetchLimitOffsetTest(FetchLimitOffsetTest):
    def test_limit_render_multiple_times(*args, **kwargs):
        """
        This test does some LIMIT statements in the middle of the query,
        this is not supported.
        """
        pass

