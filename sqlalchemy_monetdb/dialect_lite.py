from sqlalchemy_monetdb.dialect import MonetDialect


class MonetLiteDialect(MonetDialect):
    name = "monetdb"
    driver = "monetdblite"

    @classmethod
    def dbapi(cls):
        return __import__(cls.driver)
