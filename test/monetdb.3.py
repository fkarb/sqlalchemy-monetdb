import sys
sys.path.append("..")
import unittest
import datetime

import testbase
import sets
from sqlalchemy import *
from sqlalchemy import sql, exceptions
from sqlalchemy.databases import monetdb
from testlib import *


class TypesTest(AssertMixin):
    "Test Monetdb column types"

    @testing.supported('monetdb')
    def test_basic(self):
        meta1 = MetaData(testbase.db)
        table = Table(
            'monetdb_types', meta1,
            Column('id', Integer, primary_key=True),
            Column('num1', monetdb.MDBInteger(unsigned=True)),
            Column('text1', monetdb.MDBText),
            Column('text2', monetdb.MDBText()),
            Column('num2', monetdb.MDBBigInteger),
            Column('num3', monetdb.MDBBigInteger()),
            Column('num4', monetdb.MDBNumeric),
            Column('num5', monetdb.MDBDouble()),
            )
        try:
            table.drop(checkfirst=True)
            table.create()
            meta2 = MetaData(testbase.db)
            t2 = Table('monetdb_types', meta2, autoload=True)
            assert isinstance(t2.c.num1.type, monetdb.MDBInteger)
            assert isinstance(t2.c.text1.type, monetdb.MDBText)
            assert isinstance(t2.c.text2.type, monetdb.MDBText)
            assert isinstance(t2.c.num2.type, monetdb.MDBBigInteger)
            assert isinstance(t2.c.num3.type, monetdb.MDBBigInteger)
            assert isinstance(t2.c.num4.type, monetdb.MDBNumeric)
            assert isinstance(t2.c.num5.type, monetdb.MDBDouble)

            t2.drop()
            t2.create()
        finally:
            meta1.drop_all()


    @testing.supported('monetdb')
    def test_numeric(self):
        "Exercise type specification and options for numeric types."

        columns = [
            # column type, args, kwargs, expected ddl
            # e.g. Column(Integer(10, unsigned=True)) == 'INTEGER(10) UNSIGNED'
            (monetdb.MDBNumeric, [], {},
             'NUMERIC(10, 2)'),
            (monetdb.MDBNumeric, [None], {},
             'NUMERIC'),
            (monetdb.MDBNumeric, [7], {},
             'NUMERIC(7, 2)'),
            (monetdb.MDBDecimal, [], {},
             'DECIMAL(10, 2)'),
            (monetdb.MDBDecimal, [None], {},
             'DECIMAL'),
            (monetdb.MDBDecimal, [12], {},
             'DECIMAL(12, 2)'),
            (monetdb.MDBDecimal, [12, None], {},
             'DECIMAL'),
            (monetdb.MDBDouble, [None, None], {},
             'DOUBLE'),
            (monetdb.MDBReal, [None, None], {},
             'REAL'),
            (monetdb.MDBInteger, [], {},
             'INT'),
            (monetdb.MDBBigInteger, [], {},
             'BIGINT'),
            (monetdb.MDBSmallInteger, [], {},
             'SMALLINT'),
           ]

        table_args = ['test_monetdb_numeric', MetaData(testbase.db)]
        for index, spec in enumerate(columns):
            type_, args, kw, res = spec
            table_args.append(Column('c%s' % index, type_(*args, **kw)))

        numeric_table = Table(*table_args)
        gen = testbase.db.dialect.schemagenerator(testbase.db.dialect, testbase.db, None, None)

        for col in numeric_table.c:
            index = int(col.name[1:])
            self.assertEquals(gen.get_column_specification(col),
                           "%s %s" % (col.name, columns[index][3]))
            self.assert_(repr(col))

        try:
            numeric_table.create(checkfirst=True)
            assert True
        except:
            raise numeric_table.drop()

    @testing.supported('monetdb')
    def test_type_reflection(self):
        # (ask_for, roundtripped_as_if_different)
        specs = [( String(), monetdb.MDBText(), ),
                 ( String(1), monetdb.MDBString(1), ),
                 ( String(3), monetdb.MDBString(3), ),
                 ( monetdb.MDBChar(1), ),
                 ( monetdb.MDBChar(3), ),
                 ( SmallInteger(), monetdb.MDBSmallInteger(), ),
                 ( SmallInteger(4), monetdb.MDBSmallInteger(4), ),
                 ( monetdb.MDBSmallInteger(), ),
                 ( monetdb.MDBSmallInteger(4), monetdb.MDBSmallInteger(4), ),
                 ( Binary(), monetdb.MDBBinary() ),
                 ( monetdb.MDBBinary(3), monetdb.MDBBinary(3), ),
                 ]

        columns = [Column('c%i' % (i + 1), t[0]) for i, t in enumerate(specs)]

        db = testbase.db
        m = MetaData(db)
        t_table = Table('monetdb_types', m, *columns)
        try:
            m.create_all()

            m2 = MetaData(db)
            rt = Table('monetdb_types', m2, autoload=True)
            try:
                db.execute('CREATE VIEW monetdb_types_v '
                           'AS SELECT * from monetdb_types')
                rv = Table('monetdb_types_v', m2, autoload=True)

                expected = [len(c) > 1 and c[1] or c[0] for c in specs]

                tables = rt, rv

                for table in tables:
                    for i, reflected in enumerate(table.c):
                        self.assertEqual(type(reflected.type), type(expected[i]), "%s reflection column failed"%reflected)
            finally:
                db.execute('DROP VIEW monetdb_types_v')
        finally:
            m.drop_all()


    #from sqlite tests....
    @testing.supported('monetdb')
    def test_date(self):
        meta = MetaData(testbase.db)
        t = Table('testdate', meta,
                  Column('id', Integer, primary_key=True),
                  Column('adate', Date), 
                  Column('adatetime', DateTime))
        meta.create_all()
        try:
            d1 = datetime.date(2007, 10, 30)
            d2 = datetime.datetime(2007, 10, 30)
            t.insert().execute(adate=str(d1), adatetime=str(d2))
            item = t.select().execute().fetchall()[0]
            self.assertEquals(item, (1, datetime.date(2007, 10, 30),
                                     datetime.datetime(2007, 10, 30)))
        finally:
            meta.drop_all()
            


    @testing.supported('monetdb')
    def te2st_autoincrement(self):
        """ignoring for now Mysql - specific?
        Need to check if our autoincrement is correct...
        """
        meta = MetaData(testbase.db)
        try:
            Table('ai_1', meta,
                  Column('int_y', Integer, primary_key=True),
                  Column('int_n', Integer, PassiveDefault('0'),
                         primary_key=True))
            Table('ai_2', meta,
                  Column('int_y', Integer, primary_key=True),
                  Column('int_n', Integer, PassiveDefault('0'),
                         primary_key=True))
            Table('ai_3', meta,
                  Column('int_n', Integer, PassiveDefault('0'),
                         primary_key=True, autoincrement=False),
                  Column('int_y', Integer, primary_key=True))
            Table('ai_4', meta,
                  Column('int_n', Integer, PassiveDefault('0'),
                         primary_key=True, autoincrement=False),
                  Column('int_n2', Integer, PassiveDefault('0'),
                         primary_key=True, autoincrement=False))
            Table('ai_5', meta,
                  Column('int_y', Integer, primary_key=True),
                  Column('int_n', Integer, PassiveDefault('0'),
                         primary_key=True, autoincrement=False))
            Table('ai_6', meta,
                  Column('o1', String(1), PassiveDefault('x'),
                         primary_key=True),
                  Column('int_y', Integer, primary_key=True))
            Table('ai_7', meta,
                  Column('o1', String(1), PassiveDefault('x'),
                         primary_key=True),
                  Column('o2', String(1), PassiveDefault('x'),
                         primary_key=True),
                  Column('int_y', Integer, primary_key=True))
            Table('ai_8', meta,
                  Column('o1', String(1), PassiveDefault('x'),
                         primary_key=True),
                  Column('o2', String(1), PassiveDefault('x'),
                         primary_key=True))
            meta.create_all()

            table_names = ['ai_1', 'ai_2', 'ai_3', 'ai_4',
                           'ai_5', 'ai_6', 'ai_7', 'ai_8']
            mr = MetaData(testbase.db)
            mr.reflect(only=table_names)

            for tbl in [mr.tables[name] for name in table_names]:
                for c in tbl.c:
                    if c.name.startswith('int_y'):
                        assert c.autoincrement
                    elif c.name.startswith('int_n'):
                        debug_c(c)
                        #assert not c.autoincrement
                        self.assertEquals(c.autoincrement, False,
                                          "Autoincremented column %s" % c)
                tbl.insert().execute()
                if 'int_y' in tbl.c:
                    assert select([tbl.c.int_y]).scalar() == 1
                    assert list(tbl.select().execute().fetchone()).count(1) == 1
                else:
                    assert 1 not in list(tbl.select().execute().fetchone())
        finally:
            meta.drop_all()

def debug_c(column):
    pk = column.primary_key
    default = column.default
    ctype = column.type
    auto = column.autoincrement
    print("PK", pk,"default",default,"ctype",ctype  ,"auto", auto)


if __name__ == "__main__":
    testbase.main()
