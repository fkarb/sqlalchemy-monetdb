import sqlalchemy as sa
from sqlalchemy.testing.suite import *
from sqlalchemy.testing.suite import ComponentReflectionTest as _ComponentReflectionTest
from sqlalchemy.testing.suite import CompoundSelectTest as _CompoundSelectTest
from sqlalchemy import inspect
from sqlalchemy.testing import eq_
from sqlalchemy import testing
from sqlalchemy.schema import DDL
from sqlalchemy import event
from sqlalchemy import MetaData


class ComponentReflectionTest(_ComponentReflectionTest):
    @testing.requires.foreign_key_constraint_reflection
    def test_get_foreign_keys(self):
        """we use test_schema2 schema"""
        self._test_get_foreign_keys(schema='test_schema2')

    @testing.requires.temp_table_reflection
    def test_get_temp_table_columns(self):
        """In MonetDB temp tables have the tmp schema, so the name is tmp.user_tmp"""
        meta = MetaData(self.bind)
        user_tmp = self.tables['tmp.user_tmp']
        insp = inspect(meta.bind)
        cols = insp.get_columns('user_tmp', schema='tmp')
        self.assert_(len(cols) > 0, len(cols))

        for i, col in enumerate(user_tmp.columns):
            eq_(col.name, cols[i]['name'])

    @testing.requires.temp_table_reflection
    def test_get_temp_table_indexes(self):
        # TODO: it looks like MonetDB doesn't show index for temp tables (yet)
        pass

    @testing.requires.temp_table_reflection
    @testing.requires.unique_constraint_reflection
    def test_get_temp_table_unique_constraints(self):
        # TODO: it looks like MonetDB doesn't show constains for temp tables (yet)
        return

        insp = inspect(self.bind)
        reflected = insp.get_unique_constraints('user_tmp', schema='tmp')
        for refl in reflected:
            # Different dialects handle duplicate index and constraints
            # differently, so ignore this flag
            refl.pop('duplicates_index', None)
        eq_(reflected, [{'column_names': ['name'], 'name': 'user_tmp_uq'}])


    @classmethod
    def define_temp_tables(cls, metadata):
        user_tmp = Table(
            "user_tmp", metadata,
            Column("id", sa.INT, primary_key=True),
            Column('name', sa.VARCHAR(50)),
            Column('foo', sa.INT),
            sa.UniqueConstraint('name', name='user_tmp_uq'),
            sa.Index("user_tmp_ix", "foo"),
            prefixes=["TEMPORARY"],
            schema='tmp',
        )
        if testing.requires.view_reflection.enabled and \
                testing.requires.temporary_views.enabled:
            event.listen(
                user_tmp, "after_create",
                DDL("create view user_tmp_v as "
                    "select * from tmp.user_tmp")
            )
            event.listen(
                user_tmp, "before_drop",
                DDL("drop view user_tmp_v")
            )

    @classmethod
    def define_reflected_tables(cls, metadata, schema):
        if schema:
            schema_prefix = schema + "."
        else:
            schema_prefix = ""

        if testing.requires.self_referential_foreign_keys.enabled:
            users = Table('users', metadata,
                          Column('user_id', sa.INT, primary_key=True),
                          Column('test1', sa.CHAR(5), nullable=False),
                          Column('test2', sa.Float(5), nullable=False),
                          Column('parent_user_id', sa.Integer,
                                 sa.ForeignKey('%susers.user_id' %
                                               schema_prefix)),
                          schema=schema,
                          test_needs_fk=True,
                          )
        else:
            users = Table('users', metadata,
                          Column('user_id', sa.INT, primary_key=True),
                          Column('test1', sa.CHAR(5), nullable=False),
                          Column('test2', sa.Float(5), nullable=False),
                          schema=schema,
                          test_needs_fk=True,
                          )

        # somehow the dependencies are not properly resolved, so i modified
        # the standard test to have a reference to the address_id column
        mail = Table('email_addresses', metadata,
              Column('address_id', sa.Integer),
              Column('remote_user_id', sa.Integer,
                     sa.ForeignKey(users.c.user_id)),
              Column('email_address', sa.String(20)),
              sa.PrimaryKeyConstraint('address_id', name='email_ad_pk'),
              schema=schema,
              test_needs_fk=True,
              )


        Table("dingalings", metadata,
              Column('dingaling_id', sa.Integer, primary_key=True),
              Column('address_id', sa.Integer,
                     sa.ForeignKey(mail.c.address_id)),
              Column('data', sa.String(30)),
              schema=schema,
              test_needs_fk=True,
              )

        if testing.requires.index_reflection.enabled:
            cls.define_index(metadata, users)
        if testing.requires.view_column_reflection.enabled:
            cls.define_views(metadata, schema)
        if not schema and testing.requires.temp_table_reflection.enabled:
            cls.define_temp_tables(metadata)


class CompoundSelectTest(_CompoundSelectTest):
    """
    Disable tests here since we don't support order by within unions

    https://www.monetdb.org/bugzilla/show_bug.cgi?id=6434
    """

    def test_distinct_selectable_in_unions(self):
        pass

    def test_limit_offset_aliased_selectable_in_unions(self):
        pass