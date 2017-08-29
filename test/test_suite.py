import sqlalchemy as sa
from sqlalchemy.testing.suite import *
from sqlalchemy.testing.suite import ComponentReflectionTest as _ComponentReflectionTest
from sqlalchemy import inspect
from sqlalchemy.testing import eq_
from sqlalchemy import testing
from sqlalchemy.schema import DDL
from sqlalchemy import event


class ComponentReflectionTest(_ComponentReflectionTest):

    """
    # we override this test, since the default schema is "test_schema2" for us.
    @testing.provide_metadata
    def _test_get_foreign_keys(self, schema=None):
        meta = self.metadata
        users, addresses, dingalings = self.tables.users, \
                    self.tables.email_addresses, self.tables.dingalings
        insp = inspect(meta.bind)
        expected_schema = schema
        # users
        users_fkeys = insp.get_foreign_keys(users.name,
                                            schema=schema)
        fkey1 = users_fkeys[0]

        with testing.requires.named_constraints.fail_if():
            self.assert_(fkey1['name'] is not None)

        if expected_schema == None:
            expected_schema = testing.db.dialect.default_schema_name

        eq_(fkey1['referred_schema'], expected_schema)
        eq_(fkey1['referred_table'], users.name)
        eq_(fkey1['referred_columns'], ['user_id', ])
        if testing.requires.self_referential_foreign_keys.enabled:
            eq_(fkey1['constrained_columns'], ['parent_user_id'])

        #addresses
        addr_fkeys = insp.get_foreign_keys(addresses.name,
                                           schema=schema)
        fkey1 = addr_fkeys[0]

        with testing.requires.named_constraints.fail_if():
            self.assert_(fkey1['name'] is not None)

        eq_(fkey1['referred_schema'], expected_schema)
        eq_(fkey1['referred_table'], users.name)
        eq_(fkey1['referred_columns'], ['user_id', ])
        eq_(fkey1['constrained_columns'], ['remote_user_id'])
        """


    @classmethod
    def define_temp_tables(cls, metadata):
        kw = {'prefixes': ["TEMPORARY"], 'schema': 'tmp'}

        user_tmp = Table(
            "user_tmp", metadata,
            Column("id", sa.INT, primary_key=True),
            Column('name', sa.VARCHAR(50)),
            Column('foo', sa.INT),
            sa.UniqueConstraint('name', name='user_tmp_uq'),
            sa.Index("user_tmp_ix", "foo"),
            **kw
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