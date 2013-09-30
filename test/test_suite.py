from sqlalchemy.testing.suite import *

from sqlalchemy import inspect
from sqlalchemy.testing import eq_
from sqlalchemy import testing

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

ComponentReflectionTest._test_get_foreign_keys = _test_get_foreign_keys