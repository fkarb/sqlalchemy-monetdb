from sqlalchemy.dialects import registry

registry.register("monetdb", "sqlalchemy_monetdb.dialect", "MonetDialect")

from sqlalchemy.testing.plugin.pytestplugin import *