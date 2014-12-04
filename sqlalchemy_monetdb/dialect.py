import re
from sqlalchemy import pool, exc, util
from sqlalchemy.engine import default, reflection
from sqlalchemy_monetdb.base import MonetExecutionContext,\
    MonetIdentifierPreparer
from sqlalchemy_monetdb.types import MONETDB_TYPE_MAP
from sqlalchemy_monetdb.compiler import MonetDDLCompiler, MonetTypeCompiler,\
    MonetCompiler


class MonetDialect(default.DefaultDialect):
    name = "monetdb"
    driver = "monetdb"
    preexecute_pk_sequences = True
    supports_pk_autoincrement = True
    supports_sequences = True
    sequences_optional = True
    supports_native_decimal = True
    supports_default_values = True
    supports_native_boolean = True
    poolclass = pool.SingletonThreadPool

    statement_compiler = MonetCompiler
    ddl_compiler = MonetDDLCompiler
    execution_ctx_cls = MonetExecutionContext
    preparer = MonetIdentifierPreparer
    type_compiler = MonetTypeCompiler
    default_paramstyle = 'pyformat'

    def __init__(self, **kwargs):
        default.DefaultDialect.__init__(self, **kwargs)

    @classmethod
    def dbapi(cls):
        return __import__("monetdb.sql", fromlist="sql")

    def create_connect_args(self, url):
        opts = url.translate_connect_args()
        return [], opts

    def create_execution_context(self, *args, **kwargs):
        return MonetExecutionContext(self, *args, **kwargs)

    def get_table_names(self, connection, schema=None, **kw):
        """Return a list of table names for `schema`."""

        q = """
            SELECT name
            FROM sys.tables
            WHERE system = false
            AND type = 0
            AND schema_id = %(schema_id)s
        """
        args = {"schema_id": self._schema_id(connection, schema)}
        return [row[0] for row in connection.execute(q, args)]

    def has_table(self, connection, table_name, schema=None):
        return table_name in self.get_table_names(connection, schema)

    def has_sequence(self, connection, sequence_name, schema=None):
        q = """
            SELECT id
            FROM sys.sequences
            WHERE name = %(name)s
            AND schema_id = %(schema_id)s
        """
        args = {
            "name": sequence_name,
            "schema_id": self._schema_id(connection, schema)
        }
        cursor = connection.execute(q, args)
        return bool(cursor.first())

    @reflection.cache
    def _schema_id(self, connection, schema_name):
        """Fetch the id for schema"""

        if schema_name is None:
            schema_name = connection.execute("SELECT current_schema").scalar()

        query = """
                    SELECT id
                    FROM sys.schemas
                    WHERE name = %(schema_name)s
                """
        args = {"schema_name": schema_name}
        cursor = connection.execute(query, args)
        schema_id = cursor.scalar()
        if schema_id is None:
            raise exc.InvalidRequestError(schema_name)
        return schema_id

    @reflection.cache
    def _table_id(self, connection, table_name, schema_name=None):
        """Fetch the id for schema.table_name, defaulting to current schema if
        schema is None
        """
        q = """
            SELECT id
            FROM sys.tables
            WHERE name = %(name)s
            AND schema_id = %(schema_id)s
        """
        args = {
            "name": table_name,
            "schema_id": self._schema_id(connection, schema_name)
        }
        c = connection.execute(q, args)

        table_id = c.scalar()
        if table_id is None:
            raise exc.NoSuchTableError(table_name)

        return table_id


    def get_columns(self, connection, table_name, schema=None, **kw):
        q = """
            SELECT id, name, type, "default", "null", type_digits, type_scale
            FROM sys.columns
            WHERE columns.table_id = %(table_id)s
        """
        args = {"table_id": self._table_id(connection, table_name, schema)}
        c = connection.execute(q, args)

        result = []
        for row in c:
            args = ()
            name = row.name
            if row.type in ("char", "varchar"):
                args = (row.type_digits,)
            elif row.type == "decimal":
                args = (row.type_digits, row.type_scale)
            col_type = MONETDB_TYPE_MAP.get(row.type, None)
            col_type = col_type(*args)

            # monetdb translates an AUTO INCREMENT into a sequence
            autoincrement = False
            if row.default is not None:
                r = r"""next value for \"(\w*)\"\.\"(\w*)"$"""
                match = re.search(r, row.default)
                if match is not None:
                    seq_schema = match.group(1)
                    seq = match.group(2)
                    autoincrement = True

            column = {
                "name": name,
                "type": col_type,
                "default": row.default,
                "autoincrement": autoincrement,
                "nullable": row.null,
                }

            result.append(column)
        return result

    def get_foreign_keys(self, connection, table_name, schema=None, **kw):
        """Return information about foreign_keys in `table_name`.

        Given a string `table_name`, and an optional string `schema`, return
        foreign key information as a list of dicts with these keys:

        constrained_columns
          a list of column names that make up the foreign key

        referred_schema
          the name of the referred schema

        referred_table
          the name of the referred table

        referred_columns
          a list of column names in the referred table that correspond to
          constrained_columns

        name
          optional name of the foreign key constraint.

        \**kw
          other options passed to the dialect's get_foreign_keys() method.

        """

        q = """
            SELECT
            fkkey.name AS name,
            fkschema.name AS fktable_schema,
            fktable.name AS fktable_name,
            fkkeycol.name AS fkcolumn_name,
            fktable.id AS fktable_id,
            pkschema.name AS pktable_schema,
            pktable.name AS pktable_name,
            pkkeycol.name AS pkcolumn_name,
            pktable.id AS pktable_id,
            pkkeycol.nr AS key_seq
            FROM sys.keys AS fkkey
            JOIN sys.tables AS fktable ON (fktable.id = fkkey.table_id)
            JOIN sys.objects AS fkkeycol ON (fkkey.id = fkkeycol.id)
            JOIN sys.keys AS pkkey ON (fkkey.rkey = pkkey.id)
            JOIN sys.objects AS pkkeycol ON (pkkey.id = pkkeycol.id)
            JOIN sys.tables AS pktable ON (pktable.id = pkkey.table_id)
            JOIN sys.schemas AS fkschema ON (fkschema.id = fktable.schema_id)
            JOIN sys.schemas AS pkschema ON (pkschema.id = pktable.schema_id)
            JOIN sys.tables AS pktable ON (pktable.id = pkkey.table_id)
            WHERE fkkey.rkey > -1
              AND fkkeycol.nr = pkkeycol.nr
              AND fktable.id = %(table_id)s
            ORDER BY name, key_seq
        """
        args = {"table_id": self._table_id(connection, table_name, schema)}
        c = connection.execute(q, args)

        results = []
        key_data = {}
        constrained_columns = []
        referred_columns = []
        last_name = None

        for row in c:
            if last_name is not None and last_name != row.name:
                key_data["constrained_columns"] = constrained_columns
                key_data["referred_columns"] = referred_columns
                results.append(key_data)
                constrained_columns = []
                referred_columns = []

            if last_name is None or last_name != row.name:
                key_data = {
                    "name": row.name,
                    "referred_schema": row.pktable_schema,
                    "referred_table": row.pktable_name,
                }

            last_name = row.name
            constrained_columns.append(row.fkcolumn_name)
            referred_columns.append(row.pkcolumn_name)

        if key_data:
            key_data["constrained_columns"] = constrained_columns
            key_data["referred_columns"] = referred_columns
            results.append(key_data)

        return results

    def get_indexes(self, connection, table_name, schema=None, **kw):
        q = """
            SELECT idxs.name, objects.name AS "column_name"
            FROM sys.idxs
            JOIN sys.objects USING (id)
            WHERE table_id = %(table_id)s
            ORDER BY idxs.name, objects.nr
        """
        c = connection.execute(q, {
            "table_id": self._table_id(connection, table_name, schema)})

        results = []
        last_name = None
        column_names = []
        index_data = {}

        for row in c:
            if last_name is not None and last_name != row.name:
                index_data["column_names"] = column_names
                results.append(index_data)
                column_names = []

            if last_name is None or last_name != row.name:
                index_data = {
                    "name": row.name,
                    "unique": False,
                }

            last_name = row.name
            column_names.append(row.column_name)

        if index_data:
            index_data["column_names"] = column_names
            results.append(index_data)

        return results

    def do_commit(self, connection):
        connection.commit()

    def do_rollback(self, connection):
        connection.rollback()

    @reflection.cache
    def get_schema_names(self, connection, **kw):
        s = """
                SELECT name FROM sys.schemas ORDER BY name
            """
        c = connection.execute(s)
        schema_names = [row[0] for row in c]
        return schema_names

    def get_view_definition(self, connection, view_name, schema=None, **kw):
        """Return view definition.

        Given a :class:`.Connection`, a string
        `view_name`, and an optional string `schema`, return the view
        definition.
        """

        q = """
            SELECT query FROM sys.tables
            WHERE type = 1
            AND name = %(name)s
            AND schema_id = %(schema_id)s
        """
        args = {
            "name": view_name,
            "schema_id": self._schema_id(connection, schema)
        }
        return connection.execute(q, args)


    def get_view_names(self, connection, schema=None, **kw):
        """Return a list of all view names available in the database.

        schema:
          Optional, retrieve names from a non-default schema.
        """
        q = """
            SELECT name
            FROM sys.tables
            WHERE type = 1
            AND schema_id = %(schema_id)s
        """
        args = {"schema_id": self._schema_id(connection, schema)}
        return [row[0] for row in connection.execute(q, args)]

    def _get_default_schema_name(self, connection):
        """Return the string name of the currently selected schema from
        the given connection.

        This is used by the default implementation to populate the
        "default_schema_name" attribute and is called exactly
        once upon first connect.
        """
        return connection.execute("SELECT CURRENT_SCHEMA").scalar()

    def get_pk_constraint(self, connection, table_name, schema=None, **kw):
        """Return information about primary key constraint on `table_name`.

        Given a string `table_name`, and an optional string `schema`, return
        primary key information as a dictionary with these keys:

        constrained_columns
          a list of column names that make up the primary key

        name
          optional name of the primary key constraint.

        """
        q = """
        SELECT "objects"."name" AS col, keys.name as name
                 FROM "sys"."keys" AS "keys",
                         "sys"."objects" AS "objects",
                         "sys"."tables" AS "tables",
                         "sys"."schemas" AS "schemas"
                 WHERE "keys"."id" = "objects"."id"
                         AND "keys"."table_id" = "tables"."id"
                         AND "tables"."schema_id" = "schemas"."id"
                         AND "keys"."type" = 0
                         AND "tables"."id" = %(table_id)s
        """
        args = {"table_id": self._table_id(connection, table_name, schema)}
        c = connection.execute(q, args)
        table = c.fetchall()
        if table:
            cols = [c[0] for c in table]
            name = table[0][1]
            return {'constrained_columns': cols, 'name': name}
        else:
            return {}


    def get_unique_constraints(self, connection, table_name, schema=None, **kw):
        """Return information about unique constraints in `table_name`.

        Given a string `table_name` and an optional string `schema`, return
        unique constraint information as a list of dicts with these keys:

        name
          the unique constraint's name

        column_names
          list of column names in order

        \**kw
          other options passed to the dialect's get_unique_constraints() method.

        .. versionadded:: 0.9.0

        """
        q = """
        SELECT "objects"."name" AS col, keys.name as name
                 FROM "sys"."keys" AS "keys",
                         "sys"."objects" AS "objects",
                         "sys"."tables" AS "tables",
                         "sys"."schemas" AS "schemas"
                 WHERE "keys"."id" = "objects"."id"
                         AND "keys"."table_id" = "tables"."id"
                         AND "tables"."schema_id" = "schemas"."id"
                         AND "keys"."type" = 1
                         AND "tables"."id" = %(table_id)s
        """
        args = {"table_id": self._table_id(connection, table_name, schema)}
        c = connection.execute(q, args)
        table = c.fetchall()

        from collections import defaultdict

        col_dict = defaultdict(list)
        for col, name in table:
            col_dict[name].append(col)

        return [{'name': n, 'column_names': c} for n, c in col_dict.items()]
