import re
from sqlalchemy import pool, exc
from sqlalchemy.engine import default, reflection
from sqlalchemy import schema, util
from sqlalchemy import types as sqltypes
from sqlalchemy.sql import compiler
from sqlalchemy.types import INTEGER, BIGINT, SMALLINT, VARCHAR, CHAR, TEXT,\
    FLOAT, DATE, BOOLEAN, DECIMAL, TIMESTAMP, TIME, BLOB


class INET(sqltypes.TypeEngine):
    __visit_name__ = "INET"


class URL(sqltypes.TypeEngine):
    __visit_name__ = "URL"


class WRD(sqltypes.Integer):
    __visit_name__ = "WRD"


class DOUBLE_PRECISION(sqltypes.Float):
    __visit_name__ = 'DOUBLE PRECISION'


class TINYINT(sqltypes.Integer):
    __visit_name__ = "TINYINT"


MONETDB_TYPE_MAP = {
    'tinyint': TINYINT,
    'wrd': WRD,
    'url': URL,
    'inet': INET,
    'bigint': BIGINT,
    'blob': BLOB,
    'boolean': BOOLEAN,
    'char': CHAR,
    'clob': TEXT,
    'date': DATE,
    'decimal': DECIMAL,
    'double': DOUBLE_PRECISION,
    'int': INTEGER,
    'real': FLOAT,
    'smallint': SMALLINT,
    'time': TIME,
    'timestamp': TIMESTAMP,
    'varchar': VARCHAR,
}


class MDBCompiler(compiler.SQLCompiler):
    def visit_mod(self, binary, **kw):
        return self.process(binary.left) + " %% " + self.process(binary.right)

    def visit_sequence(self, seq):
        exc = "(SELECT NEXT VALUE FOR %s)" \
              % self.dialect.identifier_preparer.format_sequence(seq)
        return exc

    def limit_clause(self, select):
        text = ""
        if select._limit is not None:
            text +=  "\nLIMIT " + str(select._limit)
        if select._offset is not None:
            text += " OFFSET " + str(select._offset)
        return text

    def visit_extended_join(self, join, asfrom=False, **kwargs):
        """Support for full outer join, created by
        rb.data.sqlalchemy.ExtendedJoin
        """

        if join.isouter and join.isfullouter:
            join_type = " FULL OUTER JOIN "
        elif join.isouter:
            join_type = " LEFT OUTER JOIN "
        else:
            join_type = " JOIN "

        return (
            join.left._compiler_dispatch(self, asfrom=True, **kwargs) +
            join_type +
            join.right._compiler_dispatch(self, asfrom=True, **kwargs) +
            " ON " +
            join.onclause._compiler_dispatch(self, **kwargs)
        )

    def visit_ne(self, element, **kwargs):
        return (
            element.left._compiler_dispatch(self, **kwargs) +
            " <> " +
            element.right._compiler_dispatch(self, **kwargs))


class MDBDDLCompiler(compiler.DDLCompiler):
    def visit_create_sequence(self, create):
        text = "CREATE SEQUENCE %s AS INTEGER" % \
                self.preparer.format_sequence(create.element)
        if create.element.start is not None:
            text += " START WITH %d" % create.element.start
        if create.element.increment is not None:
            text += " INCREMENT BY %d" % create.element.increment
        return text

    def visit_drop_sequence(self, drop):
        return "DROP SEQUENCE %s" % \
                self.preparer.format_sequence(drop.element)

    def get_column_specification(self, column, **kwargs):
        colspec = self.preparer.format_column(column)
        impl_type = column.type.dialect_impl(self.dialect)
        if column.primary_key and \
            column is column.table._autoincrement_column and \
            not isinstance(impl_type, sqltypes.SmallInteger) and \
            (
                column.default is None or
                (
                    isinstance(column.default, schema.Sequence) and
                    column.default.optional
                )):
            colspec += " INT AUTO_INCREMENT"
        else:
            colspec += " " + self.dialect.type_compiler.process(column.type)
            default = self.get_column_default_string(column)
            if default is not None:
                colspec += " DEFAULT " + default

        if not column.nullable:
            colspec += " NOT NULL"
        return colspec

    def visit_check_constraint(self, constraint):
        util.warn("Skipped unsupported check constraint %s" % constraint.name)


class MDBExecutionContext(default.DefaultExecutionContext):
    def get_column_default(self, column, isinsert=True):
        if column.primary_key:
            # pre-execute passive defaults on primary keys
            if isinstance(column.default, schema.PassiveDefault):
                return self.execute_string("SELECT %s" % column.default.arg)
            elif isinstance(column.type, sqltypes.Integer) and isinstance(column.default, schema.Sequence):
                exc = "SELECT NEXT VALUE FOR %s" \
                      % self.dialect.identifier_preparer.format_sequence(column.sequence)
                next_value = self.execute_string(exc.encode(self.dialect.encoding))
                return next_value
        default_value = super(MDBExecutionContext, self).get_column_default(column)
        return default_value

    def fire_sequence(self, seq, type_):
        return self._execute_scalar(("SELECT NEXT VALUE FOR %s" %
                                     self.dialect.identifier_preparer.format_sequence(seq)), type_)


RESERVED_WORDS = set(
    ["action", "add", "admin", "after", "aggregate", "all", "alter",
     "always", "and", "any", "as", "asymmetric", "atomic", "authorization",
     "autoincrement", "before", "begin", "between", "bigint", "bigserial",
     "blob", "boolean", "by", "cache", "call", "cascade", "cascasde", "case",
     "char", "character", "check", "clob", "cluster", "clustered", "column",
     "commit", "comparison", "constraint", "copy", "create", "cross",
     "currentdate", "currentrole", "currenttime", "currenttimestamp",
     "currentuser", "cycle", "data", "datetime", "day", "decimal", "declare",
     "default", "delete", "delimiters", "distinct", "do", "double", "drop",
     "each", "else", "elseif", "encrypted", "end", "escape", "execute",
     "exists", "external", "extract", "false", "for", "foreign", "from",
     "full", "function", "generated", "global", "grant", "group", "having",
     "hour", "identity", "if", "in", "increment", "index", "inner", "insert",
     "int", "interval ", "into", "is", "join", "key", "left", "like", "limit",
     "local", "localtime", "localtimestamp", "lockedcopy", "match",
     "maxvalue", "minute", "minvalue", "month", "name", "natural", "new",
     "no", "nomaxvalue", "nominvalue", "noncycle", "noncyclecreate", "not",
     "null", "of", "offset", "old", "on", "only", "option", "options", "or",
     "order", "outer", "overlaps", "partial", "password", "path",
     "position", "precision", "preferences", "preserve", "primary", "privileges",
     "procedure", "public", "read", "real", "records", "references",
     "referencing", "rename", "restart", "restrict", "return", "returns",
     "revoke", "right", "role", "rollback", "row", "rows", "schema", "second",
     "select", "sequence", "serial", "session", "set", "simple", "smallint",
     "some", "start", "statement", "stdin", "substring", "symmetric", "table",
     "temporary", "then", "time", "timestamp", "tinyint", "to", "transaction",
     "trigger", "true", "type", "unencrypted", "union", "unique", "unknown",
     "update", "user", "using", "values", "varchar", "view", "when", "where",
     "while", "with", "year", "zone"])


class MDBTypeCompiler(compiler.GenericTypeCompiler):
    def visit_DOUBLE_PRECISION(self, type_):
        return "DOUBLE PRECISION"

    def visit_INET(self, type_):
        return "INET"

    def visit_URL(self, type_):
        return "URL"

    def visit_WRD(self, type_):
        return "WRD"

    def visit_datetime(self, type_):
        return self.visit_TIMESTAMP(type_)

    def visit_TIMESTAMP(self, type_):
        if type_.timezone:
            return "TIMESTAMP WITH TIME ZONE"
        return "TIMESTAMP"

    def visit_VARCHAR(self, type_):
        if type_.length is None:
            return "CLOB"
        return compiler.GenericTypeCompiler.visit_VARCHAR(self, type_)


class MDBIdentifierPreparer(compiler.IdentifierPreparer):
    reserved_words = RESERVED_WORDS
    def __init__(self, *args, **kwargs):
        super(MDBIdentifierPreparer, self).__init__(*args, **kwargs)


class MDBDialect(default.DefaultDialect):
    name = "monetdb"
    # preexecute_pk_sequences = True
    # supports_pk_autoincrement = False #setting to False for prefetch...
    supports_sequences = True
    # sequences_optional = True  -- check
    supports_native_decimal = True
    supports_default_values = True
    supports_native_boolean = True
    poolclass = pool.SingletonThreadPool

    statement_compiler = MDBCompiler
    ddl_compiler = MDBDDLCompiler
    execution_ctx_cls = MDBExecutionContext
    preparer = MDBIdentifierPreparer
    type_compiler = MDBTypeCompiler
    default_paramstyle = 'pyformat'

    def __init__(self, **kwargs):
        default.DefaultDialect.__init__(self, **kwargs)

    @classmethod
    def dbapi(cls):
        monetdbsql = __import__("monetdb.sql", fromlist="sql")
        return monetdbsql

    def create_connect_args(self, url):
        opts = url.translate_connect_args()
        return [], opts

    def create_execution_context(self, *args, **kwargs):
        return MDBExecutionContext(self, *args, **kwargs)

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
            "name": sequence_name.encode(self.encoding),
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
            if col_type is None:
                msg = "Did not recognize type '%s' of column '%s'," \
                      " setting to nulltype" % (row.type, name)
                util.warn(msg)
                col_type = sqltypes.NULLTYPE
            if col_type:
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
                "nullable": row.null == "true"
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
        return connection.execute(q, args).scalar()

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
        cols = [c[0] for c in table]
        name = table[0][1]
        return {'constrained_columns': cols, 'name': name}