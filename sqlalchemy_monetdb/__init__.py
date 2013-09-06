# MonetDB SQLAlchemy Dialect
#
# Copyright (C) 2007 Matt Harrison matthewharrison AT gmail.com
# Copyright (C) 2013
#
# This module maybe part of SQLAlchemy and is released under
# the MIT License: http://www.opensource.org/licenses/mit-license.php
"""
SQLAlchemy backend for MonetDB

http://www.monetdb.org/Documentation/SQLcatalog

MonetDB is similar to Oracle: case sensitive; unquoted identifiers are made lowercase; case is
preserved in quoted identifiers.

Todo/issues:
  * Autogeneration of primary keys doesn't work!
  * limit/offset - Need to get working with order by
    Offset not working, but supposed to be in monetdb
  * unicode
  * last_inserted_ids doesn't work for subselected params -
    Not currently supported at the cursor level
  * IN statement -
    select ... where name NOT IN ('matt', 'fred')
    will return null name
  * constraints don't support > operators
"""

import warnings
from sqlalchemy import schema, exc, pool, util
from sqlalchemy import types as sqltypes
from sqlalchemy.engine import default, reflection
from sqlalchemy.sql import compiler
from sqlalchemy.sql import expression as sql
from sqlalchemy.types import INTEGER, BIGINT, SMALLINT, VARCHAR, \
        CHAR, TEXT, FLOAT, NUMERIC, \
        DATE, BOOLEAN, DECIMAL, TIMESTAMP, TIME, BLOB


class INET(sqltypes.TypeEngine):
    __visit_name__ = "INET"
MDBInet = INET


class URL(sqltypes.TypeEngine):
    __visit_name__ = "URL"
MDBUrl = URL


class WRD(sqltypes.Integer):
    __visit_name__ = "WRD"
MDBWrd = WRD


class DOUBLE_PRECISION(sqltypes.Float):
    __visit_name__ = 'DOUBLE PRECISION'


# Map from MonetDB internal type name to SQLAlchemy type
MONETDB_TYPE_MAP = {
    # oid
    # ptr
    # timestamptz
    # timetz
    # tinyint
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

    def visit_column(self, column, result_map=None, **kwargs):
        # MonetDB does not currently support column references that include
        # a schema name. This could cause problems when selecting from two
        # identically named tables; this would have to be manually remedied
        # with table aliases.

        name = orig_name = column.name
        if name is None:
            raise exc.CompileError("Cannot compile Column object until "
                                   "it's 'name' is assigned.")

        is_literal = column.is_literal
        if not is_literal and isinstance(name, sql._truncated_label):
            name = self._truncated_identifier("colident", name)

        if result_map is not None:
            result_map[name.lower()] = (orig_name,
                                        (column, name, column.key),
                                        column.type)

        if is_literal:
            name = self.escape_literal_column(name)
        else:
            name = self.preparer.quote(name, column.quote)

        table = column.table
        if table is None or not table.named_with_column:
            return name
        else:
            tablename = table.name
            if isinstance(tablename, sql._truncated_label):
                tablename = self._truncated_identifier("alias", tablename)

            return self.preparer.quote(tablename, table.quote) + \
                "." + name

    def visit_extended_join(self, join, asfrom=False, **kwargs):
        """Support for full outer join, created by rb.data.sqlalchemy.ExtendedJoin"""

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

    def visit_drop_sequence(self, sequence):
        if not self.checkfirst or self.dialect.has_sequence(self.connection, sequence.name):
            self.append("DROP SEQUENCE %s" % self.preparer.format_sequence(sequence))
            self.execute()

    def visit_check_constraint(self, constraint):
        util.warn("Skipped unsupported check constraint %s" % constraint.name)


def use_sequence(column):
    """logic from postgres"""
    return (isinstance(column.type, sqltypes.Integer)
            and column.autoincrement
            and (column.default is None
                 or (isinstance(column.default, schema.Sequence)
                     and column.default.optional)))


class MDBExecutionContext(default.DefaultExecutionContext):
    def get_column_default(self, column, isinsert=True):
        """from postgres"""
        if column.primary_key:
            # pre-execute passive defaults on primary keys
            if isinstance(column.default, schema.PassiveDefault):
                return self.execute_string("SELECT %s" % column.default.arg)
            elif isinstance(column.type, sqltypes.Integer) and isinstance(column.default, schema.Sequence):
            #elif use_sequence(column):
                exc = "SELECT NEXT VALUE FOR %s" \
                      % self.dialect.identifier_preparer.format_sequence(column.sequence)
                next_value = self.execute_string(exc.encode(self.dialect.encoding))
                return next_value
        default_value = super(MDBDefaultRunner, self).get_column_default(column)
        return default_value

    def fire_sequence(self, seq, type_):
        return self._execute_scalar(("SELECT NEXT VALUE FOR %s" %
                                     self.dialect.identifier_preparer.format_sequence(seq)), _type)


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
        return ([], opts)

    def create_execution_context(self, *args, **kwargs):
        return MDBExecutionContext(self, *args, **kwargs)

    def get_table_names(self, connection, schema=None, **kw):
        """Return a list of table names for `schema`."""

        query = """SELECT name FROM sys.tables
               WHERE system = false AND type = 0 AND schema_id = %(schema_id)s"""
        return [row[0] for row in connection.execute(query, {
            "schema_id": self._schema_id(connection, schema)})]

    def has_table(self, connection, table_name, schema=None):
        return table_name in self.get_table_names(connection, schema)

    def has_sequence(self, connection, sequence_name, schema=None):
        cursor = connection.execute(
            """SELECT * FROM sys.sequences WHERE name = %(name)s AND schema_id = %(schema_id)s""",
                                    {"name": sequence_name.encode(self.encoding),
                                     "schema_id": self._schema_id(connection, schema)})
        return bool(cursor.first())

    @reflection.cache
    def _schema_id(self, connection, schema_name):
        """Fetch the id for schema"""

        if schema_name is None:
            schema_name = connection.execute("""SELECT current_schema""").scalar()

        query = """SELECT id FROM sys.schemas WHERE name = %(schema_name)s"""
        cursor = connection.execute(query, {"schema_name": schema_name})
        schema_id = cursor.scalar()
        if schema_id is None:
            raise exc.InvalidRequestError(schema_name)
        return schema_id

    @reflection.cache
    def _table_id(self, connection, table_name, schema_name=None):
        """Fetch the id for schema.table_name, defaulting to current schema if schema is None"""

        query = """
            SELECT id
            FROM sys.tables
            WHERE name = %(name)s
            AND schema_id = %(schema_id)s"""

        c = connection.execute(query, {"name": table_name,
                                       "schema_id": self._schema_id(connection, schema_name)})

        table_id = c.scalar()
        if table_id is None:
            raise exc.NoSuchTableError(table_name)

        return table_id

    def get_primary_keys(self, connection, table_name, schema=None, **kw):
        """Return information about primary keys in `table_name`.

        Given a :class:`.Connection`, a string
        `table_name`, and an optional string `schema`, return primary
        key information as a list of column names.

        """
        query = """
            SELECT objects.name
            FROM sys.keys
            JOIN sys.objects USING (id)
            WHERE keys.table_id = %(table_id)s
        """
        c = connection.execute(query,
                               {"table_id": self._table_id(connection, table_name, schema)})
        return [row.name for row in c]

    def get_columns(self, connection, table_name, schema=None, **kw):
        query = """
            SELECT id, name, type, "default", "null", type_digits, type_scale
            FROM sys.columns
            WHERE columns.table_id = %(table_id)s"""
        c = connection.execute(query, {"table_id": self._table_id(connection, table_name, schema)})

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
                warnings.warn(RuntimeWarning("Did not recognize type '%s' of column '%s', setting to nulltype" % (row.type, name)))
                col_type = sqltypes.NULLTYPE
            if col_type:
                col_type = col_type(*args)

            column = {
                "name": name,
                "type": col_type,
                "default": row.default,
                "autoincrement": False,  # XXX
                "nullable": row.null == "true"
                }

            result.append(column)
        return result

    def get_foreign_keys(self, connection, table_name, schema=None, **kw):
        """Return information about foreign_keys in `table_name`."""

        query = """
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
        c = connection.execute(query,
                               {"table_id": self._table_id(connection, table_name, schema)})

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
        query = """SELECT idxs.name, objects.name AS "column_name"
                   FROM sys.idxs
                   JOIN sys.objects USING (id)
                   WHERE table_id = %(table_id)s
                   ORDER BY idxs.name, objects.nr"""
        c = connection.execute(query, {
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

        s = """SELECT query FROM sys.tables
               WHERE type = 1
               AND name = %(name)s
               AND schema_id = %(schema_id)s"""
        return connection.execute(s, {
            "name": view_name, "schema_id": self._schema_id(connection, schema)}).scalar()

    def get_view_names(self, connection, schema=None, **kw):
        """Return a list of all view names available in the database.

        schema:
          Optional, retrieve names from a non-default schema.
        """

        s = """SELECT name
               FROM sys.tables
               WHERE type = 1
               AND schema_id = %(schema_id)s"""
        return [row[0] for row in connection.execute(s, {
            "schema_id": self._schema_id(connection, schema)})]

    def _get_default_schema_name(self, connection):
        """Return the string name of the currently selected schema from
        the given connection.

        This is used by the default implementation to populate the
        "default_schema_name" attribute and is called exactly
        once upon first connect.
        """

        return connection.execute("SELECT CURRENT_SCHEMA").scalar()


dialect = MDBDialect
