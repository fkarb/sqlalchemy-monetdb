from sqlalchemy import schema
from sqlalchemy import types as sql_types
from sqlalchemy.engine import default
from sqlalchemy.sql import compiler, operators

RESERVED_WORDS = {"asc", "action", "add", "admin", "after", "aggregate", "all", "alter", "always", "and", "any", "as",
                  "asymmetric", "atomic", "authorization", "autoincrement", "before", "begin", "between", "bigint",
                  "bigserial", "blob", "boolean", "by", "cache", "call", "cascade", "cascasde", "case", "char",
                  "character", "check", "clob", "cluster", "clustered", "column", "commit", "comparison", "constraint",
                  "copy", "create", "cross", "currentdate", "currentrole", "currenttime", "currenttimestamp",
                  "currentuser", "cycle", "data", "datetime", "day", "decimal", "declare", "default", "delete",
                  "delimiters", "distinct", "do", "double", "drop", "each", "else", "elseif", "encrypted", "end",
                  "escape", "execute", "exists", "external", "extract", "false", "for", "foreign", "from", "full",
                  "function", "generated", "global", "grant", "group", "having", "hour", "identity", "if", "in",
                  "increment", "index", "inner", "insert", "int", "interval ", "into", "is", "join", "key", "left",
                  "like", "limit", "local", "localtime", "localtimestamp", "lockedcopy", "match", "maxvalue", "minute",
                  "minvalue", "month", "name", "natural", "new", "no", "nomaxvalue", "nominvalue", "noncycle",
                  "noncyclecreate", "not", "null", "of", "offset", "old", "on", "only", "option", "options", "or",
                  "order", "outer", "overlaps", "partial", "password", "path", "position", "precision", "preferences",
                  "preserve", "primary", "privileges", "procedure", "public", "read", "real", "records", "references",
                  "referencing", "rename", "restart", "restrict", "return", "returns", "revoke", "right", "role",
                  "rollback", "row", "rows", "schema", "second", "select", "sequence", "serial", "session", "set",
                  "simple", "smallint", "some", "start", "statement", "stdin", "substring", "symmetric", "table",
                  "temporary", "then", "time", "timestamp", "tinyint", "to", "transaction", "trigger", "true", "type",
                  "unencrypted", "union", "unique", "unknown", "update", "user", "using", "values", "varchar", "view",
                  "when", "where", "while", "with", "year", "zone"}

OPERATORS = compiler.OPERATORS
OPERATORS[operators.ne] = ' <> '


class MonetExecutionContext(default.DefaultExecutionContext):
    def get_column_default(self, column, isinsert=True):
        if column.primary_key:
            # pre-execute passive defaults on primary keys
            if isinstance(column.default, schema.PassiveDefault):
                return self.execute_string("SELECT %s" % column.default.arg)
            elif isinstance(column.type, sql_types.Integer) and isinstance(column.default, schema.Sequence):
                exc = "SELECT NEXT VALUE FOR %s" \
                      % self.dialect.identifier_preparer.format_sequence(column.sequence)
                next_value = self.execute_string(exc)
                return next_value
        default_value = super(MonetExecutionContext, self).get_column_default(column)
        return default_value

    def fire_sequence(self, seq, type_):
        return self._execute_scalar(("SELECT NEXT VALUE FOR %s" %
                                     self.dialect.identifier_preparer.format_sequence(seq)), type_)


class MonetIdentifierPreparer(compiler.IdentifierPreparer):
    reserved_words = RESERVED_WORDS

    def __init__(self, *args, **kwargs):
        super(MonetIdentifierPreparer, self).__init__(*args, **kwargs)

        self._double_percents = False
