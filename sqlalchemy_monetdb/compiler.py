from sqlalchemy import types as sqltypes, schema, util
from sqlalchemy.sql import compiler


class MonetDDLCompiler(compiler.DDLCompiler):
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


class MonetTypeCompiler(compiler.GenericTypeCompiler):
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


class MonetCompiler(compiler.SQLCompiler):
    def visit_mod(self, binary, **kw):
        return self.process(binary.left) + " %% " + self.process(binary.right)

    def visit_sequence(self, seq):
        exc = "(NEXT VALUE FOR %s)" \
              % self.dialect.identifier_preparer.format_sequence(seq)
        return exc

    def limit_clause(self, select, **kw):
        text = ""
        if select._limit is not None:
            text += "\nLIMIT " + str(select._limit)
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

    def render_literal_value(self, value, type_):
        # we need to escape backslashes
        value = super(MonetCompiler, self).render_literal_value(value, type_)
        return value.replace('\\', '\\\\')
