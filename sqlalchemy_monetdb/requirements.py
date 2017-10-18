from sqlalchemy.testing.requirements import SuiteRequirements, exclusions


class Requirements(SuiteRequirements):
    @property
    def schemas(self):
        return exclusions.open()

    @property
    def sequences(self):
        return exclusions.open()

    @property
    def reflects_pk_names(self):
        return exclusions.open()

    @property
    def unicode_ddl(self):
        return exclusions.open()

    @property
    def datetime_microseconds(self):
        """monetdb doesn't support microsecond resolution"""
        return exclusions.closed()
    time_microseconds = datetime_microseconds

    @property
    def datetime_historic(self):
        return exclusions.open()

    @property
    def date_historic(self):
        return exclusions.open()

    @property
    def precision_numerics_enotation_small(self):
        return exclusions.open()

    @property
    def precision_numerics_enotation_large(self):
        """We don't support Numeric > 18"""
        return exclusions.closed()

    @property
    def view_reflection(self):
        return exclusions.open()

    @property
    def dbapi_lastrowid(self):
        return exclusions.open()

    @property
    def precision_numerics_retains_significant_digits(self):
        return exclusions.open()

    @property
    def sequences_optional(self):
        return exclusions.open()

    @property
    def independent_connections(self):
        return exclusions.open()

    @property
    def temp_table_names(self):
        """target dialect supports listing of temporary table names"""
        return exclusions.open()

    @property
    def temporary_tables(self):
        """target database supports temporary tables"""
        return exclusions.open()

    @property
    def temporary_views(self):
        """
        MonetDB doesn't have temporary views, but does support views of temporary tables.

        TODO: disabled for now, but maybe we can enable this?
        """
        return exclusions.closed()


    @property
    def foreign_key_constraint_option_reflection(self):
        """TODO: PostgreSQL, MonetDB and sqlite support this, so probably MoentDB also"""
        return exclusions.closed()

    @property
    def views(self):
        """Target database must support VIEWs."""

        return exclusions.open()

    @property
    def temp_table_reflection(self):
        return exclusions.open()

    @property
    def bound_limit_offset(self):
        """target database can render LIMIT and/or OFFSET using a bound
        parameter

        open by default, but closed since raises:

        sqlalchemy.exc.CompileError: This SELECT structure does not use a simple integer value for offset
        """
        return exclusions.closed()