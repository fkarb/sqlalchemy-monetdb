from sqlalchemy.testing.requirements import SuiteRequirements, exclusions


class Requirements(SuiteRequirements):
    @property
    def schemas(self):
        return exclusions.open()
