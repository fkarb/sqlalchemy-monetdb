"""
SQLAlchemy backend for MonetDB
"""

import pkg_resources

try:
    __version__ = pkg_resources.require("sqlalchemy-monetdb")[0].version
except pkg_resources.DistributionNotFound:
    __version__ = "dev"
