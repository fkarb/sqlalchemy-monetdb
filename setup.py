"""
Setup for SQLAlchemy backend for MonetDB
"""
from setuptools import find_packages, setup

setup_params = dict(
    name="sqlalchemy_monetdb",
    description="SQLAlchemy backend for MonetDB",
    packages=find_packages(),
    include_package_data=True,
    zip_safe=False,
    entry_points={
        "sqlalchemy.dialects" : ["monetdb = sqlalchemy_monetdb:dialect"]
    },
    license="MIT",
)

if __name__ == '__main__':
    setup(**setup_params)
