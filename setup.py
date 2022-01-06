from distutils.core import setup

REQUIRES = ["jinja2", "sqlalchemy", "sqlparse", "sql-metadata", "networkx", "psycopg2", "pyyaml", "pandas",
            "click", "cerberus", "openpyxl", "pyarrow", "PyMySQL", "snowflake-sqlalchemy", "sqlalchemy-bigquery",
            "sqlalchemy-redshift"]

setup(
    name="dbd",
    description="Declarative database",
    version="0.5.9",
    author="Zdenek Svoboda",
    license="BSD",
    install_requires=REQUIRES,
    package_dir={'dbd': 'dbd'},
    packages=['dbd', 'dbd.cli', 'dbd.config', 'dbd.db', 'dbd.executors', 'dbd.generator',
              'dbd.generator.generator_templates', 'dbd.log', 'dbd.resources', 'dbd.resources.template',
              'dbd.resources.template.model', 'dbd.tasks',
              'dbd.utils'],
    package_data={
        "": ["*.j2", "*.profile", "*.project", "*.csv", "*.ddl"]
    },
    include_package_data=True,
    python_requires=">=3.7.0",
    entry_points={
        'console_scripts': [
            'dbd = dbd.cli.dbdcli:cli',
        ],
    },
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Environment :: Console",
        "License :: OSI Approved :: BSD License",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Topic :: Database",
        "Topic :: Software Development",
        "Typing :: Typed",
    ],
    keywords=[
        "metadata",
        "postgresql",
        "database"
    ],
)
