from distutils.core import setup

REQUIRES = ["jinja2==3.0.3", "sqlalchemy==1.4.25", "sqlparse==0.4.2", "sql-metadata==2.3.0", "networkx==2.6.3",
            "pyyaml==6.0", "pandas==1.3.5", "click==8.0.3", "cerberus==1.3.4", "python-dateutil==2.8.2",
            "openpyxl==3.0.9", "pyarrow==6.0.1", "requests==2.27.1", "fsspec==2022.1.0", "s3fs==2022.1.0",
            "kaggle==1.5.12",
            "psycopg2-binary==2.9.3", "PyMySQL==1.0.2", "greenlet==1.1.2",
            "snowflake-connector-python==2.7.3", "snowflake-sqlalchemy==1.3.3",
            "sqlalchemy-redshift==0.8.9",
            "google-cloud-storage==2.1.0", "pandas-gbq==0.17.0", "sqlalchemy-bigquery==1.3.0",
            "google-cloud-bigquery-storage==2.11.0"
            ]

setup(
    name="dbd",
    description="Declarative database",
    version="0.8.7",
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
