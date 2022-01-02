from setuptools import setup

packages = \
    ['dbd',
     'dbd.cli',
     'dbd.config',
     'dbd.db',
     'dbd.executors',
     'dbd.generator',
     'dbd.log',
     'dbd.tasks',
     'dbd.utils']

package_data = \
    {'': ['*'],
     'dbd': ['resources/template/*', 'resources/template/model/*'],
     'dbd.generator': ['generator_templates/*']}

install_requires = \
    ['cerberus>=1.3.4,<2.0.0',
     'click>=8.0.3,<9.0.0',
     'fastparquet>=0.7.2,<0.8.0',
     'jinja2>=3.0.3,<4.0.0',
     'networkx>=2.6.3,<3.0.0',
     'openpyxl>=3.0.9,<4.0.0',
     'pandas>=1.3.5,<2.0.0',
     'psycopg2>=2.9.3,<3.0.0',
     'pyarrow>=6.0.0,<7.0.0',
     'pyyaml>=6.0,<7.0',
     'sql-metadata>=2.3.0,<3.0.0',
     'sqlalchemy>=1.4.29,<2.0.0',
     'sqlparse>=0.4.2,<0.5.0']

entry_points = \
    {'console_scripts': ['dbd = dbd.cli.dbdcli:cli']}

setup_kwargs = {
    'name': 'dbd',
    'version': '0.5.0',
    'description': 'Framework for declarative database creation and management.',
    'long_description': None,
    'author': 'zsvoboda',
    'author_email': 'zsvoboda@gmail.com',
    'maintainer': None,
    'maintainer_email': None,
    'url': None,
    'packages': packages,
    'package_data': package_data,
    'install_requires': install_requires,
    'entry_points': entry_points,
    'python_requires': '>=3.7,<4.0',
}

setup(**setup_kwargs)
