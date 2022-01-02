from jinja2 import Environment, PackageLoader
from sqlalchemy import PrimaryKeyConstraint, ForeignKeyConstraint, CheckConstraint, UniqueConstraint

import dbd
from dbd.utils.text_utils import strip_table_name

JINJA_GENERATOR_ENV = Environment(loader=PackageLoader(dbd.__name__, 'generator/generator_templates'))
JINJA_GENERATOR_ENV.globals.update(
    len=len, str=str, type=type, isinstance=isinstance,
    strip_table_name=strip_table_name,
    PrimaryKeyConstraint=PrimaryKeyConstraint,
    ForeignKeyConstraint=ForeignKeyConstraint,
    CheckConstraint=CheckConstraint,
    UniqueConstraint=UniqueConstraint
)
