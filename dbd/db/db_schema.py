from typing import TypeVar, List, Any, Dict, Tuple

import sqlalchemy
from cerberus import Validator
from sqlalchemy import MetaData

from dbd.db.db_table import DbTable
from dbd.generator.jinja_generator_env import JINJA_GENERATOR_ENV

DbSchemaType = TypeVar('DbSchemaType', bound='DbSchema')


class DbSchema:
    """
    Database schema definition
    """

    def __init__(self, name: str, tables: List[DbTable], alchemy_metadata: sqlalchemy.MetaData):
        """
        DbSchema constructor
        :param str name: schema name
        :param List[DbTable] tables: DbTable[] schema tables
        :param sqlalchemy.MetaData alchemy_metadata: MetaData underlying SQLAlchemy MetaData
        """
        self.__name = name
        self.__alchemy_metadata = alchemy_metadata
        self.__tables = tables

    def __eq__(self, other: DbSchemaType) -> bool:
        """
        Comparator
        :param DbSchema other: another DbSchema instance to compare this instance to
        :return: True if both DbSchemas are equal, False otherwise
        :rtype: bool
        """
        if isinstance(other, DbSchema):
            my_tables = self.tables()
            other_tables = other.tables()
            for my_table in my_tables:
                other_table = [t for t in other_tables if t.name() == my_table.name()]
                if len(other_table) != 1 or my_table != other_table[0]:
                    return False
            return True
        return False

    def tables(self) -> List[DbTable]:
        """
        :return: DbTable[] array of tables
        :type: List[DbTable]
        """
        return self.__tables

    def table(self, name: str) -> DbTable:
        """
        Returns table by name
        :param name: table name
        :return: table (None if a table with such name doesn't exist)
        :rtype: DbTable
        """
        table_matches = [t for t in self.__tables if name == t.name()]
        return table_matches[0] if len(table_matches) == 1 else None

    def alchemy_metadata(self) -> sqlalchemy.MetaData:
        """
        :return: MetaData SQLAlchemy metadata
        :rtype: sqlalchemy.MetaData
        """
        return self.__alchemy_metadata

    def name(self) -> str:
        """
        :return: schema name
        :rtype: str
        """
        return self.__name

    def create(self):
        """
        TODO: Exception handling
        Generates SQL and creates all database objects (tables)
        :return: SQLAlchemy status
        """
        self.__alchemy_metadata.create_all()

    def code(self) -> str:
        """
        TODO: shall I turn this to JSON and to dict here?
        :return: DbSchema's code
        :rtype: str
        """
        template = JINJA_GENERATOR_ENV.get_template('schema.j2')
        return template.render(dict(sn=self.__name, ts=[t.alchemy_table() for t in self.__tables]))

    @classmethod
    def from_alchemy_engine(cls, schema_name: str, alchemy_engine: sqlalchemy.engine.Engine) -> DbSchemaType:
        """
        Creates a new schema from SQLAlchemy engine
        :param str schema_name: schema name
        :param sqlalchemy.engine.Engine alchemy_engine: SQLAlchemy engine
        :return: new DbSchema instance
        :rtype: DbSchema
        """
        alchemy_metadata = MetaData(bind=alchemy_engine, schema=schema_name)
        alchemy_metadata.reflect(views=True)
        return DbSchema(schema_name, [DbTable.from_alchemy_table(t) for t in alchemy_metadata.tables.values()],
                        alchemy_metadata)

    @classmethod
    def from_code(cls, schema_code: Dict[str, Any], alchemy_engine: sqlalchemy.engine.Engine) -> DbSchemaType:
        """
        Creates a new schema from schema code
        :param Dict[str, Any] schema_code: schema code
        :param sqlalchemy.engine.Engine alchemy_engine: Engine SQLAlchemy engine
        :return: new DbSchema instance
        :rtype: DbSchema
        """
        schema_name = schema_code.get('name', None)
        alchemy_metadata = MetaData(bind=alchemy_engine, schema=schema_name)
        alchemy_metadata.reflect(views=True)
        tables = []
        for t in [t for t in schema_code['tables'].items()]:
            table_name, table_code = t
            tables.append(DbTable.from_code(table_name, table_code, alchemy_metadata, schema=schema_name))
        return DbSchema(schema_name, tables, alchemy_metadata)

    @classmethod
    def validate_code(cls, schema_code: Dict[str, Any]) -> Tuple[bool, Dict[str, Any]]:
        """
        Validates schema  code
        :param Dict[str, Any] schema_code: schema code
        :return: True if schema is valid, False otherwise and Dict of errors
        :rtype: Tuple[bool, Dict[str, Any]]
        """
        schema_errors = {}
        schema_validator = Validator(
            {
                'tables': {'type': 'dict', 'required': True}
            })
        validation_result = schema_validator.validate(schema_code)
        if not validation_result:
            schema_errors['structure'] = schema_validator.errors
        tables_errors = []
        for t in [t for t in schema_code['tables'].items()]:
            table_name, table_code = t
            table_validation_result, table_validation_errors = DbTable.validate_code(table_name, table_code)
            if not table_validation_result:
                tables_errors.append({table_name: table_validation_errors})
                validation_result = False
        if len(tables_errors) > 0:
            schema_errors.update({'tables': tables_errors})
        return validation_result, schema_errors
