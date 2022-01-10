from typing import TypeVar, Dict, Any, Tuple

import sqlalchemy
from cerberus import Validator
from sqlalchemy import ForeignKey, Column

from dbd.generator.jinja_generator_env import JINJA_GENERATOR_ENV
from dbd.utils.sql_parser import SqlParser

DbColumnType = TypeVar('DbColumnType', bound='DbColumn')


class DbColumn:
    """
    Database column definition
    """

    def __init__(self, name: str, alchemy_column: sqlalchemy.Column):
        """
        Database column's constructor
        :param str name: column name
        :param sqlalchemy.Column alchemy_column: Column underlying SQLAlchemy column
        """
        self.__name = name
        self.__alchemy_column = alchemy_column

    def alchemy_column(self) -> sqlalchemy.Column:
        """
        :return: underlying SQLAlchemy column
        :rtype: sqlalchemy.Column
        """
        return self.__alchemy_column

    def name(self) -> str:
        """
        :return: column's name
        :rtype: str
        """
        return self.__name

    def type_name(self) -> str:
        """
        :return: column's type name
        :rtype: str
        """
        return str(self.__alchemy_column.type)

    def type(self) -> sqlalchemy.types.TypeEngine:
        """
        :return: column's type
        :rtype: sqlalchemy.types.TypeEngine
        """
        return self.__alchemy_column.type

    def code(self) -> str:
        """
        TODO: shall I turn this to JSON and to dict here?
        :return: DbColumn's code
        :rtype: str
        """
        template = JINJA_GENERATOR_ENV.get_template('column.j2')
        return template.render(dict(c=self.__alchemy_column))

    def __eq__(self, other: DbColumnType) -> bool:
        """
        Comparator
        :param DbColumn other: another DbColumn instance to compare this instance to
        :return: True if both DbColumns are equal, False otherwise
        :rtype: bool
        """
        if isinstance(other, DbColumn):
            return self.code() == other.code()

    @classmethod
    def from_alchemy_column(cls, alchemy_column: sqlalchemy.Column) -> DbColumnType:
        """
        Creates database column from underlying SQLAlchemy column
        :param sqlalchemy.Column alchemy_column: SQLAlchemy column definition
        :return: new DbColumn instance
        :rtype: DbColumn
        """
        return DbColumn(alchemy_column.name, alchemy_column)

    @classmethod
    def from_code(cls, column_name: str, column_code: Dict[str, Any]) -> DbColumnType:
        """
        Creates database column from passed SQLAlchemy column
        :param str column_name: column name
        :param Dict[str, Any] column_code: DbColumn's code definition
        :return: new DbColumn instance
        :rtype: DbColumn
        """
        fks = column_code.get('foreign_keys')
        foreign_keys = [ForeignKey(column=f) for f in fks] if fks is not None else []
        return DbColumn(column_name, Column(column_name, SqlParser.parse_alchemy_data_type(column_code['type']),
                                            primary_key=column_code.get('primary_key', False),
                                            nullable=column_code.get('nullable', True),
                                            # autoincrement=column_code.get('autoincrement'),
                                            unique=column_code.get('unique', False),
                                            index=column_code.get('index', False),
                                            default=column_code.get('default'),
                                            *foreign_keys
                                            ))

    # noinspection PyUnusedLocal
    @classmethod
    def validate_code(cls, column_name: str, column_code: Dict[str, Any]) -> Tuple[bool, Dict[str, Any]]:
        """
        Validates column code
        :param str column_name: column code
        :param Dict[str, Any] column_code: column code
        :return: True if column is valid, False otherwise and Dict of errors
        :rtype: Tuple[bool, Dict[str, Any]]
        """
        column_errors = {}
        # TODO: validate the column_name
        column_validator = Validator(
            {
                'type': {'type': 'string'},
                'primary_key': {'type': 'boolean'},
                'nullable': {'type': 'boolean'},
                'unique': {'type': 'boolean'},
                'index': {'type': 'boolean'},
                'default': {'type': ['string', 'boolean', 'date', 'datetime', 'float', 'integer', 'number']},
                'foreign_keys': {'type': 'list'}
            })
        validation_result = column_validator.validate(column_code)
        if not validation_result:
            column_errors['structure'] = column_validator.errors
        fk_errors = []
        for foreign_key_column in column_code.get('foreign_keys', []):
            fk_validation_result = (len(foreign_key_column.split('.')) == 2)
            if not fk_validation_result:
                fk_errors.append({
                    foreign_key_column: f"Invalid format ( not <table>.<column>)."})
            validation_result = validation_result and fk_validation_result
        if len(fk_errors) > 0:
            column_errors.update({'foreign_keys': fk_errors})
        return validation_result, column_errors
