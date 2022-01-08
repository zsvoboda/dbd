from typing import List, Dict, TypeVar, Any, Tuple

import sqlalchemy.engine
from cerberus import Validator
from sqlalchemy import CheckConstraint, Table, PrimaryKeyConstraint, ForeignKeyConstraint, UniqueConstraint, Index, text
from sqlalchemy.exc import ProgrammingError

from dbd.db.db_column import DbColumn
from dbd.generator.jinja_generator_env import JINJA_GENERATOR_ENV
from dbd.utils.text_utils import fully_qualified_table_name

DbTableType = TypeVar('DbTableType', bound='DbTable')


class DbTable:
    """
    Database table definition
    """

    def __init__(self, name: str, columns: List[DbColumn], alchemy_table: sqlalchemy.Table):
        """
        DbTable constructor
        :param str name: table name
        :param List[DbColumn] columns:  table columns
        :param sqlalchemy.Table alchemy_table: Table underlying SQLAlchemy table
        """
        self.__name = name
        self.__columns = columns
        self.__alchemy_table = alchemy_table

    @staticmethod
    def __constraint_to_str(constraint: sqlalchemy.Constraint) -> str:
        # noinspection GrazieInspection
        """
                Returns constraint's string description for purposes of comparing it with other constraint
                :param sqlalchemy.Constraint constraint: SQLAlchemy table constraint (PrimaryKeyConstraint,
                ForeignKeyConstraint, UniqueConstraint, CheckConstraint)
                :return: the string description of the passed constraint
                :rtype: str
                """
        columns = f"{constraint.columns}" if hasattr(constraint, 'columns') else "[]"
        refcolumns = f"{constraint.refcolumns}" if hasattr(constraint, 'refcolumns') else "[]"
        sqltext = f"{constraint.sqltext}" if hasattr(constraint, 'sqltext') else "''"
        return f"{type(constraint)}(columns={columns}, refcolumns={refcolumns}, sqltext={sqltext})"

    def __eq__(self, other: DbTableType) -> bool:
        """
        Comparator
        :param DbTable other: another DbTable instance to compare this instance to
        :return: True if both DbTables are equal, False otherwise
        :rtype: bool
        """
        if isinstance(other, DbTable):
            if self.name() != other.name():
                return False
            my_columns = self.columns()
            other_columns = other.columns()
            for my_column in my_columns:
                other_column = [c for c in other_columns if c.name() == my_column.name()]
                if len(other_column) != 1 or my_column != other_column[0]:
                    return False
            return set([self.__constraint_to_str(c) for c in self.alchemy_table().constraints]) == set(
                [self.__constraint_to_str(c) for c in other.alchemy_table().constraints])
        return False

    def alchemy_table(self) -> sqlalchemy.Table:
        """
        :return: underlying SQLAlchemy table
        :rtype: sqlalchemy.Table
        """
        return self.__alchemy_table

    def columns(self) -> List[DbColumn]:
        """
        :return: table's columns
        :rtype: List[DbColumn]
        """
        return self.__columns

    def column(self, name: str) -> DbColumn:
        """
        Returns column by name
        :param name: column name
        :return: table (None if a column with such name doesn't exist)
        :rtype: DbTable
        """
        column_matches = [t for t in self.__columns if name == t.name()]
        return column_matches[0] if len(column_matches) == 1 else None

    def name(self) -> str:
        """
        :return: table name
        :rtype: str
        """
        return self.__name

    def code(self) -> str:
        """
        TODO: shall I turn this to JSON and to dict here?
        :return: DbTable's code
        :rtype: str
        """
        template = JINJA_GENERATOR_ENV.get_template('table.j2')
        return template.render(dict(t=self.__alchemy_table))

    def create(self):
        """
        TODO: exception handling
        Generates SQL and creates the table in the target database
        """
        self.__alchemy_table.create(checkfirst=True)

    def drop(self, alchemy_engine):
        """
        TODO: exception handling
        Drops the table in the target database. Also drops it if its a view.
        :param sqlalchemy.engine.Engine alchemy_engine: Engine SQLAlchemy engine database connection
        """
        try:
            self.__alchemy_table.drop(checkfirst=True)
        except ProgrammingError:
            # it may be view
            with alchemy_engine.connect() as conn:
                view_name = fully_qualified_table_name(self.__alchemy_table.schema, self.__alchemy_table.name)
                # TODO: make sure this works for all DBs
                conn.execute(text(f"DROP VIEW IF EXISTS {view_name}"))

    def truncate(self, alchemy_engine):
        """
        TODO: exception handling
        Truncates table data
        :param sqlalchemy.engine.Engine alchemy_engine: Engine SQLAlchemy engine database connection
        """
        with alchemy_engine.connect() as conn:
            table_name = fully_qualified_table_name(self.__alchemy_table.schema, self.__alchemy_table.name)
            # TODO: make sure this works for all DBs
            conn.execute(text(f"TRUNCATE TABLE {table_name}"))

    @classmethod
    def from_alchemy_table(cls, alchemy_table: sqlalchemy.Table) -> DbTableType:
        """
        Creates database table from passed SQLAlchemy table
        :param sqlalchemy.Table alchemy_table: SQLAlchemy table definition

        :return: new DbTable instance
        :rtype: DbTable
        """
        return DbTable(alchemy_table.name, [DbColumn.from_alchemy_column(c) for c in alchemy_table.columns.values()],
                       alchemy_table)

    @classmethod
    def from_code(cls, name: str, table_code: Dict[str, Any], alchemy_metadata: sqlalchemy.MetaData,
                  schema: str = None) -> DbTableType:
        """
        Creates database table from passed code
        :param str name: table name
        :param Dict[str, Any] table_code: DbTable's code definition
        :param sqlalchemy.MetaData alchemy_metadata: SQLAlchemy MetaData object
        :param str schema: table's schema
        :return: new DbTable instance
        """
        columns = cls.__extract_columns_from_table_code(table_code)
        constraints = cls.__extract_constraints_from_table_code(table_code)
        indexes = cls.__extract_indexes_from_table_code(name, table_code)

        arguments = [c.alchemy_column() for c in columns] + constraints + indexes
        return DbTable(name, columns, Table(name, alchemy_metadata, schema=schema, extend_existing=True, *arguments))

    @classmethod
    def __extract_indexes_from_table_code(cls, name: str, table_code: Dict[str, Any]) -> List[Index]:
        """
        Extracts index definitions from DbTable's code
        :param str name: table name for constructing default index name
        :param Dict[str, Any] table_code: DbTable code
        :return: Index array of SQLAlchemy indexes
        :rtype: List[Index]
        """
        indexes = []
        ti = table_code.get('indexes', {})
        for index in ti:
            cols = index.get('columns')
            unique = index.get('unique', False)
            index_name = index.get('name', f"idx_{name}_{len(indexes) + 1}")
            index_arguments = [index_name] + cols
            indexes.append(Index(*index_arguments, unique=unique))
        return indexes

    @classmethod
    def __extract_constraints_from_table_code(cls, table_code: Dict[str, Any]) -> List[sqlalchemy.Constraint]:
        """
        Extracts table constraint definitions from DbTable's code
        :param Dict[str, Any] table_code: DbTable code
        :return: array of SQLAlchemy constraints
        :rtype: List[sqlalchemy.Constraint]
        """
        constraints = []
        tc = table_code.get('constraints', {})
        for constraint in tc:
            constraint_type = constraint.get('type')
            cols = constraint.get('columns')
            refs = constraint.get('references')
            # TODO handle unknown constraint
            if constraint_type == 'checkConstraint':
                constraints.append(CheckConstraint(sqltext=constraint.get('sqltext')))
            elif constraint_type == 'primaryKeyConstraint':
                constraints.append(PrimaryKeyConstraint(*cols))
            elif constraint_type == 'foreignKeyConstraint':
                constraints.append(ForeignKeyConstraint(columns=cols, refcolumns=refs))
            elif constraint_type == 'uniqueConstraint':
                constraints.append(UniqueConstraint(*cols))
        return constraints

    @classmethod
    def __extract_columns_from_table_code(cls, table_code: Dict[str, Any]) -> List[DbColumn]:
        """
        Extracts table column definitions from DbTable's code
        :param Dict[str, Any] table_code: DbTable code
        :return: DbColumn array of table columns
        :rtype: List[DbColumn]
        """
        columns = []
        for c in table_code['columns'].items():
            column_name, column_code = c
            columns.append(DbColumn.from_code(column_name, column_code))
        return columns

    # noinspection PyUnusedLocal
    @classmethod
    def validate_code(cls, table_name: str, table_code: Dict[str, Any]) -> Tuple[bool, Dict[str, Any]]:
        """
        Validates table code
        :param str table_name: table name
        :param Dict[str, Any] table_code: table code
        :return: True if table is valid, False otherwise and Dict of errors
        :rtype: Tuple[bool, Dict[str, Any]]
        """
        table_errors = {}
        # TODO: validate the table_name
        table_validator = Validator(
            {
                'columns': {'type': 'dict'},
                'constraints': {'type': 'list'},
                'indexes': {'type': 'list'}
            })
        validation_result = table_validator.validate(table_code)
        if not validation_result:
            table_errors['structure'] = table_validator.errors
        column_errors = {}
        for c in [c for c in table_code.get('columns', {}).items()]:
            column_name, column_code = c
            columns_validation_result, column_validation_errors = DbColumn.validate_code(column_name, column_code)
            if not columns_validation_result:
                column_errors[column_name] = column_validation_errors
                validation_result = False
        if len(column_errors) > 0:
            table_errors.update({'columns': column_errors})
        constraint_errors = []
        for constraint_code in table_code.get('constraints', []):
            constraints_validation_result, constraints_validation_errors = cls.__validate_constraint(constraint_code)
            if not constraints_validation_result:
                constraint_errors.append(constraints_validation_errors)
                validation_result = False
        if len(constraint_errors) > 0:
            table_errors.update({'constraints': constraint_errors})
        index_errors = []
        for index_code in table_code.get('indexes', []):
            indexes_validation_result, indexes_validation_errors = cls.__validate_index(index_code)
            if not indexes_validation_result:
                index_errors.append(indexes_validation_errors)
                validation_result = False
        if len(index_errors) > 0:
            table_errors.update({'indexes': index_errors})
        return validation_result, table_errors

    @classmethod
    def __validate_index(cls, index_code: Dict[str, Any]) -> Tuple[bool, Dict[str, Any]]:
        """
        Validates index
        :param Dict[str, Any] index_code: index code
        :return: True if indexes are valid, False otherwise and Dict of errors
        :rtype: Tuple[bool, Dict[str, Any]]
        """
        # noinspection PyUnusedLocal
        index_errors = {}
        index_validator = Validator({
            'columns': {
                'type': 'list',
                'required': True
            },
            'unique': {'type': 'bool'}
        })
        index_validation_result = index_validator.validate(index_code)
        if not index_validation_result:
            return False, {'structure': index_validator.errors}
        return True, {}

    @classmethod
    def __validate_constraint(cls, constraint_code: Dict[str, Any]) -> Tuple[bool, Dict[str, Any]]:
        """
        Validates constraint
        :param Dict[str, Any] constraint_code: constraint code
        :return: True if constraints are valid, False otherwise and Dict of errors
        :rtype: Tuple[bool, Dict[str, Any]]
        """
        constraints_errors = {}
        constraint_validator = Validator({'type': {'allowed': ['checkConstraint', 'primaryKeyConstraint',
                                                               'foreignKeyConstraint', 'uniqueConstraint']}})
        constraint_validator.allow_unknown = True
        validation_result = constraint_validator.validate(constraint_code)
        if not validation_result:
            constraints_errors['structure'] = constraint_validator.errors
        constraint_type = constraint_code.get('type')
        if constraint_type == 'checkConstraint':
            check_constraint_validator = Validator({
                'type': {
                    'type': 'string',
                    'allowed': ['checkConstraint'],
                    'required': True
                },
                'sqltext': {
                    'type': 'string',
                    'required': True
                }
            })
            check_validation_result = check_constraint_validator.validate(constraint_code)
            if not check_validation_result:
                constraints_errors['checkConstraint'] = check_constraint_validator.errors
                validation_result = False
        elif constraint_type == 'primaryKeyConstraint':
            pk_constraint_validator = Validator({
                'type': {
                    'type': 'string',
                    'allowed': ['primaryKeyConstraint'],
                    'required': True
                },
                'columns': {
                    'type': 'list',
                    'required': True
                }
            })
            pk_validation_result = pk_constraint_validator.validate(constraint_code)
            if not pk_validation_result:
                constraints_errors['primaryKeyConstraint'] = pk_constraint_validator.errors
                validation_result = False
        elif constraint_type == 'foreignKeyConstraint':
            fk_constraint_validator = Validator({
                'type': {
                    'type': 'string',
                    'allowed': ['foreignKeyConstraint'],
                    'required': True
                },
                'columns': {
                    'type': 'list',
                    'required': True
                },
                'references': {
                    'type': 'list',
                    'required': True
                }
            })
            fk_validation_result = fk_constraint_validator.validate(constraint_code)
            if not fk_validation_result:
                constraints_errors['foreignKeyConstraint'] = fk_constraint_validator.errors
                validation_result = False
        elif constraint_type == 'uniqueConstraint':
            unique_constraint_validator = Validator({
                'type': {
                    'type': 'string',
                    'allowed': ['uniqueConstraint'],
                    'required': True
                },
                'columns': {
                    'type': 'list',
                    'required': True
                }
            })
            unique_validation_result = unique_constraint_validator.validate(constraint_code)
            if not unique_validation_result:
                constraints_errors['uniqueKeyConstraint'] = unique_constraint_validator.errors
                validation_result = False
        else:
            validation_result = False
        return validation_result, constraints_errors
