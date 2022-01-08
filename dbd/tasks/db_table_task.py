from typing import Dict, List, Any, TypeVar, Tuple

import sqlalchemy.engine
from cerberus import Validator

from dbd.db.db_table import DbTable
from dbd.tasks.task import Task, InvalidTaskDefinition
from dbd.utils.sql_parser import SqlParser
from dbd.utils.text_utils import fully_qualified_table_name

DbTableTaskType = TypeVar('DbTableTaskType', bound='DbTableTask')


class DbTableTask(Task):
    """
    Generic DB table task. Its result is a DB table that sits in a schema
    """

    def __init__(self, task_def: Dict[str, Any]):
        """
        DbTableTask constructor
        :param Dict[str, Any] task_def: table definition
        """

        self.__task_def = task_def
        self.__target_db_table = None

        runtime_def = self.runtime_def()

        if 'schema' not in runtime_def:
            raise InvalidTaskDefinition("Missing task runtime 'schema' key.")
        if 'table' not in runtime_def:
            raise InvalidTaskDefinition("Missing task runtime 'table' key.")

        table = runtime_def.get('table', None)
        schema = runtime_def.get('schema', None)
        task_id = Task.generate_task_id(table, schema)
        super().__init__(task_id, table, schema)

    def task_def(self) -> Dict[str, Any]:
        """
        Task definition dict
        :return: task definition (dict)
        :rtype: Dict[str, Any]
        """
        return self.__task_def

    def table_def(self) -> Dict[str, Any]:
        """
        Table definition dict
        :return: the table definition (dict)
        :rtype: Dict[str, Any]
        """
        return self.__task_def.get('table', {})

    def runtime_def(self) -> Dict[str, Any]:
        """
        Return runtime parameters
        :return: runtime parameters
        :rtype: Dict[str, Any]
        """
        return self.__task_def.get('runtime', {})

    def process_def(self) -> Dict[str, Any]:
        """
        Process definition dict
        :return: process definition (dict)
        :rtype: Dict[str, Any]
        """
        return self.__task_def.get('process', {})

    def db_table(self) -> DbTable:
        """
        Underlying DbTable
        :return: underlying DbTable
        :rtype: DbTable
        """
        return self.__target_db_table

    def set_db_table(self, db_table: DbTable):
        """
        Sets underlying DbTable
        :param DbTable db_table: DbTable
        """
        self.__target_db_table = db_table

    def drop(self, alchemy_metadata: sqlalchemy.MetaData, alchemy_engine: sqlalchemy.engine.Engine):
        """
        Drops the target table in the database
        :param sqlalchemy.MetaData alchemy_metadata: SqlAlchemy metadata
        :param sqlalchemy.engine.Engine alchemy_engine: Engine SQLAlchemy engine database connection
        """
        if self.__target_db_table is None:
            alchemy_table = alchemy_metadata.tables.get(self.fully_qualified_target())
            if alchemy_table is not None:
                self.__target_db_table = DbTable.from_alchemy_table(alchemy_table)
                self.__target_db_table.drop(alchemy_engine)
        else:
            self.__target_db_table.drop(alchemy_engine)
        self.__target_db_table = None

    def truncate(self, alchemy_metadata: sqlalchemy.MetaData, alchemy_engine: sqlalchemy.engine.Engine):
        """
        Truncates the target table
        :param sqlalchemy.MetaData alchemy_metadata: SqlAlchemy metadata
        :param sqlalchemy.engine.Engine alchemy_engine: Engine SQLAlchemy engine database connection
        """
        if self.__target_db_table is None:
            # TODO: fix the SQL statement to work for all DBs
            alchemy_table = alchemy_metadata.tables.get(self.fully_qualified_target())
            if alchemy_table is not None:
                self.__target_db_table = DbTable.from_alchemy_table(alchemy_table)
                self.__target_db_table.truncate(alchemy_engine)
        else:
            self.__target_db_table.truncate(alchemy_engine)

    def depends_on(self) -> List[str]:
        """
        Returns list of tables that the task depends on
        :return: list of tables that the task depends on
        :rtype: List[str]
        """
        target_db_schema = self.target_schema()
        columns = self.table_def().get('columns', {})
        foreign_key_tables = []
        for column in columns.values():
            column_foreign_keys = column.get('foreign_keys', [])
            foreign_key_tables += SqlParser.extract_foreign_key_tables(column_foreign_keys)
        return [f"{fully_qualified_table_name(target_db_schema, t)}" if len(t.split('.')) < 2 else t for t in
                foreign_key_tables]

    def validate(self) -> Tuple[bool, Dict[str, Any]]:
        """
        Validates task
        :return: True if task is valid, False otherwise and Dict of errors
        :rtype: Tuple[bool, Dict[str, Any]]
        """
        # TODO validate materialization, mode etc.
        task_errors = {}
        task_validator = Validator(
            {
                'table': {'type': 'dict'},
                'process': {'type': 'dict'},
                'runtime': {'type': 'dict'}
            })
        validation_result = task_validator.validate(self.__task_def)
        if not validation_result:
            task_errors['structure'] = task_validator.errors
        table_name = fully_qualified_table_name(self.target_schema(), self.target())
        table_validation_result, table_validation_errors = DbTable.validate_code(table_name, self.table_def())
        if not table_validation_result:
            task_errors['table'] = table_validation_errors
            validation_result = False

        process_validator = Validator(
            {
                'materialization': {'type': 'string'},
                'mode': {'type': 'string'}
            })
        process_validation_result = process_validator.validate(self.process_def())
        if not process_validation_result:
            task_errors['process'] = process_validator.errors
            validation_result = False

        runtime_validator = Validator(
            {
                'schema': {'type': 'string', 'nullable': True},
                'table': {'type': 'string'}
            })
        runtime_validation_result = runtime_validator.validate(self.runtime_def())
        if not runtime_validation_result:
            task_errors['runtime'] = runtime_validator.errors
            validation_result = False

        return validation_result, task_errors
