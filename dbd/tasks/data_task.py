import os
from typing import Dict, List, Any, TypeVar

import pandas as pd
import sqlalchemy.engine
from sqlalchemy import Column, TEXT

from dbd.db.db_table import DbTable
from dbd.tasks.db_table_task import DbTableTask


class UnsupportedDataFile(Exception):
    pass


DataTaskType = TypeVar('DataTaskType', bound='DataTask')


class DataTask(DbTableTask):
    """
    Data loading task. Loads data from a local data file (e.g. CSV) to database.
    """

    def __init__(self, task_def: Dict[str, Any]):
        """
        Data task constructor
        :param Dict[str, Any] task_def: Target table definition
        """
        super().__init__(task_def)
        self.__data_frame = None

    def data_file(self) -> str:
        """
        Task SQL statement
        :return: task SQL statement
        """
        return self.task_data()

    def set_data_file(self, sql: str):
        """
        Sets task SQL
        :param sql: SQL statement
        """
        self.set_task_data(sql)

    @classmethod
    def from_code(cls, task_def: Dict[str, Any]) -> DataTaskType:
        """
        Creates a new task from table definition (dict)
        :param Dict[str, Any] task_def: table definition (dict)
        :return: new EltTask instance
        :rtype: EltTask
        """

        return DataTask(task_def)

    def __data_file_columns(self) -> List[sqlalchemy.Column]:
        """
        Introspects data file columns
        :return: list of data file columns (SQLAlchemy Column[])
        :rtype: List[sqlalchemy.Column]
        """
        return [Column(c, TEXT) for c in self.__data_frame.columns]

    # noinspection DuplicatedCode
    def __override_data_file_column_definitions(self) -> Dict[str, Any]:
        """
        Merges the data file column definitions with the column definitions from the task_def.
        The column definitions override the introspected data file types

        :return: data file columns overridden with the task's explicit column definitions
        :rtype: Dict[str, Any]
        """
        table_def = self.table_def()
        column_overrides = table_def.get('columns', {})
        data_file_columns = self.__data_file_columns()
        ordered_columns = {}
        for c in data_file_columns:
            overridden_column = column_overrides.get(c.name)
            if overridden_column:
                if 'type' not in overridden_column:
                    overridden_column['type'] = c.type
                ordered_columns[c.name] = overridden_column
            else:
                ordered_columns[c.name] = {"type": c.type}

        table_def['columns'] = ordered_columns
        return table_def

    def create(self, target_alchemy_metadata: sqlalchemy.MetaData, alchemy_engine: sqlalchemy.engine.Engine):
        """
        Executes the task. Creates the target table and loads data
        :param sqlalchemy.MetaData target_alchemy_metadata: MetaData SQLAlchemy MetaData
        :param sqlalchemy.engine.Engine alchemy_engine:
        """
        self.__read_file_to_dataframe()

        table_def = self.__override_data_file_column_definitions()
        db_table = DbTable.from_code(self.target(), table_def, target_alchemy_metadata, self.target_schema())
        self.set_db_table(db_table)
        db_table.create()

        self.__data_frame.to_sql(self.target(), alchemy_engine, schema=self.target_schema(),
                                 if_exists='append', index=False)

    def __read_file_to_dataframe(self):
        """
        Read the file content to Pandas dataframe
        """
        absolute_file_name = self.data_file()
        file_name, file_extension = os.path.splitext(absolute_file_name)
        if file_extension.lower() == '.csv':
            self.__data_frame = pd.read_csv(absolute_file_name)
        elif file_extension.lower() == '.json':
            self.__data_frame = pd.read_json(absolute_file_name)
        elif file_extension.lower() in ['.xls', '.xlsx', '.xlsm', '.xlsb', '.odf', '.ods', '.odt']:
            self.__data_frame = pd.read_excel(absolute_file_name)
        elif file_extension.lower() == '.parquet':
            self.__data_frame = pd.read_parquet(absolute_file_name)
        else:
            raise UnsupportedDataFile(f"Data files with extension '{file_extension}' aren't supported.")
