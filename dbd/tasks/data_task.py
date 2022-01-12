import os
import tempfile
from datetime import datetime, date
from typing import Dict, List, Any, TypeVar

import click
import pandas as pd
import sqlalchemy.engine
from sqlalchemy import Column, TEXT

from dbd.db.db_table import DbTable
from dbd.log.dbd_exception import DbdException
from dbd.tasks.db_table_task import DbTableTask
from dbd.utils.io_utils import download_file, url_to_filename
from dbd.utils.io_utils import is_url
from dbd.utils.sql_parser import SqlParser


class UnsupportedDataFile(DbdException):
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

    def data_files(self) -> List[str]:
        """
        Task data files
        :return: task data files

        """
        return self.task_data()

    def set_data_files(self, data_files: List[str]):
        """
        Sets task data files
        :param List[str] data_files: task data file
        """
        self.set_task_data(data_files)

    @classmethod
    def from_code(cls, task_def: Dict[str, Any]) -> DataTaskType:
        """
        Creates a new task from table definition (dict)
        :param Dict[str, Any] task_def: table definition (dict)
        :return: new EltTask instance
        :rtype: EltTask
        """

        return DataTask(task_def)

    # noinspection PyMethodMayBeStatic
    def __data_file_columns(self, data_frame: pd.DataFrame) -> List[sqlalchemy.Column]:
        """
        Introspects data file columns
        :param pd.DataFrame data_frame: Pandas dataframe with populated data
        :return: list of data file columns (SQLAlchemy Column[])
        :rtype: List[sqlalchemy.Column]
        """
        # TODO: Consider introspection of the dataframe columns
        return [Column(c, TEXT) for c in data_frame.columns]

        # noinspection DuplicatedCode

    def __override_data_file_column_definitions(self, data_frame: pd.DataFrame) -> Dict[str, Any]:
        """
        Merges the data file column definitions with the column definitions from the task_def.
        The column definitions override the introspected data file types
        :param pd.DataFrame data_frame: Pandas dataframe with populated data
        :return: data file columns overridden with the task's explicit column definitions
        :rtype: Dict[str, Any]
        """
        table_def = self.table_def()
        column_overrides = table_def.get('columns', {})
        data_file_columns = self.__data_file_columns(data_frame)
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
        for data_file in self.data_files():
            if len(data_file) > 0:
                with tempfile.TemporaryDirectory() as tmpdirname:
                    if is_url(data_file):
                        absolute_file_name = os.path.join(tmpdirname, url_to_filename(data_file))
                        click.echo(f"\tDownloading file: '{absolute_file_name}'.")
                        download_file(data_file, absolute_file_name)
                    else:
                        absolute_file_name = data_file

                    df = self.__read_file_to_dataframe(absolute_file_name)

                    if self.db_table() is None:
                        table_def = self.__override_data_file_column_definitions(df)
                        db_table = DbTable.from_code(self.target(), table_def, target_alchemy_metadata,
                                                     self.target_schema())
                        self.set_db_table(db_table)
                        db_table.create()

                    dtype = self.__adjust_dataframe_datatypes(df)
                    click.echo(f"\tLoading data to database.")
                    df.to_sql(self.target(), alchemy_engine, chunksize=1024, method='multi',
                              schema=self.target_schema(), if_exists='append', index=False, dtype=dtype)

    def __adjust_dataframe_datatypes(self, df):
        """
        Adjusts the dataframe datatypes to match the target table
        :param pd.DataFrame df: Pandas dataframe with populated data
        :return: dtype for to_sql
        """
        dtype = {}
        for c in self.db_table().columns():
            column_name = c.name()
            column_type = c.type()
            python_type = SqlParser.parse_alchemy_data_type(column_type).python_type
            if isinstance(python_type, type) and issubclass(python_type, datetime):
                df[column_name] = df[column_name].map(lambda x: SqlParser.parse_datetime(x))
            elif isinstance(python_type, type) and issubclass(python_type, date):
                df[column_name] = df[column_name].map(lambda x: SqlParser.parse_date(x))
            elif isinstance(python_type, type) and issubclass(python_type, bool):
                df[column_name] = df[column_name].map(lambda x: SqlParser.parse_bool(x))
            elif isinstance(python_type, type) and issubclass(python_type, int):
                python_type = 'Int64'
                # pandas bug workaround
                df[column_name] = df[column_name].astype('float').astype(python_type)
            elif isinstance(python_type, type) and issubclass(python_type, float):
                df[column_name] = df[column_name].astype(python_type)
            dtype[column_name] = column_type
        return dtype

    # noinspection PyMethodMayBeStatic
    def __read_file_to_dataframe(self, absolute_file_name: str) -> pd.DataFrame:
        """
        Read the file content to Pandas dataframe
        :param str absolute_file_name: filename to read the dataframe from
        :return: Pandas DataFrame
        :rtype: pd.DataFrame
        """
        if is_url(absolute_file_name):
            absolute_file_name = download_file(absolute_file_name, absolute_file_name)
        file_name, file_extension = os.path.splitext(absolute_file_name)
        if file_extension.lower() == '.csv':
            return pd.read_csv(absolute_file_name, dtype=str)
        elif file_extension.lower() == '.json':
            # noinspection PyTypeChecker
            return pd.read_json(absolute_file_name)
        elif file_extension.lower() in {'.xls', '.xlsx', '.xlsm', '.xlsb', '.odf', '.ods', '.odt'}:
            return pd.read_excel(absolute_file_name)
        elif file_extension.lower() == '.parquet':
            return pd.read_parquet(absolute_file_name)
        else:
            raise UnsupportedDataFile(f"Data files with extension '{file_extension}' aren't supported.")
