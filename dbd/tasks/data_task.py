import os
import tempfile
from typing import Dict, List, Any, TypeVar

import pandas as pd
import sqlalchemy.engine
from sqlalchemy import Column, TEXT

from dbd.utils.io_utils import download_file, url_to_filename

from dbd.db.db_table import DbTable
from dbd.log.dbd_exception import DbdException
from dbd.tasks.db_table_task import DbTableTask
from dbd.utils.text_utils import relative_path_to_base_dir_no_ext


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
        self.__data_frame = self.__read_file_to_dataframe(self.data_file())

        table_def = self.__override_data_file_column_definitions()
        db_table = DbTable.from_code(self.target(), table_def, target_alchemy_metadata, self.target_schema())
        self.set_db_table(db_table)
        db_table.create()

        self.__data_frame.to_sql(self.target(), alchemy_engine, chunksize=1024, method = 'multi',
                                 schema=self.target_schema(), if_exists='append', index=False)

    def __urls_to_dataframe(self, absolute_file_name: str) -> pd.DataFrame:
        """
        Downloads and appends files from a URLs to a Pandas dataframe
        :param str absolute_file_name: filename of a file with on or more urls of the files to download
        :return: Pandas DataFrame
        :rtype: pd.DataFrame
        """
        with tempfile.TemporaryDirectory() as tmpdirname:
            with open(absolute_file_name, 'r') as f:
                urls = f.readlines()
                all_dataframes = []
                for u in [u.strip() for u in urls]:
                    if len(u) > 0:
                        tmp_file_name = os.path.join(tmpdirname, url_to_filename(u))
                        download_file(u, tmp_file_name)
                        df = self.__read_file_to_dataframe(tmp_file_name)
                        all_dataframes.append(df)
        return pd.concat(all_dataframes, axis=0, ignore_index=True)

    def __refs_to_dataframe(self, absolute_file_name: str) -> pd.DataFrame:
        """
        Appends files from a references to a Pandas dataframe
        :param str absolute_file_name: filename of a file with on or more references to the files to append
        :return: Pandas DataFrame
        :rtype: pd.DataFrame
        """
        with open(absolute_file_name, 'r') as f:
            files = f.readlines()
            all_dataframes = []
            for f in [f.strip() for f in files]:
                if len(f) > 0:
                    relative_file_name = relative_path_to_base_dir_no_ext('', os.path.dirname(absolute_file_name), f)
                    df = self.__read_file_to_dataframe(relative_file_name)
                    all_dataframes.append(df)
        return pd.concat(all_dataframes, axis=0, ignore_index=True)

    
    def __read_file_to_dataframe(self, absolute_file_name : str) -> pd.DataFrame:
        """
        Read the file content to Pandas dataframe
        :param str absolute_file_name: filename to read the dataframe from
        :return: Pandas DataFrame
        :rtype: pd.DataFrame
        """
        file_name, file_extension = os.path.splitext(absolute_file_name)
        if file_extension.lower() == '.csv':
            return pd.read_csv(absolute_file_name, dtype=str)
        elif file_extension.lower() == '.json':
            return pd.read_json(absolute_file_name, dtype=False)
        elif file_extension.lower() in ['.xls', '.xlsx', '.xlsm', '.xlsb', '.odf', '.ods', '.odt']:
            return pd.read_excel(absolute_file_name, dtype=str)
        elif file_extension.lower() == '.parquet':
            return pd.read_parquet(absolute_file_name)
        elif file_extension.lower() == '.url':
            return self.__urls_to_dataframe(absolute_file_name)
        elif file_extension.lower() == '.ref':
            return self.__refs_to_dataframe(absolute_file_name)
        else:
            raise UnsupportedDataFile(f"Data files with extension '{file_extension}' aren't supported.")
