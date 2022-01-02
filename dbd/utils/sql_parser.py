import re
import sys
from typing import List

import sqlalchemy
from sql_metadata import Parser


class SQlParserException(Exception):
    pass


class SqlParser:
    """ Parses SQL and extracts different parts from the parsed SQL statement."""

    @classmethod
    def extract_tables(cls, sql: str) -> List[str]:
        """
        Extracts tables from the parsed SQL statement.
        Works with subquery, CTE, and many other SQL constructs
        :param str sql: parsed SQL statement
        :return: list of tables that the SQL statement depends on
        :rtype: List[str]
        """
        return Parser(sql).tables

    @classmethod
    def extract_foreign_key_tables(cls, foreign_keys_def: List[str]) -> List[str]:
        """
        Extracts tables that the passed foreign keys depend on
        Relies on the <table>.<column> format
        :param List[str] foreign_keys_def: foreign key
        :return: str array of table names extracted from passed foreign keys
        :rtype: List[str]
        """
        for f in foreign_keys_def:
            if len(f.split('.')) <= 1:
                raise SQlParserException(f"Invalid foreign key format '{f}'. There is no table component.")
        return [f.split('.')[0] for f in foreign_keys_def]

    @classmethod
    def compact_sql(cls, sql: str) -> str:
        """
        Compacts the SQL text. Strip comments, etc.
        :param sql: input SQL
        :return: compacted SQL text
        :rtype: str
        """
        parsed_sql = Parser(sql)
        return parsed_sql.without_comments

    @classmethod
    def comments(cls, sql: str) -> List[str]:
        """
        Returns the SQL comments
        :param sql: input SQL
        :return: comments as array of string
        :rtype: List[str]
        """
        parsed_sql = Parser(sql)
        return parsed_sql.comments

    @classmethod
    def parse_alchemy_data_type(cls, data_type: str) -> sqlalchemy.types.TypeEngine:
        """
        Parses SQLAlchemy datatype from string
        :param str data_type: SQLAlchemy data type as string
        :return: SQLAlchemy data type
        :rtype:  sqlalchemy.types.TypeEngine subclass
        """
        parsed_data_type = Parser(f"CREATE TABLE a( c {data_type} )")
        # parsed_data_type = Parser(data_type)
        core_data_type = parsed_data_type.tokens[5].value
        length = int(parsed_data_type.tokens[7].value) if len(
            parsed_data_type.tokens) > 7 and parsed_data_type.tokens[7].is_integer else None
        scale = int(parsed_data_type.tokens[9].value) if len(
            parsed_data_type.tokens) > 9 and parsed_data_type.tokens[9].is_integer else None

        try:
            datatype_class = getattr(sys.modules['sqlalchemy.sql.sqltypes'], core_data_type)
        except AttributeError:
            datatype_class = getattr(sys.modules['sqlalchemy.dialects.postgresql'], core_data_type)

        if core_data_type in ('CHAR', 'VARCHAR'):
            return datatype_class(length=length)
        elif core_data_type in ('DECIMAL', 'NUMERIC'):
            return datatype_class(precision=length, scale=scale)
        elif core_data_type in ('TIMESTAMP', 'DATE', 'INTEGER', 'FLOAT', 'DOUBLE', 'TEXT', 'SMALLINT',
                                'DOUBLE_PRECISION', 'REAL'):
            return datatype_class()
        else:
            raise SQlParserException(f"Unsupported data type {core_data_type}.")

    @classmethod
    def remove_sql_comments(cls, sql_text: str) -> str:
        """
        Remove SQL comments from a SQL text
        :param str sql_text: SQL texts with comments
        :return: SQL texts without comments
        :rtype: str
        """
        pattern = r"(\".*?\"|\'.*?\')|(/\*.*?\*/|--[^\r\n]*$)"
        # first group captures quoted strings (double or single)
        # second group captures comments (//single-line or /* multi-line */)
        regex = re.compile(pattern, re.MULTILINE | re.DOTALL)

        def _replacer(match):
            # if the 2nd group (capturing comments) is not None,
            # it means we have captured a non-quoted (real) comment string.
            if match.group(2) is not None:
                return ""  # so we will return empty to remove the comment
            else:  # otherwise, we will return the 1st group
                return match.group(1)  # captured quoted-string

        no_comments = regex.sub(_replacer, sql_text)
        # replace multiple newlines with one
        return re.sub(r'\n+', '\n', no_comments).strip()
