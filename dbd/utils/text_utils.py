import os


def remove_prefix(text: str, prefix: str) -> str:
    """
    Removes the prefix from the beginning of the text if the text starts with the prefix

    :param str text: text that potentially starts with a prefix
    :param str prefix: prefix that the text potentially starts with
    :return: the text with stripped prefix
    :rtype: str
    """
    if len(prefix) > 0 and text.startswith(prefix):
        return text[len(prefix):]
    return text


def relative_path_to_base_dir(base_directory: str, directory: str, file_name: str, extension: str) -> str:
    """
    Returns path represented by the directory, file_name, and extension relative to the base_directory
    :param str base_directory: base directory
    :param str directory: directory
    :param str file_name: file name
    :param str extension: file extension
    :return: path represented by the directory, file_name, and extension relative to the base_directory
    :rtype: str
    """
    return relative_path_to_base_dir_no_ext(base_directory, directory, f"{file_name}{'' if extension.startswith('.') else '.'}{extension}")

def relative_path_to_base_dir_no_ext(base_directory: str, directory: str, file_name: str) -> str:
    """
    Returns path represented by the directory, file_name, and extension relative to the base_directory
    :param str base_directory: base directory
    :param str directory: directory
    :param str file_name: file name
    :return: path represented by the directory, file_name, and extension relative to the base_directory
    :rtype: str
    """
    full_file_name = os.path.join(directory, file_name)
    return remove_prefix(full_file_name, base_directory)



def strip_table_name(column_name: str) -> str:
    """
    Strips the table name from the fully qualified column name
    :param str column_name: column name in format <table>.<column_name>
    :return: the column name
    :rtype: str
    """
    return ','.split(column_name)[-1]


def fully_qualified_table_name(schema: str, table: str) -> str:
    """
    Returns fully qualified table name
    :param str schema: schema name
    :param str table: table name
    :return: fully qualified table name
    :rtype: str
    """
    if schema is None or len(schema) < 1:
        return table
    else:
        return f"{schema}.{table}"
