import os
import importlib

from mysql.connector.pooling import PooledMySQLConnection
from mysql.connector.abstracts import MySQLConnectionAbstract

def fetch() -> list[str]:
    """
    Get a list of all files in the banks directory
    Returns:
        list: A list of filenames from the banks directory
    """
    banks_path = os.path.join(
        os.path.dirname(__file__),
        'banks'
    )

    if not os.path.exists(banks_path):
        os.makedirs(banks_path)
        raise FileNotFoundError('Banks folder not found!')

    files: list[str] = [
        f for f in os.listdir(
            banks_path
        ) if os.path.isfile(
            os.path.join(banks_path, f)
        )
    ]

    file_names: list[str] = [
        os.path.splitext(
            p=file
        )[0] for file in files if file.endswith('.py')
    ]

    return file_names


def run(
        parser: str,
        runtype: str,
        args: list[str] | str,
        conn: MySQLConnectionAbstract | PooledMySQLConnection | None
) -> list[
    dict[
        str,
        str | int | float | None
    ]
] | None:

    modules = importlib.import_module(
        name=f'parsers.banks.{parser}'
    )
    return modules.parse(
        conn=conn,
        args=args,
        runtype=runtype
    )
