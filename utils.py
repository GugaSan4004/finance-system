import os
import json
import time

import mysql
import parsers

from mysql.connector.pooling import PooledMySQLConnection
from mysql.connector.abstracts import MySQLConnectionAbstract


def clear(

) -> None:
    """
    Clear the console.
    """
    os.system('clear')
    print('\n' * 50)


def message(
        code: int,
        module: str,
        args: str | Exception
) -> None:
    """
    Prints formatted status or error messages.

    Parameters:
        code (int): Message type code:
            -1: Information message
             0: Code execution error or Unknow Error
            >0: Error code (refer to error code dictionary)
        module (str): The name of the module generating the message
        args (str | Exception): Message content or exception object

    Error Code Dictionary:
        1001: MySQL Connection error.
        1004: Path or File Not Found

    Example:
        message(
            module='pdf',
            code=1002,
            args='No pdf files found in directory.'
        )
    """

    if code == -1:
        print(
            f'[{module.upper()}]: {args}'
        )
    elif code == 0:
        print(
            f'[{module.upper()}]: Error executing code -> "{args}"'
        )
    else:
        print(
            f'[{module.upper()}]: Error code ({code}) -> "{args}"'
        )


def connect_database(
        mysql_host: str,
        mysql_user: str,
        mysql_port: int,
        mysql_password: str,
        mysql_database: str
) -> MySQLConnectionAbstract | PooledMySQLConnection:
    """
    Establishes a connection to the MySQL database.

    Parameters:
        mysql_host (str): Database host address
        mysql_user (str): Database username
        mysql_port (int): Database port number
        mysql_password (str): Database user password
        mysql_database (str): Name of the database to connect to

    Returns:
        MySQLConnectionAbstract | PooledMySQLConnection: Active database connection object

    Prints connection status message using the message() function.
    """
    conn: PooledMySQLConnection | MySQLConnectionAbstract = mysql.connector.connect(
        port=mysql_port,
        host=mysql_host,
        user=mysql_user,
        password=mysql_password,
        database=mysql_database
    )
    return conn


def get_files(
        pdf_dir: str
) -> list[str] | None:
    """
    Retrieves a list of PDF files from the specified directory.

    Parameters:
        pdf_dir (str): Path to the directory containing PDF files

    Returns:
        list[str] | None: List of PDF filenames if found, None if no files found and user chooses to exit

    Continuously prompts user to retry or exit if no PDF files are found.
    Uses message() function to display status messages.
    """

    result: bool | None | list[str] = True
    while result is True:
        pdf_files = [
            f for f in os.listdir(
                path=pdf_dir
            ) if f.endswith(
                '.pdf'
            )
        ]
        if not pdf_files:
            message(
                module='pdf',
                code=1004,
                args='No pdf files found in directory.'
            )
            if input('Press enter to try again, or type "exit" to finalize: ').strip().lower() == 'exit':
                result = None
        else:
            result = pdf_files
    return result


def select_files(
        pdf_dir: list[str]
) -> str | None:
    """
    Presents a menu for user to select a PDF file for processing.

    Parameters:
        pdf_dir (list[str]): List of available PDF files

    Returns:
        str | None: Selected filename, 'manual' for manual entry, or None if user chooses to exit

    Displays numbered list of files and handles user input for file selection.
    Includes option for manual entry (0) and exit.
    Uses message() function to display error messages for invalid selections.
    """

    result: bool | str | None = True
    while result is True:
        message(
            module='pdf',
            code=-1,
            args='Found The Following Files: '
        )
        for index, name in enumerate(pdf_dir, 1):
            print(f'|----- {index}: {name}')
        choice: str = input(
            '|' + '\n' +
            '|----- 0: Manual Insert' + '\n' * 2 +
            'Enter the number of the file to extract' + '\n' +
            'Or enter exit: '
        ).strip().lower()

        if choice == 'exit' or choice == 'e':
            result = None
        elif choice == '0':
            result = 'manual'
        elif choice and int(choice):
            index: int = int(choice) - 1
            if 0 <= index < len(pdf_dir):
                result = pdf_dir[index]
            else:
                clear()
                message(
                    module='pdf',
                    code=1004,
                    args=f'No pdf with number "{choice}" found.'
                )
        else:
            clear()
            message(
                module='pdf',
                code=1004,
                args=f'No pdf with number "{choice}" found.'
            )
    return result


def parser(
        app_info,
        args: str | list[str],
        conn: MySQLConnectionAbstract | PooledMySQLConnection | None
) -> list[
    dict[
        str,
        str | int | float | None
    ]
] | dict[
        str,
        str | int | float | None
] | None:
    """
    Processes and parses financial document data based on bank-specific patterns.

    Parameters:
        app_info: Application configuration information (loaded from config.json)
        args (str | list): Text content to parse or special command ('manual')
        conn (MySQLConnectionAbstract | PooledMySQLConnection | None): Database connection object

    Returns:
        list[dict[str, str | int | float | None]] | dict[str, str | int | float | None] | None: List of parsed movements if successful, None if parsing fails

    Detects bank type and parsing parameters from document content.
    Handles both manual and automatic parsing modes.
    Uses message() function to display status and error messages.
    """

    with open('config.json', 'r') as f:
        app_info = json.load(f)

    bank_key: str | None = None
    bank_name: str | None = None
    movements: list[
        dict[
            str,
            str | int | float | None
        ]
    ] | None = None
    bank_parameter: str | None = None

    brute_text = '\n'.join(args).lower()
    json_parse = app_info['parsers']
    for parameter in json_parse:
        for data in json_parse[parameter]['values']:
            for slices in json_parse[parameter]['values'][data]:
                if slices in brute_text:
                    bank_key = parameter
                    bank_name = json_parse[parameter]['name']
                    bank_parameter = data
                    break
            if bank_key:
                break
        if bank_key:
            break

    if (
            not bank_key or bank_key not in parsers.fetch()
    ) and args != 'manual':
        message(
            code=1004,
            module='parser',
            args=f'Module "{bank_key}" not found.' if bank_name else f'No modules found to parse.'
        )
    else:
        if args != 'manual':
            message(
                code=-1,
                module='parser',
                args=f'"{bank_name}" detected, Extracting with the model -> ( {bank_parameter} )'
            )
            time.sleep(5)
        try:
            movements = parsers.run(
                conn=conn,
                args=args,
                parser=bank_key,
                runtype=bank_parameter
            )
        except Exception as e:
            message(
                code=0,
                module='parser',
                args=e
            )
    return movements

def insert_db(
    args:  list[
       dict[
           str,
           str | int | float | None
       ]
    ] | dict[
       str,
       str | int | float | None
    ],
    conn: mysql.connector.abstracts.MySQLConnectionAbstract | mysql.connector.pooling.PooledMySQLConnection
) -> None:
    try:
        with conn.cursor() as cursor:
            values = [
                (
                    movement['transaction_id'],
                    movement['date'],
                    movement['description'],
                    movement['value'],
                    movement['counterparty'],
                    movement['method'],
                    movement['banks_id'],
                    movement['banks_name'],
                    movement['cards_id'],
                    movement['category_name']
                )
                for movement in args
            ]
            cursor.executemany(
                operation="""
                INSERT INTO movements (
                    transaction_id, date, description, value, counterparty,
                    method, banks_id, banks_name, cards_id, category_name
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """,
                seq_params=values
            )
            conn.commit()
            message(
                module='mysql', 
                code=-1, 
                args=f'Operation ran successfully. {cursor.rowcount} rows affected!'
            )
    except Exception as e:
        message(
            module='mysql',
            code=0,
            args=e
        )