import os
import json

import nltk
import utils
import dotenv
import pathlib
import pdf2image
import pytesseract

from tkinter import Image
from getpass import getpass
from utils import message, clear
from mysql.connector.pooling import PooledMySQLConnection
from mysql.connector.abstracts import MySQLConnectionAbstract

dotenv.load_dotenv()
nltk.download('stopwords')

base_path = os.path.dirname(
    __file__
)
config_path = os.path.join(
    base_path,
    'config.json'
)
with open(
    file=config_path,
    mode='r'
) as f:
    appInfo = json.load(f)


def main(
        conn: MySQLConnectionAbstract | PooledMySQLConnection | None
) -> None:

    running: bool = True
    while running:
        try:
            run_parameter: str | list[str] = []

            pdf_dir: str = './pdf'

            selection: str | None = utils.select_files(
                pdf_dir=utils.get_files(
                    pdf_dir=pdf_dir
                )
            )

            if selection:
                clear()
                if selection != 'manual':
                    message(
                        code=-1,
                        module='parser',
                        args='Please Wait, This action can take a while...'
                    )
                    image: list[Image] = pdf2image.convert_from_path(
                        dpi=600,
                        pdf_path=pathlib.Path(pdf_dir) / selection
                    )
                    for img in image:
                        text:  bytes | str | dict[str, bytes | str] = pytesseract.image_to_string(
                            image=img,
                            lang='por',
                            config=r'--oem 3 --psm 4'
                        )
                        run_parameter.append(text)
                else:
                    run_parameter = selection
                if len(run_parameter) > 0:
                    movements = utils.parser(
                        conn=conn,
                        app_info=appInfo,
                        args=run_parameter
                    )
                    if movements[0] and conn:
                        for index, movement in enumerate(movements, 1):
                            print(f'{index}: {movement}')
                        message(
                            module='parser',
                            code=-1,
                            args=f'!!!Warning!!! The app is in the {appInfo["version"]} version, check if everything is ok before confirm.'
                        )
                        message(
                            module='parser',
                            code=-1,
                            args=f'"{len(movements)}" movements found in the PDF. Do you want to add them to the Database?'
                        )
                        confirm: str = input('"Y" to Yes or "N" to No: ').strip().lower()
                        if confirm == 'y' or confirm == 'yes':
                            utils.insert_db(
                                args=movements,
                                conn=conn
                            )
            elif selection is None:
                running = False
        except Exception as e:
            message(
                module='pdf',
                code=0,
                args=e
            )
            running = False


def debug_mode(

) -> None:
    clear()
    try:
        if getpass('Debug Mode Password: ') == os.getenv('DEBUGMODE_PASSWORD'):
            message(
                module='mysql',
                code=-1,
                args='-------!!! DEBUG MODE !!!-------' + '\n' +
                     '- DATABASE CONNECTION DISABLED -' + '\n' +
                     '--------------------------------' + '\n' * 2
            )
            main(
                conn=None
            )
        else:
            return None
    except Exception as e:
        message(
            module='system',
            code=0,
            args=e
        )

def normal_mode(

) -> None:
    clear()
    try:
        message(
            module='mysql',
            code=-1,
            args='Connecting to database...'
        )
        db = utils.connect_database(
            mysql_host=os.getenv('MYSQL_HOST'),
            mysql_user=os.getenv('MYSQL_USER'),
            mysql_port=int(os.getenv('MYSQL_PORT')),
            mysql_password=os.getenv('MYSQL_PASSWORD'),
            mysql_database=os.getenv('MYSQL_DATABASE'),
        )
    except Exception as e:
        message(
            module='mysql',
            code=1001,
            args=e
        )
    else:
        message(
            module='mysql',
            code=-1,
            args='Connection successful.'
        )
        main(
            conn=db
        )


clear()
print(
    f'Welcome to: {appInfo["name"].upper()}' + '\n' +
    f'Version: {appInfo["version"]}'
)

choiceScreen: bool = True
while choiceScreen:
    modeNumbers: list[str] = []

    for number in appInfo['modes']:
        modeNumbers.append(number)
        print(
            f'|-- {number}: {appInfo["modes"][number]["name"]}'
        )
    modeSelected = input(
        '\n' + 'Please enter the required mode or "exit" to finalize the program: '
    ).strip().lower()

    if modeSelected == 'exit' or modeSelected == 'e':
        clear()
        message(
            module='system',
            code=-1,
            args='Bye <3.'
        )
        choiceScreen = False
    elif modeSelected not in modeNumbers:
        clear()
        message(
            module='system',
            code=1004,
            args=f'Mode "{modeSelected}" not found.'
        )
    else:
        if eval(appInfo['modes'][modeSelected]['alias'])() is None:
            choiceScreen = True
        else:
            choiceScreen = False