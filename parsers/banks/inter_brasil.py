import re
import os
import json

from datetime import datetime
from nltk.corpus import stopwords
from sklearn.pipeline import Pipeline
from sklearn.metrics import classification_report
from sklearn.linear_model import LogisticRegression
from sklearn.model_selection import train_test_split
from mysql.connector.pooling import PooledMySQLConnection
from sklearn.feature_extraction.text import TfidfVectorizer
from mysql.connector.abstracts import MySQLConnectionAbstract

class Modules:
    def __init__(self) -> None:
        base_path = os.path.dirname(
            p=__file__
        )
        file_path = os.path.join(
            base_path,
            'mp_category.json'
        )

        with open(
                file=file_path,
                mode='r',
                encoding='utf-8'
        ) as f:
            data = json.load(f)

        descriptions: list = [
            item['description'] for item in data
        ]
        categories: list = [
            item['category'] for item in data
        ]

        stopwords_pt: list = stopwords.words('portuguese')

        self.pipeline = Pipeline([
            (
                'vectorizer',
                TfidfVectorizer(
                    lowercase=True,
                    stop_words=stopwords_pt
                )
            ),
            (
                'classifier',
                LogisticRegression(
                    max_iter=1000
                )
            )
        ])

        x_train, x_test, y_train, y_test = train_test_split(
            descriptions, categories,
            test_size=0.2,
            random_state=42,
            stratify=categories
        )

        self.pipeline.fit(descriptions, categories)
        # self.pipeline.fit(
        #     X=x_train,
        #     Y=y_train
        # )

        y_pred = self.pipeline.predict(
            X=x_test
        )
        classification_report(
            y_test,
            y_pred,
            zero_division=0
        )

        self.dict = {
            "January": ["janeiro", "jan."],
            "February": ["fevereiro", "feb."],
            "March": ["março", "mar."],
            "April": ["abril", "abr."],
            "May": ["maio", "mai."],
            "June": ["junho", "jun."],
            "July": ["julho", "jul."],
            "August": ["agosto", "ago."],
            "September": ["setembro", "set."],
            "October": ["outubro", "out."],
            "November": ["novembro", "nov."],
            "December": ["dezembro", "dez."]
        }

        self.transaction_id: int | None = None
        self.date: str = ""
        self.description: str | None = None
        self.value: float = 0.0
        self.counterparty: str | None = None
        self.method: str = ""
        self.banks_id: int = 0
        self.banks_name: str = ""
        self.cards_id: int | None = None
        self.category_name: str | None = None

        self.bill_line = False
        self.date_line = False

    def analyze(
            self,
            line: str
    ) -> None:
        start: str = "despesas da fatura"
        end: str = "gustavo ribeiro silva"

        if start in line:
            self.bill_line = True

        if end in line and self.bill_line:
            self.bill_line = False

    def fetch_date(
            self,
            line: str
    ) -> None:
        data_match = ''
        if data_match := re.match(
            pattern=r'(\d{1,2}) de (\w+\.?) de (\d{4})',
            string=line
        ) or re.match(
                pattern=r'(\d{1,2}) de (\w+\.?) (\d{4})',
                string=line
        ):
            day = data_match[1]
            month = data_match[2]
            year = data_match[3]

            month_en = None
            for month_dictEng, month_dictPtList in self.dict.items():
                for month_dictPt in month_dictPtList:
                    if month == month_dictPt or month.rstrip('.') == month_dictPt.rstrip('.'):
                        month_en = month_dictEng
                        break
                if month_en:
                    break

            if month_en:
                date_str = f"{day} {month_en} {year}"
                date_obj = datetime.strptime(date_str, '%d %B %Y').date()
                self.date = date_obj.strftime('%Y-%m-%d')
                self.date_line = True if not self.bill_line else False

    def fetch_value(
            self,
            line: str
    ) -> None:
        parts = line.split()
        if re.search(
                string=line,
                pattern=r'[r]\$\s?-?\s?\d{1,}\.?\d{2}'
        ):
            brute_value: str = parts[-1]
            self.value = float(brute_value)

        value_match = re.search(
            string=line,
            pattern=r'(-?r\$ ?[\d\.,]+)'
        )
        self.value = (
            float(
                value_match[1]
                .replace(
                    'r$',
                    ''
                )
                .replace(
                    '.',
                    ''
                )
                .replace(
                    ',',
                    '.'
                )
            )
            if value_match
            else 0.0
        )

    def fetch_counterparty(
            self,
            line: str
    ) -> None:
        if self.bill_line:
            match = re.match(
                pattern=r'\d{2}/\d{2}\s+([^\d]+?)\s+(?:\d{9,}|r\$)',
                string=line,
                flags=re.IGNORECASE
            )
            if match:
                self.counterparty = match.group(1).strip().upper()
        else:
            counterparty_match1 = re.search(
                string=line,
                pattern=r'cp\s*:\d{8,}-([A-Za-z\s\.]+)'
            )
            counterparty_match2 = re.search(
                string=line,
                pattern=r'no estabelecimento\s+(.+?)(?:"|-)'
            )

            if counterparty_match1:
                self.counterparty = counterparty_match1[1].upper()
            elif counterparty_match2:
                self.counterparty = counterparty_match2[1].upper()
            else:
                self.counterparty = None

    def fetch_description(
            self,
            line: str
    ) -> None:
        descriptions_dict:  dict[str, str] = {
            "CLT SALARY": "URBAN TECNOLOGIA E INOVACAO LTDA",
            "HAIR CUT": "OTAVIANO ALVES DA SILVA",
            "YOUTUBE PREMIUM": "GOOGLE YOUTUBE",
            "GYMPASS": "WELLHUB GYMPASS"
        }

        for description, counterparty in descriptions_dict.items():
            if counterparty.lower() in line:
                self.description = description

    def fetch_method(
            self,
            line: str
    ) -> None:
        if self.bill_line:
            self.method = 'CREDIT'
        else:
            methods = {
                "PIX": [
                    "pix",
                    "transferencia",
                    "transferência"
                ],
                "DEBIT": [
                    "debito",
                    "estorno"
                ],
                "CREDIT": [
                    "pagamento"
                ],
                "REVENUE": [
                    "rendimento",
                    "rendimentos"
                ]
            }

            for method, words in methods.items():
                for word in words:
                    if word in line:
                        self.method = method

    def fetch_cards_id(
            self,
            line: str,
            conn: MySQLConnectionAbstract | PooledMySQLConnection | None
    ) -> None:
        if re.search(
                string=line,
                pattern=r'cart[a|ã]o\s{1,}\d{4}\*{4,}\d{4}'
        ) and conn and self.bill_line:
            cursor = conn.cursor(
                dictionary=True
            )
            cursor.execute(
                'SELECT id FROM cards'
            )
            cards = cursor.fetchall()

            for card in cards:
                digits = re.findall(
                    pattern=r'\d{4}',
                    string=line
                )
                if len(digits) >= 2:
                    first_four = digits[0]
                    last_four = digits[-1]
                    card_id = card['id'] if isinstance(card, dict) else card[0]
                    card_number = str(card_id)

                    if card_number.startswith(first_four) and card_number.endswith(last_four):
                        self.cards_id = int(
                            str(
                                object=card_id
                            )
                        )
                        break
                    else:
                        self.cards_id = None
            cursor.close()

    def fetch_category(
            self,
            line: str
    ) -> None:
        family = [
            "SILVIA",
            "ROSANA",
            "CELIO",
            "SILVA",
            "CAROLINA"
        ]

        self.category_name = str(
            self.pipeline.predict(
                X=[line]
            )[0]
        )

        if self.counterparty:
            match self.counterparty:
                case 'GUSTAVO RIBEIRO SILVA':
                    self.category_name = 'REPASS'
                case 'URBAN TECNOLOGIA E INOVACAO LTDA' | 'JUSCILEY BELEM DE OLIVEIRA':
                    self.category_name = 'WAGE'
                case _:
                    for people in family:
                        if people in self.counterparty:
                            self.category_name = 'FAMILY'

    def finish(
            self,
            bank_parameter: str
    ) -> dict[
        str,
        str | int | float | None
    ]:
        self.banks_id = 35555757
        self.banks_name = "Inter Brasil"

        processed = {
            "transaction_id": self.transaction_id,
            "date": self.date,
            "description": self.description,
            "value": self.value,
            "counterparty": self.counterparty,
            "method": self.method,
            "banks_id": self.banks_id,
            "banks_name": self.banks_name,
            "cards_id": self.cards_id,
            "category_name": self.category_name
        }

        if self.date_line:
            self.date_line = False
            processed = None

        if (
            (bank_parameter == 'inter_bill' and not self.bill_line) or
            (bank_parameter == 'inter_bill' and self.date == '') or
            self.value == 0.0
        ):
            processed = None


        self.transaction_id = None
        self.date = "" if self.bill_line else self.date
        self.description = None
        self.value = 0.0
        self.counterparty = None
        self.method = ""
        self.banks_id = 0
        self.banks_name = ""
        self.cards_id = None if not self.bill_line else self.cards_id
        self.category_name = None

        return processed


def parse(
        runtype,
        args: list[str],
        conn: MySQLConnectionAbstract | PooledMySQLConnection | None
) -> list[
    dict[
        str,
        str | int | float | None
    ]
] | None:
    __modules = Modules()
    __movements: list = []

    brute_text: str = '\n'.join(args).lower()

    for line in brute_text.split('\n'):
        steps = [
            "fetch_date",
            "fetch_value",
            "fetch_counterparty",
            "fetch_description",
            "fetch_method",
            "fetch_category"
        ]

        if runtype == 'inter_bill':
            __modules.analyze(
                line=line
            )
        if conn:
            __modules.fetch_cards_id(
                line=line,
                conn=conn
            )
        for step in steps:
            getattr(
                __modules,
                step
            )(
                line=line
            )

        movement = __modules.finish(
            bank_parameter=runtype
        )

        if movement:
            __movements.append(movement)
    return __movements