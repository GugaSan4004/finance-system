import re
import os
import json

from datetime import datetime
from nltk.corpus import stopwords
from collections import defaultdict
from sklearn.pipeline import Pipeline
from sklearn.metrics import classification_report
from sklearn.linear_model import LogisticRegression
from sklearn.model_selection import train_test_split
from mysql.connector.pooling import PooledMySQLConnection
from sklearn.feature_extraction.text import TfidfVectorizer
from mysql.connector.abstracts import MySQLConnectionAbstract


class Modules:
    def __init__(
            self
    ) -> None:
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

        self.transaction_id: int | None = None
        self.date: str = ''
        self.description: str | None = None
        self.value: float = 0.0
        self.counterparty: str | None = None
        self.method: str = ''
        self.banks_id: int = 0
        self.banks_name: str = ''
        self.cards_id: int | None = None
        self.category_name: str | None = None

        self.bill_line = False

    def analyze(
            self,
            line: str
    ) -> None:
        start: list[str] = [
            'movimentacoes na fatura',
            'na fatura'
        ]
        end: str = 'gustavo ribeiro silva'

        for text in start:
            if text in line:
                self.bill_line = True

        if end in line and self.bill_line:
            self.bill_line = False

    def fetch_transaction_id(
            self,
            line: str
    ) -> None:
        parts: list = line.split()

        self.transaction_id = int(
            str(
                parts[-5]
            )
        ) if len(parts) >= 5 and re.fullmatch(
            pattern=r'\d{9,}',
            string=parts[-5]
        ) else None

    def fetch_date(
            self,
            line: str
    ) -> None:
        if data_match := re.match(
                pattern=r'(\d{2})-(\d{2})-(\d{4})',
                string=line
        ):
            date_obj = datetime.strptime(data_match[0], '%d-%m-%Y').date()
            self.date = date_obj.strftime('%Y-%m-%d')

        data_match = re.match(
            pattern=r'(\d{2}\/\d{2})',
            string=line
        )

        if self.bill_line and data_match:
            split_line = line.split()
            current_year = datetime.now().year
            date_str = f'{split_line[0]}/{current_year}'
            date_obj = datetime.strptime(
                '%d/%m/%Y',
                date_str
            ).date()
            self.date = date_obj.strftime('%Y-%m-%d')

    def fetch_description(
            self,
            line: str
    ) -> None:
        if 'rendimentos' in line:
            self.description = 'MERCADO PAGO RETURN FROM CDB INVESTMENT'

    def fetch_value(
            self,
            line: str
    ) -> None:
        parts = line.split()
        if re.search(
                string=line,
                pattern=r'[r]\$\s?-?\s?\d{1,}\.?\d{2}'
        ):
            brute_value: str = parts[-1].replace(
                'r$',
                ''
            ).replace(
                '.',
                ''
            ).replace(
                ',',
                '.'
            )
            self.value = float(brute_value)

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
            match_counterparty = (
                    re.search(
                        pattern='transferência pix (?:enviada|recebida) (.+?)\s+\d{9,}',
                        string=line
                    ) or
                    re.search(
                        pattern=r'pagamento com qr pix (.+?)\s+\d{9,}',
                        string=line
                    )
            )

            if match_counterparty and not self.bill_line:
                self.counterparty = match_counterparty.group(1).upper()

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
                pattern=r'\[[“\"»]*\*\d{4}\]',
                string=line
        ) and conn:
            cursor = conn.cursor(
                dictionary=True
            )
            cursor.execute(
                'SELECT id FROM cards'
            )
            cards = cursor.fetchall()

            for card in cards:
                last_digits = re.findall(
                    pattern=r'\d{4}',
                    string=line
                )[-1]
                card_id = card['id'] if isinstance(
                    card,
                    dict
                ) else card[0]
                card_number = str(
                    card_id
                )

                if card_number.endswith(last_digits):
                    self.cards_id = int(
                        str(
                            card_id
                        )
                    )
                    break
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
        self.banks_id = 5968138149
        self.banks_name = 'Mercado Pago'

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

        if (
            (bank_parameter == 'mp_bill' and not self.bill_line) or
            (bank_parameter == 'mp_bill' and self.date == "") or
            self.value == 0.0
        ):
            processed = None

        self.transaction_id = None
        self.date = ""
        self.description = None
        self.value = 0.0
        self.counterparty = None
        self.method = ""
        self.banks_id = 0
        self.banks_name = ""
        self.cards_id = None if not self.bill_line else self.cards_id
        self.category_name = None

        return processed


def refactor(
        args: str,
        runtype: str
) -> list[str] | None:
    if runtype != 'mp_bill':
        lines: list[str] = args.split('\n')
        result: list[str] | None = []
        i = 0

        while i < len(lines):
            line = lines[i].strip()
            if re.match(
                    pattern=r'\d{2}-\d{2}-\d{4}',
                    string=line
            ):
                result.append(line)
                i += 1
            elif i + 2 < len(lines):
                data_block = lines[i + 1].strip()
                desc_block = f'{line.strip()} {lines[i + 2].strip()}'
                if match := re.search(
                        pattern=r'(\d{2}-\d{2}-\d{4})\s+(\d{9,})\s+r\$.*',
                        string=data_block
                ):
                    data = match[1]
                    trans_id = match[2]
                    rest = data_block.replace(data, '').replace(trans_id, '').strip()
                    line_final = f'{data} {desc_block} {trans_id} {rest}'
                    result.append(line_final)

                    i += 3
                else:
                    i += 1
            else:
                i += 1
        return result
    return None


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

    result = refactor(
        args=brute_text,
        runtype=runtype
    )
    for line in result if result else brute_text.split('\n'):
        steps = [
            'fetch_transaction_id',
            'fetch_date',
            'fetch_value',
            'fetch_description',
            'fetch_counterparty',
            'fetch_method',
            'fetch_category'
        ]

        if runtype == 'mp_bill':
            __modules.analyze(
                line=line
            )
            __modules.fetch_cards_id(
                line=line,
                conn=conn
            ) if conn else None
        if (
                (runtype == 'mp_bill' and __modules.bill_line) or
                (runtype == 'mp_common' and not __modules.bill_line)
        ):
            for step in steps:
                getattr(
                    __modules,
                    step
                )(
                    line
                )

        movement = __modules.finish(
            bank_parameter=runtype
        )

        if movement:
            __movements.append(movement)

    id_counts = defaultdict(int)
    for m in __movements:
        if tid := m['transaction_id']:
            id_counts[tid] += 1

    dup_counts = defaultdict(int)
    for m in __movements:
        tid = m['transaction_id']
        if tid and id_counts[tid] > 1:
            dup_counts[tid] += 1
            m['transaction_id'] = f'{tid}-{dup_counts[tid]}'

    return __movements