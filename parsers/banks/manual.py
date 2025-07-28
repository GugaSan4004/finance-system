import re

from datetime import datetime
from mysql.connector.pooling import PooledMySQLConnection
from mysql.connector.abstracts import MySQLConnectionAbstract


class Modules:
    def __init__(
            self
    ) -> None:
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

    def ask_transaction_id(
            self
    ) -> None:
        if answer := int(input("transaction_id: ")):
            self.transaction_id = answer

    def ask_date(
            self
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

    def ask_description(
            self,
            line: str
    ) -> None:
        if 'rendimentos' in line:
            self.description = 'MERCADO PAGO RETURN FROM CDB INVESTMENT'

    def ask_value(
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

    def ask_counterparty(
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

    def ask_method(
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

    def ask_cards_id(
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
            cards = cursor.askall()

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

    def ask_category(
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
) -> dict[
    str,
    str | int | float | None
] | None:
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


def parse(
        runtype,
        args: list[str],
        conn: MySQLConnectionAbstract | PooledMySQLConnection | None
) -> dict[
    str,
    str | int | float | None
] | None:
    __modules = Modules()
    __movements: dict = {}

    steps = [
        'ask_transaction_id',
        'ask_date',
        'ask_value',
        'ask_description',
        'ask_counterparty',
        'ask_method',
        'ask_category'
    ]
    for step in steps:
        getattr(
            __modules,
            step
        )

    __movements = __modules.finish()

    return __movements