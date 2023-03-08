import json
from typing import Union, List
from unidecode import unidecode

from bs4 import BeautifulSoup

from ..config import (
    PERIOD,
    FIRST_DESCRIPTIVE_FINAL_COST_ADDRESS, FIRST_DESCRIPTIVE_FINAL_COST_TITLES,
    SALES_AND_COST_OVER_5_YEARS_ADDRESS, SALES_AND_COST_OVER_5_YEARS_TITLES,
    DESCRIPTIVE_TWO_TYPES,
)

from src.utils import (
    ConvertText,
    SearchReport,
    make_log,
    MonthlyMethods
)


class TransformDescriptiveTwo:
    """
    circulation_amount
    circulation_amount_est1
    circulation_amount_est2
    purchase_and_consumption
    purchase_and_consumption_est1
    purchase_and_consumption_est2
    estimate_of_change_in_purchase_price_of_raw_materials
    """

    def __init__(self, company: int, soup: BeautifulSoup, report: dict, logger, codal_company: dict):
        self.company = company
        self.soup = soup
        self.report = report
        self.logger = logger
        self.codal_company = codal_company

    async def make_extra_params(self):
        sent_date_time = unidecode(self.report.get('SentDateTime')).replace('/', '-').replace(' ', 'T')
        publish_date_time = unidecode(self.report.get('PublishDateTime')).replace('/', '-').replace(' ', 'T')
        source_url = f"https://www.stockcodal.com{self.report.get('Url')}&SheetId=21"

        return sent_date_time, publish_date_time, source_url

    @staticmethod
    async def get_tables(pure_data: dict) -> list:
        circulation_amount = None
        circulation_amount_est1 = None
        circulation_amount_est2 = None
        purchase_and_consumption = None
        purchase_and_consumption_est1 = None
        purchase_and_consumption_est2 = None
        estimate_of_change_in_purchase_price_of_raw_materials = None
        sheets = pure_data.get('sheets')[0]
        tables: list = sheets.get('tables')

        for item in tables:
            cells: list = item.get('cells')
            alias_name: str = item.get('aliasName')
            if alias_name == 'CirculationAmountRialCommodityInventory':
                circulation_amount = cells
            elif alias_name == 'CirculationAmountRialCommodityInventory-Est1':
                circulation_amount_est1 = cells
            elif alias_name == 'CirculationAmountRialCommodityInventory-Est2':
                circulation_amount_est2 = cells
            elif alias_name == 'PurchaseAndConsumptionOfNawMaterials':
                purchase_and_consumption = cells
            elif alias_name == 'PurchaseAndConsumptionOfNawMaterials-Est1':
                purchase_and_consumption_est1 = cells
            elif alias_name == 'PurchaseAndConsumptionOfNawMaterials-Est2':
                purchase_and_consumption_est2 = cells
            elif alias_name == 'CompanyEstimatesOfChangesInThePurchasePriceOfRawMaterials':
                estimate_of_change_in_purchase_price_of_raw_materials = cells

        return [circulation_amount, circulation_amount_est1, circulation_amount_est2, purchase_and_consumption,
                purchase_and_consumption_est1, purchase_and_consumption_est2,
                estimate_of_change_in_purchase_price_of_raw_materials]

    @staticmethod
    async def get_min_address(adds: list) -> int:
        min_address = 1000
        for cell in adds:
            address = int(cell.get('address')[1:])
            if address < min_address:
                min_address = address

        return min_address

    async def get_circulation_amount(self, circulation_amount: list, _type: str) -> Union[dict, None]:
        """
        product: A
        measure_unit
        Beginning_period_amount
            amount: C
            rate: D
            total_price: E
        Production:
            amount: F
            rate: G
            total_price: H
        Adjustments
            amount: I
            rate: J
            total_price: K
        Sales
            amount: L
            rate: M
            total_price: N
        End period amount
            amount: O
            rate: P
            total_price: Q
        """
        if circulation_amount is None:
            return None

        titles = await MonthlyMethods.find_titles(circulation_amount, monthly=True)
        items = []
        total_fields = ["beg_period_amount", "beg_period_rate", "beg_period_total_price", "production_amount",
                        "production_rate", "production_total_price", "adjust_amount", "adjust_rate",
                        "adjust_total_price", "sales_amount", "sales_rate", "sales_total_price", "end_period_amount",
                        "end_period_rate", "end_period_total_price"]
        for title in titles:
            items.append({
                'product': (await SearchReport.get_value(circulation_amount, f'A{title}')).strip(),
                'measure_unit': (await SearchReport.get_value(circulation_amount, f'B{title}')).strip(),
                'beg_period_amount': await SearchReport.get_value(circulation_amount, f'C{title}'),
                'beg_period_rate': await SearchReport.get_value(circulation_amount, f'D{title}'),
                'beg_period_total_price': await SearchReport.get_value(circulation_amount, f'E{title}'),
                'production_amount': await SearchReport.get_value(circulation_amount, f'F{title}'),
                'production_rate': await SearchReport.get_value(circulation_amount, f'G{title}'),
                'production_total_price': await SearchReport.get_value(circulation_amount, f'H{title}'),
                'adjust_amount': await SearchReport.get_value(circulation_amount, f'I{title}'),
                'adjust_rate': await SearchReport.get_value(circulation_amount, f'J{title}'),
                'adjust_total_price': await SearchReport.get_value(circulation_amount, f'K{title}'),
                'sales_amount': await SearchReport.get_value(circulation_amount, f'L{title}'),
                'sales_rate': await SearchReport.get_value(circulation_amount, f'M{title}'),
                'sales_total_price': await SearchReport.get_value(circulation_amount, f'N{title}'),
                'end_period_amount': await SearchReport.get_value(circulation_amount, f'O{title}'),
                'end_period_rate': await SearchReport.get_value(circulation_amount, f'P{title}'),
                'end_period_total_price': await SearchReport.get_value(circulation_amount, f'Q{title}')
            })
        for num, item in enumerate(items):
            items[num] = {k: round(float(v), 2) if k not in ['product', 'measure_unit'] else v for k, v in item.items()}
        total = await MonthlyMethods.get_descriptive_two_total(items, self.report, _type)
        if not any([isinstance(item, float) for item in total.values()]):
            for field in total_fields:
                total[f'total_{field}'] = float(0)
        # for cell in ["C2", "C8", "C14"]:
        #     value = await SearchReport.get_value(circulation_amount, cell)
        #     if value is not None:
        #         break

        value = await SearchReport.get_value(circulation_amount, f'C{await self.get_min_address(circulation_amount)}')

        report = await MonthlyMethods.descriptive_get_report(self.soup, value, None)
        return {'total': total, 'items': items, 'report': report}

    async def get_purchase_and_consumption_section(self, purchase_and_consumption: list, _type: str,
                                                   category: int) -> dict:
        total_fields = ['beg_period_amount', 'beg_period_rate', 'beg_period_total_price', 'period_purch_amount',
                        'period_purch_rate', 'period_purch_price', 'consump_amount', 'consump_rate', 'consump_price',
                        'end_period_amount', 'end_period_rate', 'end_period_price']
        titles = await MonthlyMethods.find_titles(purchase_and_consumption, category=category, monthly=True)
        items = []
        for title in titles:
            items.append({
                'product': await SearchReport.get_value(purchase_and_consumption, f'A{title}'),
                'measure_unit': await SearchReport.get_value(purchase_and_consumption, f'B{title}'),
                'beg_period_amount': await SearchReport.get_value(purchase_and_consumption, f'C{title}'),
                'beg_period_rate': await SearchReport.get_value(purchase_and_consumption, f'D{title}'),
                'beg_period_total_price': await SearchReport.get_value(purchase_and_consumption, f'E{title}'),
                'period_purch_amount': await SearchReport.get_value(purchase_and_consumption, f'F{title}'),
                'period_purch_rate': await SearchReport.get_value(purchase_and_consumption, f'G{title}'),
                'period_purch_price': await SearchReport.get_value(purchase_and_consumption, f'H{title}'),
                'consump_amount': await SearchReport.get_value(purchase_and_consumption, f'I{title}'),
                'consump_rate': await SearchReport.get_value(purchase_and_consumption, f'J{title}'),
                'consump_price': await SearchReport.get_value(purchase_and_consumption, f'K{title}'),
                'end_period_amount': await SearchReport.get_value(purchase_and_consumption, f'L{title}'),
                'end_period_rate': await SearchReport.get_value(purchase_and_consumption, f'M{title}'),
                'end_period_price': await SearchReport.get_value(purchase_and_consumption, f'N{title}'),
            })
        for num, item in enumerate(items):
            items[num] = {k: round(float(v), 2) if k not in ['product', 'measure_unit'] else v for k, v in item.items()}
        total = await MonthlyMethods.get_descriptive_two_total(items, self.report, _type)
        if not any([isinstance(item, float) for item in total.values()]):
            for field in total_fields:
                total[f'total_{field}'] = float(0)

        return {'total': total, 'items': items}

    async def get_purchase_and_consumption(self, purchase_and_consumption: list, _type: str) -> Union[dict, None]:
        """
         category = 1
         category = 2
        ----------------------------
        product: A
        measure_unit: B
        beginning period:
            amount: C
            rate: D
            price: E
        period_purchase:
            amount: F
            rate: G
            price: H
        consumption:
            amount: I
            rate: J
            price: K
        End period:
            amount: L
            rate: M
            price: N
        """

        if purchase_and_consumption is None:
            return None

        domestics = await self.get_purchase_and_consumption_section(purchase_and_consumption, _type, 1)
        imports = await self.get_purchase_and_consumption_section(purchase_and_consumption, _type, 2)
        total = await MonthlyMethods.get_descriptive_two_total([imports.get('total'), domestics.get('total')],
                                                               self.report, _type)
        if not any([isinstance(item, float) for item in total.values()]):
            return None

        # for cell in [7, 8, 9, 19, 20, 35, 38, 39, 41, 51, 56, 57, 59, 62]:
        #     value = await SearchReport.get_value(purchase_and_consumption, f'C{cell}')
        #     if value is not None:
        #         break

        value = await SearchReport.get_value(purchase_and_consumption,
                                             f'C{await self.get_min_address(purchase_and_consumption)}')

        report = await MonthlyMethods.descriptive_get_report(self.soup, value, None)

        return {'total': total, 'domestics': domestics, 'imports': imports, 'report': report}

    async def get_raw_materials(self, raw_materials: list) -> Union[list, None]:
        if raw_materials is None:
            return None

        items = []
        sent_date_time, publish_date_time, source_url = await self.make_extra_params()
        titles = []
        for cell in raw_materials:
            address: str = cell.get('address')
            if address.startswith("A"):
                titles.append(address[1:])

        for ttl in titles:
            report = None
            text = None
            for cell in raw_materials:
                address: str = cell.get('address')
                if address == f'B{ttl}':
                    report = await MonthlyMethods.descriptive_get_report(self.soup, cell.get('value').strip(), None)
                elif address == f'C{ttl}':
                    text = cell.get('value').strip()

            if text != "":
                items.append({'report': report, 'text': text, 'sent_date_time': sent_date_time,
                              'publish_date_time': publish_date_time, 'source_url': source_url})

        return items

    @property
    async def result(self) -> Union[dict, None, str]:
        soup = self.soup
        result = {"codal_company": self.codal_company}

        if await SearchReport.descriptive_report_is_empty(soup):
            return "empty report"

        scripts = soup.find_all("script", type="text/javascript")
        try:
            try:
                response = scripts[12].text.split("var datasource = ")[1][:-7]
            except IndexError:
                response = scripts[6].text.split("var datasource = ")[1][:-7]
        except IndexError:
            try:
                response = scripts[7].text.split("var datasource = ")[1][:-7]
            except IndexError:
                response = scripts[7].text.split("var rawdatasource = ")[1][:-7]
        pure_data = json.loads(response)
        tbs: list = await self.get_tables(pure_data)

        result['circulation_amount'] = await self.get_circulation_amount(tbs[0], DESCRIPTIVE_TWO_TYPES.get('NORMAL'))
        result['circulation_amount_est1'] = await self.get_circulation_amount(tbs[1],
                                                                              DESCRIPTIVE_TWO_TYPES.get('EST1'))
        # result['circulation_amount_est1']['items'][0]['measure_unit'] = ''
        result['circulation_amount_est2'] = await self.get_circulation_amount(tbs[2],
                                                                              DESCRIPTIVE_TWO_TYPES.get('EST2'))
        result['purchase_and_consumption'] = await self.get_purchase_and_consumption(tbs[3], DESCRIPTIVE_TWO_TYPES.get(
            'NORMAL'))
        result['purchase_and_consumption_est1'] = await self.get_purchase_and_consumption(tbs[4],
                                                                                          DESCRIPTIVE_TWO_TYPES.get(
                                                                                              'EST1'))
        result['purchase_and_consumption_est2'] = await self.get_purchase_and_consumption(tbs[5],
                                                                                          DESCRIPTIVE_TWO_TYPES.get(
                                                                                              'EST2'))
        result['estimate_of_change_in_purchase_price_of_raw_materials'] = await self.get_raw_materials(tbs[6])

        return result


class TransformDescriptiveOne:

    def __init__(self, company: int, soup: BeautifulSoup, report: dict, logger, codal_company: dict):
        self.company = company
        self.soup = soup
        self.report = report
        self.logger = logger
        self.codal_company = codal_company

    async def make_extra_params(self):
        sent_date_time = unidecode(self.report.get('SentDateTime')).replace('/', '-').replace(' ', 'T')
        publish_date_time = unidecode(self.report.get('PublishDateTime')).replace('/', '-').replace(' ', 'T')
        source_url = f"https://www.stockcodal.com{self.report.get('Url')}&SheetId=20"

        return sent_date_time, publish_date_time, source_url

    @staticmethod
    async def get_tables(pure_data: dict) -> list:
        income_and_expense = None
        sales_and_cost = None
        final_cost = None
        further_strategies = None
        sale_rate_changes = None
        final_price_changes = None

        sheets = pure_data.get("sheets")
        tables = sheets[0].get("tables")
        for table in tables:
            alias_name = table.get("aliasName")
            cells = table.get('cells')
            if alias_name == "OperatingIncomeAndExpense":
                income_and_expense = cells
            elif alias_name == "SalesTrendAndCostOverTheLast5Years":
                sales_and_cost = cells
            elif alias_name == "TheCostOfTheSoldGoods":
                final_cost = cells
            elif alias_name == 'FutureManagementGoalsAndStrategies':
                further_strategies = cells
            elif alias_name == 'CompanyEstimatesOfChangesInTheRateOfSalesOfProducts':
                sale_rate_changes = cells
            elif alias_name == 'CompanyEstimatesOfChangesInTheCostOfTheSoldGoods':
                final_price_changes = cells

        return [income_and_expense, sales_and_cost, further_strategies, final_cost, sale_rate_changes,
                final_price_changes]

    async def get_statements_report(self) -> dict:
        report = dict()
        period_end_to_date = await SearchReport.get_period_end_to_date(self.soup)
        year_end_to_date = await SearchReport.get_year_end_to_date(self.soup)
        period = await SearchReport.get_period(self.soup)
        if period is None and period_end_to_date is not None:
            period = PERIOD.get("12")
        report['period'] = period
        report['period_end_to_date'] = period_end_to_date.replace("/", "-")
        report['year_end_to_date'] = year_end_to_date.replace("/", "-")
        report['is_audited'] = True
        return report

    async def get_statement(self, further_strategies: list) -> Union[dict, None]:
        result = {}
        sent_date_time, publish_date_time, source_url = await self.make_extra_params()
        if further_strategies is None:
            return None
        value = further_strategies[0].get('value')
        result['further_strategies'] = value
        result['sent_date_time'] = sent_date_time
        result['publish_date_time'] = publish_date_time
        result['source_url'] = source_url
        result['report'] = await self.get_statements_report()
        return result

    @staticmethod
    async def remove_extra_columns(result: list) -> list:
        for col1 in result[-2:]:
            period_end_to_date = col1.get('report').get('period_end_to_date')
            for col in result:
                if col.get('report').get('period_end_to_date') == period_end_to_date:
                    if col.get('total_sales_amount') == str(0) and col.get('final_price') == str(0):
                        result.remove(col)

        return result

    async def get_sales_and_cost_over_5years(self, sales_and_cost: list) -> list:

        result = []
        address_conf = await SearchReport.get_address(SALES_AND_COST_OVER_5_YEARS_ADDRESS, sales_and_cost)
        for letter in ["B", "C", "D", "E", "F", "G", "H", "I"]:
            value = await self.find_value_by_config(address_conf, SALES_AND_COST_OVER_5_YEARS_TITLES,
                                                    sales_and_cost, letter, self.report)
            if value is not None and value != "empty":
                result.append(value)

        result = await self.remove_extra_columns(result)

        for item in result:
            if item['sales_amount'] == '0' and item['final_price'] == '0':
                result.remove(item)

        return result

    async def find_value_by_config(self, address_conf: dict, titles_conf: list, table: list, base_address: str,
                                   report: dict) -> Union[dict, None, str]:
        is_predict = False
        result = {}
        for enum, (key, address) in enumerate(zip(titles_conf, address_conf.values())):
            if address == "":
                result[key] = ""
                continue
            for row in table:
                if row.get('address') == f'{base_address}{address}':
                    value = row.get('value')
                    if 'predict' in value:
                        is_predict = True
                    result[key] = value if value != "" else "0"
                    break

        if "monthly" in result.get('report'):
            period = None
        else:
            period = PERIOD.get("12")

        result['report'] = await MonthlyMethods.descriptive_get_report(self.soup, result.get('report'), period)
        if result['report'] is None:
            return None
        elif result['report'] == "empty date":
            return "empty"

        result["sent_date_time"] = unidecode(report.get('SentDateTime')).replace('/', '-').replace(' ', 'T')
        result["publish_date_time"] = unidecode(report.get('PublishDateTime')).replace('/', '-').replace(' ', 'T')
        result["source_url"] = f"https://www.codal.ir{report.get('Url')}&SheetId=20"

        result['is_predict'] = is_predict

        return result

    async def get_final_cost(self, final_cost: list) -> list:
        """
        Some reports have 2 columns of data and some others have 3 columns and some others have 4 columns.
        Reports with 2 columns have A, B and C chars of data.
        But reports with 3 columns have A, B, C and D chars
        """
        result = []

        address_conf = await SearchReport.get_address(FIRST_DESCRIPTIVE_FINAL_COST_ADDRESS, final_cost)
        titles_conf = FIRST_DESCRIPTIVE_FINAL_COST_TITLES
        result.append(
            await self.find_value_by_config(address_conf, titles_conf, final_cost, "B", self.report))
        result.append(await self.find_value_by_config(address_conf, titles_conf, final_cost, "C", self.report))
        if await SearchReport.get_value(final_cost, "D30") is not None:
            result.append(await self.find_value_by_config(address_conf, titles_conf, final_cost, "D", self.report))
        if await SearchReport.get_value(final_cost, "E30") is not None:
            result.append(await self.find_value_by_config(address_conf, titles_conf, final_cost, "E", self.report))

        result = await self.to_int_result(result)

        for item in result:
            if all([x == 0 or x is None for k, x in item.items() if
                    k not in ['report', 'sent_date_time', 'publish_date_time', 'source_url', 'is_predict']]):
                result.remove(item)
        return result

    @staticmethod
    async def to_int_result(result: list):
        for item in result:
            for k, v in item.items():
                if k not in ['report', 'sent_date_time', 'publish_date_time', 'source_url', 'is_predict']:
                    item[k] = int(v) if v != "" else None

        return result

    @staticmethod
    async def find_income_and_expense_dates_address(letter: str, cells: list) -> str:
        value = await SearchReport.get_value(cells, f'{letter}2')
        if value is not None:
            return value

    @staticmethod
    async def get_income_and_expense_item(cell_ad: str, income_and_expense: list, first_letter: str, date_key: str,
                                          next_letter: Union[str, None], is_single: bool) -> dict:
        second_letter = chr(ord(first_letter) + 1).upper()

        product = ConvertText.fix_company_name(await SearchReport.get_value(income_and_expense, f'A{cell_ad}'))

        if date_key in ["date2", "date3"]:
            third_letter = chr(ord(second_letter) + 1).upper()
            fourth_letter = chr(ord(third_letter) + 1).upper()
            fifth_letter = chr(ord(fourth_letter) + 1).upper()

            if ord(next_letter) - ord(fifth_letter) - 1 != 0:
                sixth_letter = chr(ord(fifth_letter) + 1).upper()

                num_of_production = float(ConvertText.fa_num_to_eng(
                    await SearchReport.get_value(income_and_expense, f'{first_letter}{cell_ad}')))
                num_of_sales = float(ConvertText.fa_num_to_eng(
                    await SearchReport.get_value(income_and_expense, f'{second_letter}{cell_ad}')))
                sales_rate = float(ConvertText.fa_num_to_eng(
                    await SearchReport.get_value(income_and_expense, f'{third_letter}{cell_ad}')))
                sales_amount = float(ConvertText.fa_num_to_eng(
                    await SearchReport.get_value(income_and_expense, f'{fourth_letter}{cell_ad}')))
                final_price = float(ConvertText.fa_num_to_eng(
                    await SearchReport.get_value(income_and_expense, f'{fifth_letter}{cell_ad}')))
                not_pure_profit = float(ConvertText.fa_num_to_eng(
                    await SearchReport.get_value(income_and_expense, f'{sixth_letter}{cell_ad}')))
            else:
                num_of_production = None
                num_of_sales = float(ConvertText.fa_num_to_eng(
                    await SearchReport.get_value(income_and_expense, f'{first_letter}{cell_ad}')))
                sales_rate = float(ConvertText.fa_num_to_eng(
                    await SearchReport.get_value(income_and_expense, f'{second_letter}{cell_ad}')))
                sales_amount = float(ConvertText.fa_num_to_eng(
                    await SearchReport.get_value(income_and_expense, f'{third_letter}{cell_ad}')))
                final_price = float(ConvertText.fa_num_to_eng(
                    await SearchReport.get_value(income_and_expense, f'{fourth_letter}{cell_ad}')))
                not_pure_profit = float(ConvertText.fa_num_to_eng(
                    await SearchReport.get_value(income_and_expense, f'{fifth_letter}{cell_ad}')))

        elif date_key == "date1":
            num_of_production = None
            num_of_sales = float(ConvertText.fa_num_to_eng(
                await SearchReport.get_value(income_and_expense, f'{first_letter}{cell_ad}')))
            sales_rate = None
            sales_amount = float(ConvertText.fa_num_to_eng(
                await SearchReport.get_value(income_and_expense, f'{second_letter}{cell_ad}')))
            final_price = None
            not_pure_profit = None

        else:  # date4
            if is_single:
                num_of_production = None
                num_of_sales = float(ConvertText.fa_num_to_eng(
                    await SearchReport.get_value(income_and_expense, f'{first_letter}{cell_ad}')))
                sales_rate = None
                sales_amount = None
                final_price = None
                not_pure_profit = None
            else:
                third_letter = chr(ord(second_letter) + 1).upper()
                fourth_letter = chr(ord(third_letter) + 1).upper()

                num_of_production = float(ConvertText.fa_num_to_eng(
                    await SearchReport.get_value(income_and_expense, f'{first_letter}{cell_ad}')))
                num_of_sales = float(ConvertText.fa_num_to_eng(
                    await SearchReport.get_value(income_and_expense, f'{second_letter}{cell_ad}')))
                sales_rate = float(
                    ConvertText.fa_num_to_eng(
                        await SearchReport.get_value(income_and_expense, f'{third_letter}{cell_ad}')))
                sales_amount = float(ConvertText.fa_num_to_eng(
                    await SearchReport.get_value(income_and_expense, f'{fourth_letter}{cell_ad}')))
                final_price = None
                not_pure_profit = None

        return {"product": product,
                "num_of_production": round(num_of_production, 2) if num_of_production is not None else None,
                "num_of_sales": round(num_of_sales, 2),
                "sales_rate": round(sales_rate, 2) if sales_rate is not None else None,
                "sales_amount": round(sales_amount, 2) if sales_amount is not None else None,
                "final_price": round(final_price, 2) if final_price is not None else None,
                "not_pure_profit": round(not_pure_profit, 2) if not_pure_profit is not None else None}

    async def get_income_and_expense_dates(self, income_and_expense: list) -> dict:
        dates = dict()
        date1 = await self.find_income_and_expense_dates_address("E", income_and_expense)
        letter = "E"
        if date1 is None:
            date1 = await self.find_income_and_expense_dates_address("D", income_and_expense)
            letter = "D"
        dates['date1'] = {
            'report': await MonthlyMethods.descriptive_one_get_monthly_date(date1, self.soup),
            'letter': letter, 'is_predict': await MonthlyMethods.find_is_predict(date1)}
        date2 = await self.find_income_and_expense_dates_address("G", income_and_expense)
        letter = "G"
        if date2 is None:
            date2 = await self.find_income_and_expense_dates_address("F", income_and_expense)
            letter = "F"
        dates['date2'] = {'report': await MonthlyMethods.descriptive_one_get_year_date(date2, self.soup),
                          'letter': letter, 'is_predict': await MonthlyMethods.find_is_predict(date2)}
        date3 = await self.find_income_and_expense_dates_address("M", income_and_expense)
        letter = "M"
        if date3 is None:
            date3 = await self.find_income_and_expense_dates_address("K", income_and_expense)
            letter = "K"
        dates['date3'] = {
            'report': await MonthlyMethods.descriptive_one_get_monthly_date(date3, self.soup),
            'letter': letter, 'is_predict': await MonthlyMethods.find_is_predict(date3)}
        date4 = await self.find_income_and_expense_dates_address("S", income_and_expense)
        letter = "S"
        if date4 is None:
            date4 = await self.find_income_and_expense_dates_address("P", income_and_expense)
            letter = "P"
        report = await MonthlyMethods.descriptive_one_get_monthly_date(date4, self.soup)
        dates['date4'] = {
            'report': report,
            'letter': letter,
            'is_single': "sale num" in date4,
            'is_predict': await MonthlyMethods.find_is_predict(date4)}
        date5 = await self.find_income_and_expense_dates_address("Q", income_and_expense)
        letter = "Q"
        if date5 is None:
            date5 = await self.find_income_and_expense_dates_address("W", income_and_expense)
        if date5 is not None:
            if 'monthly' in date5:
                report = await MonthlyMethods.descriptive_one_get_monthly_date(date5, self.soup)
            else:
                report = await MonthlyMethods.descriptive_one_get_year_date(date5, self.soup)
            dates['date5'] = {
                'report': report,
                'letter': letter,
                'is_single': "sales num" in date5,
                'is_predict': await MonthlyMethods.find_is_predict(date5)
            }

        return dates

    async def get_income_and_expense_items(self, titles: list, cells: list, letter: str, date_key: str,
                                           next_letter: str, is_single: bool, is_predict: bool) -> dict:
        items = []
        for add in titles:
            items.append(await self.get_income_and_expense_item(add, cells, letter, date_key, next_letter, is_single))
        total = await MonthlyMethods.get_monthly_total(items, self.report, is_predict, first_descriptive=True)
        return {"items": items, "total": total}

    async def get_income_and_expense(self, income_and_expense: list) -> list:
        income_items = []

        dates = await self.get_income_and_expense_dates(income_and_expense)
        domestics = await MonthlyMethods.find_titles(income_and_expense, 1, monthly=True)
        exports = await MonthlyMethods.find_titles(income_and_expense, 2, monthly=True)
        service_incomes = await MonthlyMethods.find_titles(income_and_expense, 3, monthly=True)

        for num, (key, value) in enumerate(dates.items()):
            if value.get('report') is None:
                continue
            is_predict = dates[key]['is_predict']
            if num == len(dates) - 1:
                next_letter = None
            else:
                next_letter = dates[list(dates.keys())[num + 1]].get("letter")
            if key in ["date4", "date5"]:
                is_single = value.get("is_single")
            else:
                is_single = False
            domestic = await self.get_income_and_expense_items(domestics, income_and_expense, value.get('letter'), key,
                                                               next_letter, is_single, is_predict)
            export = await self.get_income_and_expense_items(exports, income_and_expense, value.get('letter'), key,
                                                             next_letter, is_single, is_predict)
            service_income = await self.get_income_and_expense_items(service_incomes, income_and_expense,
                                                                     value.get('letter'), key, next_letter, is_single,
                                                                     is_predict)
            total = await MonthlyMethods.get_monthly_total(
                [domestic["total"], export['total'], service_income['total']], self.report, is_predict,
                first_descriptive=True)
            income_items.append({"report": value.get('report'), "domestic": domestic, "export": export,
                                 "service_income": service_income, "total": total})

        return income_items

    @property
    async def result(self) -> Union[str, dict, None]:
        soup = self.soup
        result = {"codal_company": self.codal_company}

        if await SearchReport.descriptive_report_is_empty(soup):
            return "empty report"

        scripts = soup.find_all("script", type="text/javascript")
        try:
            try:
                response = scripts[12].text.split("var datasource = ")[1][:-7]
            except IndexError:
                response = scripts[6].text.split("var datasource = ")[1][:-7]
        except IndexError:
            try:
                response = scripts[7].text.split("var datasource = ")[1][:-7]
            except IndexError:
                response = scripts[7].text.split("var rawdatasource = ")[1][:-7]
        pure_data = json.loads(response)
        tbs = await self.get_tables(pure_data)
        income_and_expense, sales_and_cost, further_strategies, final_cost, sale_rate_changes, final_price_changes = tbs
        if all([item is None for item in tbs]):
            return None
        result['final_cost'] = await self.get_final_cost(final_cost)
        result['statement'] = await self.get_statement(further_strategies)
        result['sale_rate_changes'] = await MonthlyMethods.descriptive_one_get_statement(sale_rate_changes, "B", "C")
        result['final_price_changes'] = await MonthlyMethods.descriptive_one_get_statement(final_price_changes, "A",
                                                                                           "B")
        result['sales_and_cost'] = await self.get_sales_and_cost_over_5years(sales_and_cost)
        result['income_and_expense'] = await self.get_income_and_expense(income_and_expense)
        return result


class TransformMonthlyActivity:
    """
   Energy table:
   category: 0

   **If a field is not in the report, it should not be set as None. Because update_or_create method will update that
   field to null. So put nothing for that field.
   """

    def __init__(self, company: int, soup: BeautifulSoup, report: dict, logger, codal_company: dict):
        self.company = company
        self.soup = soup
        self.report = report
        self.logger = logger
        self.codal_company = codal_company

    @staticmethod
    async def find_tables(tables):
        produce_cells = None
        energy_cells = None
        for table in tables:
            alias_name = table.get('aliasName')
            if alias_name == 'ProductionAndSales':
                produce_cells = table.get('cells')
            elif alias_name == 'Energy':
                energy_cells = table.get('cells')

        return produce_cells, energy_cells

    async def get_report(self, date_val: dict, date_key: str) -> dict:
        """
        date_val.get("val") in here is already converted to milady date time. Do not convert it again.
        """
        if date_key in ["last_month_yearly", "this_month_monthly", "this_month_yearly"]:
            year_end_to_date = (await SearchReport.get_year_end_to_date(self.soup)).replace("/", "-")
            period_end_to_date = date_val.get("val")
        else:
            this_year = (await SearchReport.get_year_end_to_date(self.soup)).replace("/", "-")
            last_year_end_to_date = f'{str(int(this_year.split("-")[0]) - 1)}-{this_year.split("-")[1]}-' \
                                    f'{this_year.split("-")[2]}'
            year_end_to_date = last_year_end_to_date
            period_end_to_date = date_val.get("val")

        return {"is_audited": True, "period_end_to_date": period_end_to_date, "year_end_to_date": year_end_to_date,
                "company": self.company}

    @staticmethod
    async def get_energy_dates(cells: list) -> dict:
        dates = {"last_month_yearly": {}, "this_month_monthly": {}, "this_month_yearly": {}, "last_year_yearly": {},
                 "predict_year_yearly": {}}
        value1 = await SearchReport.get_value(cells, "K1")
        dates['last_month_yearly'] = {'val': await MonthlyMethods.get_year_end_to_date(value1), 'letter': 'K'}
        value2 = await SearchReport.get_value(cells, "N1")
        dates['this_month_monthly'] = {'val': await MonthlyMethods.get_month_end_to_date(value2), 'letter': 'N'}
        value3 = await SearchReport.get_value(cells, "Q1")
        dates['this_month_yearly'] = {'val': await MonthlyMethods.get_year_end_to_date(value3), 'letter': 'Q'}
        value4 = await SearchReport.get_value(cells, "T1")
        dates['last_year_yearly'] = {'val': await MonthlyMethods.get_year_end_to_date(value4), 'letter': 'T'}
        value5 = await SearchReport.get_value(cells, "V1")
        dates['predict_year_yearly'] = {'val': await MonthlyMethods.get_year_end_to_date(value5), 'letter': 'V'}

        return dates

    async def get_energy_items(self, industries: list, cells: list, dates: dict) -> list:
        energy_items = []
        for key, value in dates.items():
            item_report = await self.get_report(value, key)
            items = []
            for address in industries:
                items.append(await self.get_energy_item(address, key, value.get('letter'), cells))
            total = await self.get_energy_total(items, key)
            total["report_type"] = '1 MONTH' if "monthly" in key else "SEVERAL MONTHS"
            total["is_predict_year_end_to_date"] = bool(key == "predict_year_yearly")
            energy_items.append({"total": total, "report": item_report, "items": items})

        return energy_items

    @staticmethod
    async def get_energy_item(address: int, date_key: str, letter: str, cells: list) -> dict:
        industry = await SearchReport.get_value(cells, f'A{address}')
        category = await SearchReport.get_value(cells, f'B{address}')
        energy_type = await SearchReport.get_value(cells, f'C{address}')
        measure_unit = await SearchReport.get_value(cells, f'D{address}')

        first_letter = letter
        second_letter = chr(ord(first_letter) + 1).upper()
        third_letter = chr(ord(second_letter) + 1).upper()

        consumption_rate = ConvertText.fa_num_to_eng(await SearchReport.get_value(cells, f'{first_letter}{address}'))
        rate = ConvertText.fa_num_to_eng(
            await SearchReport.get_value(cells, f'{second_letter}{address}')) if date_key not in [
            "last_year_yearly", "predict_year_yearly"] else None
        price_amount = ConvertText.fa_num_to_eng(
            await SearchReport.get_value(cells,
                                         f'{third_letter}{address}')) if date_key != "predict_year_yearly" else None

        result = {'industry': industry, 'category': category, 'energy_type': energy_type, 'measure_unit': measure_unit,
                  'consumption_rate': round(float(consumption_rate), 2)}
        if rate is not None:
            result['rate'] = round(float(rate), 2)
        if price_amount is not None:
            result['price_amount'] = round(float(price_amount), 2)

        return result

    async def get_energy_total(self, items: list, date_key: str) -> dict:
        char_fields = ['industry', 'category', 'energy_type', 'measure_unit']
        total = {}
        for itm in items:
            for k, val in itm.items():
                if k not in char_fields:
                    key = f'total_{k}'
                    if key not in total:
                        total[key] = 0
                    if val is not None:
                        total[key] += float(val)
                        total[key] = round(total[key], 2)

        total['sent_date_time'] = unidecode(self.report.get('SentDateTime')).replace('/', '-').replace(' ', 'T')
        total['publish_date_time'] = unidecode(self.report.get('PublishDateTime')).replace('/', '-').replace(' ', 'T')
        total['source_url'] = f"https://www.stockcodal.com{self.report.get('Url')}"

        if len(items) == 0:
            total['total_consumption_rate'] = 0.0
            total['total_rate'] = 0.0
            total['total_price_amount'] = 0.0

        total = await MonthlyMethods.add_report_type(date_key, total)
        return total

    async def get_energy_table(self, cells: list) -> Union[list, None]:
        if cells is None:
            return None
        industries = await MonthlyMethods.find_titles(cells)
        dates = await self.get_energy_dates(cells)
        return await self.get_energy_items(industries, cells, dates)

    @staticmethod
    async def find_monthly_dates_address(letter: str, cells: list) -> str:
        for address in ["1", "13", "31", "9"]:
            value = await SearchReport.get_value(cells, f'{letter}{address}')
            if value is not None and not value.isnumeric():
                return value

    async def get_monthly_dates(self, cells: list) -> dict:
        dates = {"last_month_yearly": {}, "this_month_monthly": {}, "this_month_yearly": {}, "last_year_yearly": {}}
        value1 = await self.find_monthly_dates_address("J", cells)
        dates['last_month_yearly'] = {'val': await MonthlyMethods.get_year_end_to_date(value1), 'letter': 'J',
                                      'is_predict': await MonthlyMethods.find_is_predict(value1)}
        value2 = await self.find_monthly_dates_address("N", cells)
        dates['this_month_monthly'] = {'val': await MonthlyMethods.get_month_end_to_date(value2), 'letter': 'N',
                                       'is_predict': await MonthlyMethods.find_is_predict(value2)}
        value3 = await self.find_monthly_dates_address("R", cells)
        dates['this_month_yearly'] = {'val': await MonthlyMethods.get_year_end_to_date(value3), 'letter': 'R',
                                      'is_predict': await MonthlyMethods.find_is_predict(value3)}
        value4 = await self.find_monthly_dates_address("V", cells)
        dates['last_year_yearly'] = {'val': await MonthlyMethods.get_year_end_to_date(value4), 'letter': 'V',
                                     'is_predict': await MonthlyMethods.find_is_predict(value4)}

        return dates

    async def get_monthly_items(self, titles: list, cells: list, date_key: str, date_val: dict,
                                is_predict: bool) -> dict:
        items = []
        for add in titles:
            items.append(await self.get_monthly_item(add, cells, date_val))
        total = await MonthlyMethods.get_monthly_total(items, self.report, is_predict, date_key)
        total = await MonthlyMethods.add_report_type(date_key, total)
        return {"items": items, "total": total}

    @staticmethod
    async def get_monthly_item(cell_ad: Union[str, int], cells: list, date_val: dict) -> dict:
        first_letter = date_val.get('letter')
        second_letter = chr(ord(first_letter) + 1).upper()
        third_letter = chr(ord(second_letter) + 1).upper()
        fourth_letter = chr(ord(third_letter) + 1).upper()

        product = ConvertText.fix_company_name(await SearchReport.get_value(cells, f'A{cell_ad}'))
        num_of_production = float(
            ConvertText.fa_num_to_eng(await SearchReport.get_value(cells, f'{first_letter}{cell_ad}')))
        num_of_sales = float(
            ConvertText.fa_num_to_eng(await SearchReport.get_value(cells, f'{second_letter}{cell_ad}')))
        sales_rate = float(ConvertText.fa_num_to_eng(await SearchReport.get_value(cells, f'{third_letter}{cell_ad}')))
        sales_amount = float(
            ConvertText.fa_num_to_eng(await SearchReport.get_value(cells, f'{fourth_letter}{cell_ad}')))

        return {"product": product, "num_of_production": round(num_of_production, 2),
                "num_of_sales": round(num_of_sales, 2), "sales_rate": round(sales_rate, 2),
                "sales_amount": round(sales_amount, 2)}

    @staticmethod
    async def edit_first_col(cols: List[dict]) -> list:
        first_col = cols[0]
        for _value in first_col.get("total").values():
            if isinstance(_value, float):
                if _value != float(0):
                    return cols
        return cols[1:]

    async def get_monthly_table(self, cells: list) -> list:
        dates = await self.get_monthly_dates(cells)
        domestics = await MonthlyMethods.find_titles(cells, 1, monthly=True)
        exports = await MonthlyMethods.find_titles(cells, 2, monthly=True)
        service_incomes = await MonthlyMethods.find_titles(cells, 3, monthly=True)
        return_from_sales = await MonthlyMethods.find_titles(cells, 4, monthly=True)
        discounts = await MonthlyMethods.find_titles(cells, 5, monthly=True)

        monthly_items = []
        for key, val in dates.items():
            is_predict = val.get('is_predict')
            monthly_report = await self.get_report(val, key)
            domestic = await self.get_monthly_items(domestics, cells, key, val, is_predict)
            export = await self.get_monthly_items(exports, cells, key, val, is_predict)
            service_income = await self.get_monthly_items(service_incomes, cells, key, val, is_predict)
            return_from_sale = await self.get_monthly_items(return_from_sales, cells, key, val, is_predict)
            discount = await self.get_monthly_items(discounts, cells, key, val, is_predict)
            total = await MonthlyMethods.get_monthly_total([domestic["total"], export['total'], service_income['total'],
                                                            return_from_sale['total'], discount['total']], self.report,
                                                           is_predict, key)
            monthly_items.append({"report": monthly_report, "domestic": domestic, "export": export,
                                  "service_income": service_income, "return_from_sale": return_from_sale,
                                  "discount": discount, "total": total})

        return await self.edit_first_col(monthly_items)

    @property
    async def result(self) -> Union[dict, None, str]:
        try:
            soup = self.soup
            result = {"energy": [],
                      "monthly": [],
                      "codal_company": self.codal_company}
            if await SearchReport.monthly_activity_report_is_empty(soup):
                return "empty report"

            scripts = soup.find_all("script", type="text/javascript")
            try:
                try:
                    response = scripts[12].text.split("var datasource = ")[1][:-7]
                except IndexError:
                    response = scripts[6].text.split("var datasource = ")[1][:-7]
            except IndexError:
                response = scripts[7].text.split("var datasource = ")[1][:-7]
            pure_data = json.loads(response)
            tables = pure_data['sheets'][0]['tables']
            monthly_cells, energy_cells = await self.find_tables(tables)
            if monthly_cells is None and energy_cells is None:
                return None
            result["energy"] = await self.get_energy_table(energy_cells)
            result["monthly"] = await self.get_monthly_table(monthly_cells)
            return result
        except Exception as e:
            report_url = f'https://www.stockcodal.com{self.report.get("Url")}'
            await make_log(f'Error while getting monthly activity of {self.codal_company.get("company_name")}: {e}',
                           url=report_url, company=self.company)
            await self.logger.error(
                f'Error while getting monthly activity of {self.codal_company.get("company_name")}: {e}')
