import re
from bs4 import BeautifulSoup
from unidecode import unidecode
from typing import Union, List, Dict

from .search_report import SearchReport
from src.config import PERIOD


class MonthlyMethods:

    async def get_dates(self, cells: list) -> dict:

        dates = {"date1": "", "date2": "", "date3": "", "date4": "", "date5": ""}
        for row in cells:
            address = row.get('address')
            value = row.get('value')
            if address == 'K1' and 'finance year' in value and 'edited' in value:
                dates['year_end_to_dates'].append({'value': await self.get_year_end_to_date(value), 'letter': 'K'})
            elif address == "N1" and 'one month' in value:
                dates['month_end_to_dates'].append({'value': await self.get_month_end_to_date(value), 'letter': 'N'})
            elif address == 'Q1' and 'finance year' in value:
                dates['year_end_to_dates'].append({'value': await self.get_year_end_to_date(value), 'letter': 'Q'})
            elif address == "T1" and 'finance year' in value:
                dates['year_end_to_dates'].append({'value': await self.get_year_end_to_date(value), 'letter': 'T'})
            elif address == "V1" and 'finance year' in value and 'predict' in value:
                dates['predict_year_end_to_dates'].append(
                    {'value': await self.get_year_end_to_date(value), 'letter': 'V'})

        return dates

    @staticmethod
    async def get_year_end_to_date(value: str):
        if "date" in value:
            year_end_to_date = value.split("date")[1].strip()
        else:
            year_end_to_date = value.split("fiance year")[1].strip()
        year_end_to_date = year_end_to_date.replace("/", "-")
        return year_end_to_date

    @staticmethod
    async def get_month_end_to_date(value: str):
        month_end_to_date = value.split('end to')[1].strip()
        month_end_to_date = month_end_to_date.replace("/", "-")
        return month_end_to_date

    @staticmethod
    async def find_titles(cells: list, category: int = None, monthly=False) -> list:
        industries = []
        for row in cells:
            address = row.get('address')
            value = row.get('value')
            if address[0] == 'A' and address[1:] != "1" and value != "":
                if category is not None:
                    if row.get('category') == category:
                        industries.append(address[1:])
                else:
                    industries.append(address[1:])

        if monthly:
            industries = sorted([int(x) for x in industries])
            industries = industries[1:-1]
        return industries

    @staticmethod
    async def add_report_type(key: str, total: dict) -> dict:
        if "monthly" in key:
            total["report_type"] = '1 MONTH'
        else:
            total["report_type"] = 'SEVERAL MONTHS'
        return total

    @staticmethod
    async def find_is_predict(value: str):
        return "predict" in value

    @classmethod
    async def get_monthly_total(cls, items: list, report: dict, is_predict: bool, date_key: str = None,
                                first_descriptive=False) -> dict:
        char_fields = ['product', 'sent_date_time', 'publish_date_time', 'source_url', 'report_type', 'letter_type']
        total = {}
        for itm in items:
            for k, val in itm.items():
                if k not in char_fields:
                    if 'total' not in k:
                        key = f"total_{k}"
                    else:
                        key = k
                    if key not in total:
                        total[key] = 0

                    total[key] += float(val) if val is not None else float(0)
                    total[key] = round(total[key], 2)

        total["sent_date_time"] = unidecode(report.get('SentDateTime')).replace('/', '-').replace(' ', 'T')
        total["publish_date_time"] = unidecode(report.get('PublishDateTime')).replace('/', '-').replace(' ', 'T')
        total["source_url"] = f"https://www.stockcodal.com{report.get('Url')}"
        total['is_predict'] = is_predict
        total['report_type'] = 'SEVERAL MONTHS'
        if first_descriptive:
            total['letter_type'] = "n-10"
            total['source_url'] += '&SheetId=20'

        if len(items) == 0:
            total['total_num_of_production'] = 0.0
            total['total_num_of_sales'] = 0.0
            total['total_sales_rate'] = 0.0
            total['total_sales_amount'] = 0.0
            if first_descriptive:
                total['final_price'] = 0.0
                total['not_pure_profit'] = 0.0

        if date_key is not None:
            total = await cls.add_report_type(date_key, total)
        return total

    @staticmethod
    async def descriptive_one_get_monthly_date(value: str, soup: BeautifulSoup) -> Union[dict, None]:
        period = re.findall(r"[1-9]+", value.split('end to')[0].strip())[0]
        month_end_to_date = value.split('end to')[1].strip()
        if month_end_to_date == '':
            return None
        month_end_to_date = month_end_to_date.replace("/", "-")
        year_end_to_date = (await SearchReport.get_year_end_to_date(soup)).replace("/", "-")
        report = {'is_audited': True, 'period': PERIOD.get(period), 'period_end_to_date': month_end_to_date,
                  'year_end_to_date': year_end_to_date}
        return report

    @staticmethod
    async def descriptive_one_get_year_date(value: str, soup: BeautifulSoup) -> dict:
        period_end_to_date = re.findall(r"[0-9]+[/][0-9]+[/][0-9]+", value.split("fiance year")[1].strip())[0]
        period_end_to_date = period_end_to_date.replace("/", "-")
        year_end_to_date = (await SearchReport.get_year_end_to_date(soup)).replace("/", "-")
        report = {'is_audited': True, 'period': PERIOD.get("12"), 'period_end_to_date': period_end_to_date,
                  'year_end_to_date': year_end_to_date}
        return report

    @classmethod
    async def descriptive_one_get_statement(cls, cells: list, letter_one: str, letter_two: str) -> Union[list, None]:
        if cells is None:
            return None
        values = []
        titles = await cls.find_titles(cells)
        for ttl in titles:
            for cell in cells:
                address = cell.get('address')
                if address == f'{letter_one}{ttl}':
                    value: str = cell.get('value')
                    period_matches = re.findall(r"[1-9]+", value.split('end to')[0].strip())
                    if len(period_matches) != 0:
                        period = period_matches[0]
                        period = PERIOD.get(period)
                    else:
                        if 'finance year' in value.split('end to')[0]:
                            period = PERIOD.get('12')
                        else:
                            period = None
                    period_end_to_date = value.split('end to')[1].strip()
                    period_end_to_date = period_end_to_date.replace("/", "-")
                    values.append({'address': ttl, 'period': period, 'period_end_to_date': period_end_to_date})
                    break

            for cell in cells:
                address = cell.get('address')
                if address == f'{letter_two}{ttl}':
                    for item in values:
                        if item.get('address') == ttl:
                            value = cell.get('value')
                            if value != "":
                                item['text'] = value
                            else:
                                values.remove(item)

        return values

    @staticmethod
    async def get_descriptive_two_total(items: List[Dict], report: dict, _type: str) -> dict:
        total = {}
        exception_fields = ['production_total_price', 'adjust_total_price', 'beg_period_total_price',
                            'sales_total_price', 'end_period_total_price']
        for itm in items:
            for k, val in itm.items():
                if not isinstance(val, float) and not isinstance(val, int) and not val.isnumeric():
                    continue
                if 'total' not in k or k in exception_fields:
                    key = f"total_{k}"
                else:
                    key = k
                if key not in total:
                    total[key] = 0

                total[key] += float(val) if val is not None else float(0)
                total[key] = round(total[key], 2)

        total["sent_date_time"] = unidecode(report.get('SentDateTime')).replace('/', '-').replace(' ', 'T')
        total["publish_date_time"] = unidecode(report.get('PublishDateTime')).replace('/', '-').replace(' ', 'T')
        total["source_url"] = f"https://www.stockcodal.com{report.get('Url')}&SheetId=21"
        total['report_type'] = _type

        return total

    @staticmethod
    async def descriptive_get_report(soup, value: str, period) -> Union[dict, None, str]:
        report = {'is_audited': True}

        if period is None:
            if value is None:
                return None
            elif "finance year" in value:
                period = PERIOD.get("12")
            else:
                period = re.findall(r"[1-9]+", value.split("monthly")[0])[0]
                period = PERIOD.get(period)

        period_end_to_date_matches = re.findall(r"[0-9]+[/][0-9]+[/][0-9]+", value)
        if len(period_end_to_date_matches) == 0:
            if period == PERIOD.get("12"):
                period_end_to_date_jalali = await SearchReport.get_period_end_to_date(soup)
            else:
                return "empty date"
        else:
            period_end_to_date_jalali = period_end_to_date_matches[0]

        period_end_to_date = period_end_to_date_jalali.replace("/", "-")
        year_end_to_date_jalali = await SearchReport.get_year_end_to_date(soup)
        year_end_to_date = year_end_to_date_jalali.replace("/", "-")

        report['period'] = period
        report['period_end_to_date'] = period_end_to_date
        report['year_end_to_date'] = year_end_to_date

        return report

    @staticmethod
    async def descriptive_three_get_report(soup, value: str):
        report = {'is_audited': True}

        if value is None:
            return None
        elif "finance year" in value:
            period = PERIOD.get("12")
        elif 'monthly' in value:
            period = re.findall(r"[1-9]+", value.split("monthly")[0])[0]
            period = PERIOD.get(period)
        else:
            if await SearchReport.get_period(soup) == PERIOD.get("12"):
                period = PERIOD.get('12')

        period_end_to_date_matches = re.findall(r"[0-9]+[/][0-9]+[/][0-9]+", value)
        if len(period_end_to_date_matches) == 0:
            if period == PERIOD.get("12"):
                period_end_to_date_jalali = await SearchReport.get_period_end_to_date(soup)
            else:
                return "empty date"
        else:
            period_end_to_date_jalali = period_end_to_date_matches[0]

        period_end_to_date = period_end_to_date_jalali.replace("/", "-")
        year_end_to_date_jalali = await SearchReport.get_year_end_to_date(soup)
        year_end_to_date = year_end_to_date_jalali.replace("/", "-")

        report['period'] = period
        report['period_end_to_date'] = period_end_to_date
        report['year_end_to_date'] = year_end_to_date

        return report
