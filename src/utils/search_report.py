import json
from typing import Union
from bs4 import BeautifulSoup


class SearchReport:
    @staticmethod
    async def find_value_by_address(data, key, year3_symbol: str = None, keys_with_ads: dict = None,
                                    keys_for_ads: list = None, result=None):
        """
        This function loops inside the data param and finds the cell which its address is equal to the key param
        and based on the cell cssClass if it is not equal to dynamic_comp will return the value. Otherwise it will find
        the value from keys_for_ads param.
        If the key param is not a cell address it means it is a number and it will be returned.
        """
        if key[0] not in ["A", "B", "C", "D", "E", "F", "G", "H", "I", "J", "K"]:
            return key
        for item in data:
            if item.get('address') and item.get('address') == key:
                address = item['address']
                if item.get('cssClass') != 'dynamic_comp':
                    if item.get('value') == "":  # and (key == "A48" or key == "B48")
                        return '0'
                    else:
                        return item.get('value')
                else:
                    for ks, ls in keys_with_ads.items():
                        if address[1:] in ls:
                            index = list(keys_with_ads.keys()).index(ks)
                            title = keys_for_ads[index]
                            if address[0] == "B":  # for balance sheet and profit loss reports
                                return result['year1']['data'][title]
                            elif address[0] == "C":  # for balance sheet and profit loss reports
                                return result['year2']['data'][title]
                            elif address[0] == "E":  # for profit loss reports
                                if year3_symbol == "E":
                                    return result['year3']['data'][title]
                            elif address[0] == "D":
                                if year3_symbol == "D":
                                    return result['year3']['data'][title]
                            elif address[0] == "F":  # for balance sheet reports
                                return result['year1']['data'][title]
                            elif address[0] == "G":  # for balance sheet reports
                                return result['year2']['data'][title]

    @staticmethod
    async def portfolio_report_is_empty(soup: BeautifulSoup) -> Union[str, None]:
        """
        This function searches a portfolio report with soup parameter and determines if it is empty or not.
        if the report is empty it returns a string otherwise returns None.
        """
        select_div = soup.find("div", {"class": "ddlReportWrapper"})
        if select_div is None:
            return "empty report"
        ttl = select_div.find("select", {"id": "ctl00_ddlTable"})
        if ttl is None:
            ttl = select_div.find("select", {"id": "ddlTable"})
            if ttl is not None:
                ttl = ttl.find("option", {"selected": "selected"})
                if ttl.getText() == "balance sheet":
                    return "empty report"

    @staticmethod
    async def monthly_activity_report_is_empty(soup: BeautifulSoup) -> bool:
        select_tag = soup.find("select", {"id": "ctl00_ddlTable"})
        return select_tag.getText() == "\n"

    @staticmethod
    async def descriptive_report_is_empty(soup: BeautifulSoup) -> bool:
        sel = soup.find("select", {"id": "ctl00_ddlTable"})
        if sel is None:
            sel = soup.find("select", {"id": "ddlTable"})
        ttl = sel.find("option", {'selected': 'selected'}).getText()
        return ttl in ["income statement", "balance sheet", "auditor review", 'combined balance sheet']

    @staticmethod
    async def get_value(dataset, key) -> str:
        for item in dataset:
            if item.get('address') and item.get('address') == key:
                return item.get('value')

    @staticmethod
    async def get_row(dataset, key) -> dict:
        for item in dataset:
            if item.get('address') and item.get('address') == key:
                return item

    @classmethod
    async def get_paid(cls, soup):
        response = soup.find_all("script", type="text/javascript")[7].text.split("var datasource = ")[1][:-7]
        data = json.loads(response).get('sheets')[0].get('tables')[1].get('cells')
        val = await cls.get_value(data, 'B1')
        return val

    @classmethod
    async def json_check_3rd_year_column(cls, data: list, logger) -> list:
        percent_list = ["percentage_change", 'percentages_change']
        try:
            e1_value = await SearchReport.find_value_by_address(data, "E1")
            d1_value = await SearchReport.find_value_by_address(data, "D1")
            if e1_value in percent_list:
                col_code = (await cls.get_row(data, "D1")).get('columnCode')
                return ["D", col_code]
            if d1_value in percent_list:
                col_code = (await cls.get_row(data, "E1")).get('columnCode')
                return ["E", col_code]
        except Exception as e:
            logger.error(f"Error while checking 3rd year column: {e}")

    @staticmethod
    async def get_year_date(dataset, key):
        for item in dataset:
            if item.get('address') and item.get('address') == key:
                date = item.get('yearEndToDate')
                if date != "":
                    if date.split("/")[1] == "01" and date.split("/")[2] == "01":
                        date = f'{int(date.split("/")[0]) - 1}/12/29'
                return date

    @staticmethod
    async def get_period_date(dataset, key):
        for item in dataset:
            if item.get('address') and item.get('address') == key:
                date = item.get('periodEndToDate')
                if date.split("/")[1] == "01" and date.split("/")[2] == "01":
                    date = f'{int(date.split("/")[0]) - 1}/12/29'
                return date

    @staticmethod
    async def get_is_audited(year, data, logger):
        if year == 'year3':
            pure_data = data
            data = pure_data["sheets"][0]["tables"][0]["cells"]
            if len(data) == 0:
                data = pure_data["sheets"][0]["tables"][1]["cells"]
            year3_column, _ = await SearchReport.json_check_3rd_year_column(data, logger)
            val = await SearchReport.find_value_by_address(data, f'{year3_column}2')
            if val is None:
                return False
        elif year == "year1":
            val = await SearchReport.find_value_by_address(data["sheets"][0]["tables"][0]["cells"], 'B2')
        elif year == "year2":
            val = await SearchReport.find_value_by_address(data["sheets"][0]["tables"][0]["cells"], 'C2')
        if val is None or val == '0':
            return False
        return 'audited' in val

    @staticmethod
    async def get_year_end_to_date(soup: BeautifulSoup) -> str:
        year_span = soup.find("span", {"id": "ctl00_lblYearEndToDate"})
        year = year_span.bdo.getText()
        return year

    @staticmethod
    async def get_period_end_to_date(soup: BeautifulSoup) -> str:
        span = soup.find("span", {"id": "ctl00_lblPeriodEndToDate"})
        date = span.bdo.getText()
        return date

    @staticmethod
    async def get_period(soup: BeautifulSoup) -> Union[str, None]:
        period_span = soup.find("span", {"id": "ctl00_lblPeriod"})
        if period_span is None:
            return None
        return period_span.getText()

    @staticmethod
    async def get_address(config: dict, table: list) -> dict:
        for key in config:
            converted_key = key
            for row in table:
                if row.get('address')[0] == "A":
                    value = row['value']
                    if value == converted_key:
                        address = row['address'][1:]
                        config[key] = address
                        break
        return config
