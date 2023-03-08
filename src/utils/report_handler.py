import asyncio
import aiohttp
import datetime
from typing import Union
from bs4 import BeautifulSoup
from aiolimiter import AsyncLimiter

from src.config import HEADERS, MIDDLEWARE_SECRET_KEY
from .make_logger import make_log


class ReportHandler:

    @classmethod
    async def get_total_reports(cls, company, letter_type=None, sort_rep=True) -> list:
        async with AsyncLimiter(1, 3):
            total_reports = list()
            for length in ['1', '3', '6', '9', '12']:
                reports = await cls.report_list_request(company.get("codal_name"), "1", letter_type,
                                                        company.get('last_check'), length)
                if reports is None:
                    return []
                total_reports.extend(await cls.add_period_length(reports.get("Letters"), length))
                if int(reports.get("Page")) > 1:
                    for i in range(2, int(reports.get("Page")) + 1):
                        reports = await cls.report_list_request(
                            company.get("codal_name"), i, letter_type, company.get('last_check'), length
                        )
                        total_reports.extend(await cls.add_period_length(reports.get("Letters"), length))
            if sort_rep:
                return await cls.sort_reports(total_reports)
            return total_reports

    async def get_summ_total_reports(self, company):
        total_reports = await self.get_total_reports(company, 'n-10')
        async with AsyncLimiter(1, 3):
            for length in ['1', '3', '6', '9', '12']:
                reports = await self.report_list_request(company.get("codal_name"), "1", 'n-31',
                                                         company.get('last_check'),
                                                         length)
                total_reports.extend(await self.add_period_length(reports.get("Letters"), length))
                if int(reports.get("Page")) != 1:
                    for i in range(2, int(reports.get("Page")) + 1):
                        reports = await self.report_list_request(company.get("codal_name"), i, 'n-31',
                                                                 company.get('last_check'), length)
                        total_reports.extend(await self.add_period_length(reports.get("Letters"), length))
            length = "1"
            reports = await self.report_summ_list_request(company.get("codal_name"), "1", company.get('last_check'),
                                                          length)
            total_reports.extend(await self.add_period_length(reports.get("Letters"), length))
            if int(reports.get("Page")) != 1:
                for i in range(2, int(reports.get("Page")) + 1):
                    reports = await self.report_summ_list_request(company.get("codal_name"), i,
                                                                  company.get('last_check'),
                                                                  length)
                    total_reports.extend(await self.add_period_length(reports.get("Letters"), length))
            return await self.sort_reports(total_reports)

    @classmethod
    async def report_list_request(cls, symbol, page_number, letter_type, from_date, length):
        try:
            await asyncio.sleep(1)
            if from_date is not None:
                from_date = datetime.datetime.strptime(from_date, "%Y-%m-%d")
                from_date += datetime.timedelta(days=1)
                url = f"https://search.stockcodal.com/api/search/v2/q?&Symbol={symbol}&Childs={False}&Mains=true" \
                      f"&Length={length}&PageNumber={page_number}&FromDate={from_date}"
            else:
                url = f"https://search.stockcodal.com/api/search/v2/q?&Symbol={symbol}&Childs={False}&Mains=true" \
                      f"&Length={length}&PageNumber={page_number}"
            if letter_type is not None:
                url += f"&LetterCode={letter_type}"
            async with aiohttp.ClientSession() as session:
                async with session.get(url, headers=HEADERS) as response:
                    return await response.json()
        except Exception as e:
            pass

    async def report_summ_list_request(self, symbol, page_number, from_date, length):
        try:
            await asyncio.sleep(1)
            if from_date is not None:
                from_date = datetime.datetime.strptime(from_date, "%Y-%m-%d")
                from_date += datetime.timedelta(days=1)
                url = f"https://search.stockcodal.com/api/search/v2/q?&Symbol={symbol}&Childs={False}&Mains=true" \
                      f"&Length={length}&PageNumber={page_number}&FromDate={from_date}"
            else:
                url = f"https://search.stockcodal.com/api/search/v2/q?&Symbol={symbol}&Childs={False}&Mains=true" \
                      f"&Length={length}&PageNumber={page_number}"
            async with aiohttp.ClientSession() as session:
                async with session.get(url, headers=HEADERS) as response:
                    return await response.json()
        except Exception as e:
            pass

    @staticmethod
    async def backend_request(url: str, logger):
        """
        Needs middle ware secret key in header to be authenticated in backend.
        """
        headers = {'token': MIDDLEWARE_SECRET_KEY}
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(url, headers=headers) as resp:
                    response = await resp.json()
                    return response
        except Exception as e:
            logger.error(f"Error while getting backend request")

    @staticmethod
    async def add_period_length(data: list, length: str):
        for item in data:
            item['length'] = length
        return data

    @staticmethod
    async def sort_reports(total_reports: list):
        for i in range(len(total_reports)):
            for j in range(len(total_reports) - 1):
                first_date = total_reports[j].get('converted_date', None)
                if first_date is None:
                    first_date = str(total_reports[j]['PublishDateTime'].split(' ')[0].replace('/', '-'))
                    total_reports[j]["converted_date"] = first_date
                second_date = total_reports[j + 1].get('converted_date', None)
                if second_date is None:
                    second_date = str(total_reports[j + 1]['PublishDateTime'].split(' ')[0].replace('/', '-'))
                    total_reports[j + 1]["converted_date"] = second_date
                if first_date > second_date:
                    tmp = total_reports[j + 1]
                    total_reports[j + 1] = total_reports[j]
                    total_reports[j] = tmp
        return total_reports

    @staticmethod
    async def get_report(url, sheet_id=None):
        if sheet_id is None:
            report_url = f"https://www.stockcodal.com{url}"
        else:
            report_url = f"https://www.stockcodal.com{url}&sheetId={sheet_id}"
        try:
            await asyncio.sleep(1)
            session_timeout = aiohttp.ClientTimeout(total=None, sock_read=None, connect=300, sock_connect=300)
            async with aiohttp.ClientSession(timeout=session_timeout) as session:
                async with session.get(report_url) as response:
                    text = await response.text()
                    return text
        except Exception as e:
            await make_log(f"get_report error: {e}", url=url)

    @staticmethod
    async def get_n10_codal_company(soup: BeautifulSoup, logger, report_url: str, comp_id) -> Union[dict, None, str]:
        try:
            symbol_and_name = soup.find("div", {"class": "symbol_and_name"})
            if symbol_and_name is None:
                return "empty"
            rows = symbol_and_name.find_all("div", {"class": "rows"})
            company_name = rows[0].find_all("div", {"class": "text_holder"})[0].find("div", {"class": "varios"}).find(
                "span", {
                    "id": "ctl00_txbCompanyName"}).getText()
            symbol = rows[1].find_all("div", {"class": "text_holder"})[0].find("div", {"class": "varios"}).find(
                "span", {"id": "ctl00_txbSymbol"}).getText()
            return {"company_name": company_name,
                    "codal_name": symbol,
                    "company": comp_id}
        except Exception as e:
            await make_log(f"Error while getting n 10 codal company for report: {report_url} error: {e}",
                           company=comp_id)
            logger.error(f"Error while getting n 10 codal company for report: {report_url} error: {e}")
