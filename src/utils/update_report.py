import types
from aiolimiter import AsyncLimiter
from typing import Union
from bs4 import BeautifulSoup

from .webhook import (
    get_data_from_middleware,
    hook_to_middleware
)
from .make_logger import make_log
from .report_handler import ReportHandler


class UpdateReport(ReportHandler):

    def __init__(self, get_data_kw: str, get_data_tp: str, report_ttl: str, report_async_limiter: AsyncLimiter,
                 transform, hook_kw, sheet_id: int, logger):
        self.get_data_kw = get_data_kw
        self.get_data_tp = get_data_tp
        self.report_ttl = report_ttl
        self.report_async_limiter = report_async_limiter
        self.transform = transform
        self.hook_kw = hook_kw
        self.sheet_id = sheet_id
        self.logger = logger

    async def run(self):
        try:
            api_result = await get_data_from_middleware(self.get_data_kw, self.get_data_tp, page_size=100)
            reports_count = api_result.get('count')
            codal_reports = {}
            while True:
                invalid_reports = api_result.get('results')
                self.logger.warning(f"{self.report_ttl} invalid reports : {reports_count}")
                for enum, invalid_report in enumerate(invalid_reports):
                    company = invalid_report.get('company_id')
                    report_url = await self.fix_report_url(invalid_report.get('source_url'))
                    codal_name = invalid_report.get('codal_name')
                    if codal_name not in codal_reports:
                        codal_reports[codal_name] = await self.get_total_reports({'codal_name': codal_name})
                        if len(codal_reports[codal_name]) == 0:
                            break

                    try:
                        self.logger.warning(
                            f"Updating {self.report_ttl} report {enum + 1} of reports {reports_count} of "
                            f"company {company}")

                        report = await self.find_report_from_codal(codal_reports, codal_name, report_url)
                        if report is None:
                            continue

                        async with self.report_async_limiter:
                            request = await self.get_report("".join(report_url.split("stockcodal.com")[1:]),
                                                            self.sheet_id)
                            if request is None:
                                continue
                            soup = BeautifulSoup(request, features="html.parser")
                            codal_company = await self.get_n10_codal_company(soup, self.logger, report_url, company)
                            if codal_company is None:
                                continue
                            if codal_company != "empty":
                                result = await self.run_transform(self.transform, company, soup, report, self.logger,
                                                                  codal_company)
                                first_report_error = True
                                if result is not None:
                                    if result != "empty report":
                                        result = await self.add_params_to_result(result, invalid_report)
                                        status = await hook_to_middleware(self.hook_kw, result)
                                        if status not in [200, 201]:
                                            break
                                        continue
                                    first_report_error = False
                            else:
                                first_report_error = False

                            report_url = report_url.replace("Decision", "InterimStatement")
                            request = await self.get_report("".join(report_url.split("codal.ir")[1:]), self.sheet_id)
                            if request is None:
                                continue
                            soup = BeautifulSoup(request, features="html.parser")
                            if codal_company == "empty":
                                codal_company = await self.get_n10_codal_company(soup, self.logger, report_url, company)
                            report = await self.find_report_from_codal(codal_reports, codal_name, report_url)
                            if report is None:
                                continue
                            result = await self.run_transform(self.transform, company, soup, report, self.logger,
                                                              codal_company)
                            if result is not None:
                                if result != "empty report":
                                    result = await self.add_params_to_result(result, invalid_report)
                                    status = await hook_to_middleware(self.hook_kw, result)
                                    if status not in [200, 201]:
                                        break
                                    continue
                                if not first_report_error:
                                    continue
                    except Exception as e:
                        await make_log(f"{self.report_ttl} error: {e} at report: {report_url}", url=report_url,
                                       company=company)
                        self.logger.error(f"{self.report_ttl} error: {e} at report: {report_url}")

                next_url = api_result.get('next')
                if next_url is None:
                    break
                api_result = await self.backend_request(next_url, self.logger)

        except Exception as e:
            await make_log(f"{self.report_ttl} with error: {e}")
            # self.logger.error(f"{self.report_ttl} with error: {e}")

    async def find_report_from_codal(self, codal_reports: dict, codal_name: str, report_url: str) -> Union[dict, None]:
        sheet_id = None
        if "sheetId" in report_url:
            sheet_id = "sheetId"
        elif "SheetId" in report_url:
            sheet_id = "SheetId"
        elif "sheetid" in report_url:
            sheet_id = "sheetid"
        report_url = report_url.split("LetterSerial=")[1].split(f"&{sheet_id}")[0]
        for codal_report in codal_reports[codal_name]:
            codal_url = (await self.fix_report_url(codal_report['Url'])).split("LetterSerial=")[1]
            if codal_url == report_url:
                return codal_report

    @staticmethod
    async def fix_report_url(url: str) -> str:
        url = url.split("&")[0]
        # url = re.sub(r"&rt=.", "", url)
        # url = re.sub(r"&let=.", "", url)
        # url = re.sub(r"&ct=.", "", url)
        # url = re.sub(r"ft=.", "", url)
        return url

    @staticmethod
    async def add_params_to_result(result: dict, invalid_report: dict) -> dict:
        result['is_update_request'] = True
        result['source_url'] = invalid_report.get('source_url')
        for i in result:
            if i in ["year1", "year2", "year3"]:
                result[i]['data']['is_admin_accepted'] = True
            elif i == "total":
                result[i]['is_admin_accepted'] = True

        return result

    @staticmethod
    async def run_transform(transform, *args):
        if isinstance(transform, types.FunctionType):
            return await transform(*args)
        else:
            return await transform(*args).result
