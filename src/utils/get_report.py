import types
from typing import Union
from bs4 import BeautifulSoup

from .report_handler import ReportHandler
from .webhook import (
    get_data_from_middleware,
    hook_to_middleware
)
from .make_logger import make_log
from src.config import CEMENT_COMPANIES


class GetReport(ReportHandler):
    def __init__(self, report_async_limiter, get_data_kw, get_data_tp, report_ttl: str, transform, hook_kw: str,
                 sheet_id: Union[int, None], logger, letter_type: str = "n-10", skip_comp_symbs: list = None,
                 is_cement: Union[bool, None] = None,
                 sort_rep=True):
        self.report_async_limiter = report_async_limiter
        self.get_data_kw = get_data_kw
        self.get_data_tp = get_data_tp
        self.report_ttl = report_ttl
        self.transform = transform
        self.hook_kw = hook_kw
        self.sheet_id = sheet_id
        self.logger = logger
        self.letter_type = letter_type
        if skip_comp_symbs is None:
            self.skip_comp_symbs = []
        else:
            self.skip_comp_symbs = skip_comp_symbs
        self.is_cement = is_cement
        self.sort_rep = sort_rep

    async def run(self):
        try:
            companies = await get_data_from_middleware(self.get_data_kw, self.get_data_tp)
            if companies is None:
                return
            self.logger.warning(f"{self.report_ttl} length companies : {len(companies)}")
            for ecomp, company in enumerate(companies):
                if company.get('symbol') in self.skip_comp_symbs:
                    continue
                # comp_detail = await get_data_from_middleware("company_detail", data="tse", pk=company.get('id'))
                # if comp_detail is None:
                #     break

                company_in_cement = company.get('symbol') in CEMENT_COMPANIES
                if self.is_cement is not None:
                    if (company_in_cement and not self.is_cement) or (not company_in_cement and self.is_cement):
                        continue

                report_url = None
                try:
                    total_reports = await self.get_total_reports(company, self.letter_type, self.sort_rep)
                    self.logger.warning(
                        f"{self.report_ttl} length total reports : {len(total_reports)} for company: "
                        f"{company.get('symbol')}")
                    for enum, report in enumerate(total_reports):
                        # if enum < 42:
                        #     continue
                        self.logger.warning(
                            f"Getting {self.report_ttl} report {enum + 1} of reports {len(total_reports)} of company "
                            f"{company['symbol']}")
                        async with self.report_async_limiter:
                            if self.sheet_id is not None:
                                report_url = f"https://www.stockcodal.com{report.get('Url')}&sheetId={self.sheet_id}"
                            else:
                                report_url = f"https://www.stockcodal.com{report.get('Url')}"
                            request = await self.get_report(report.get("Url"), self.sheet_id)
                            if request is None:
                                break
                            soup = BeautifulSoup(request, features="html.parser")
                            codal_company = await self.get_n10_codal_company(soup, self.logger, report_url,
                                                                             company.get("id"))
                            if codal_company is None:
                                break
                            result = await self.run_transform(self.transform, company.get("id"), soup, report,
                                                              self.logger, codal_company)
                            result = await self.add_params_to_result(result)
                            first_report_error = True
                            if result is not None:
                                if result != "empty report":
                                    status = await hook_to_middleware(self.hook_kw, result)
                                    if status not in [200, 201]:
                                        break
                                    continue
                                first_report_error = False
                            report['Url'] = str(report['Url']).replace('Decision', 'InterimStatement')
                            report_url = report_url.replace("Decision", "InterimStatement")
                            request = await self.get_report(report.get("Url"), self.sheet_id)
                            if request is None:
                                break
                            soup = BeautifulSoup(request, features="html.parser")
                            result = await self.run_transform(self.transform, company.get("id"), soup, report,
                                                              self.logger, codal_company)
                            result = await self.add_params_to_result(result)
                            if result is not None:
                                if result != "empty report":
                                    status = await hook_to_middleware(self.hook_kw, result)
                                    if status not in [200, 201]:
                                        break
                                    continue
                                if not first_report_error:
                                    continue
                            break
                except Exception as e:
                    await make_log(f"{self.report_ttl} error: {e} at report: {report_url}", url=report_url,
                                   company=int(company.get('id')))
                    self.logger.error(f"{self.report_ttl} error: {e} at report: {report_url}")
        except Exception as e:
            await make_log(f"exit {self.report_ttl} with error: {e}")
            self.logger.error(f"exit {self.report_ttl} with error: {e}")

    @staticmethod
    async def run_transform(transform, *args):
        if isinstance(transform, types.FunctionType):
            return await transform(*args)
        else:
            return await transform(*args).result

    @staticmethod
    async def add_params_to_result(result: dict) -> dict:
        for i in result:
            if i in ["year1", "year2", "year3"]:
                result[i]['data']['is_admin_accepted'] = True
            elif i == "total":
                result[i]['is_admin_accepted'] = True

        return result
