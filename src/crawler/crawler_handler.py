from aiolimiter import AsyncLimiter

from src.transform import (
    TransformMonthlyActivity,
    TransformDescriptiveOne,
    TransformDescriptiveTwo,
)
from src.utils import (
    get_logger,
    GetReport,
)


class CrawlerHandler:

    async def __aenter__(self):
        self._tse_async_limiter = AsyncLimiter(1, 3)
        self._monthly_activity_async_limiter = AsyncLimiter(1, 10)
        self._get_total_reports_async_limiter = AsyncLimiter(1, 2)
        self.logger = await get_logger()
        """
        self.logger.warning("Warning message"): Yellow title
        self.logger.error("Error message"): Red title
        """
        return self

    async def __aexit__(self, *args, **kwargs):
        pass

    async def get_monthly_activities(self):
        """
        Which companies have this kind of report:
        Finance companies: No
        Cement companies: Yes
        """
        while True:
            get_report = GetReport(self._monthly_activity_async_limiter, "company_list", "monthly_activity",
                                   "Monthly activities", TransformMonthlyActivity, "monthly_activity", None,
                                   self.logger, letter_type="n-30", skip_comp_symbs=[], is_cement=True,
                                   sort_rep=True)
            await get_report.run()

    async def get_descriptive_one(self):
        """
        Which companies have this kind of report:
        Finance companies: No
        Cement companies: Yes
        """
        while True:
            get_report = GetReport(self._monthly_activity_async_limiter, "company_list", "descriptive_one",
                                   "Descriptive One", TransformDescriptiveOne, "get_create_descriptive_one", 20,
                                   self.logger, skip_comp_symbs=[], is_cement=True, sort_rep=True)
            await get_report.run()

    async def get_descriptive_two(self):
        """
        Which companies have this kind of report:
        Finance companies: No
        Cement companies: Yes
        """
        while True:
            get_report = GetReport(self._monthly_activity_async_limiter, "company_list", "descriptive_two",
                                   "Descriptive Two", TransformDescriptiveTwo, "get_create_descriptive_two", 21,
                                   self.logger, skip_comp_symbs=[], is_cement=True, sort_rep=True)
            await get_report.run()
