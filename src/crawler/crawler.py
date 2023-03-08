import asyncio

from .crawler_handler import CrawlerHandler


class Crawler:
    def __init__(self):
        self.handler = CrawlerHandler()

    async def get_monthly_activities(self):
        async with self.handler as handler:
            return await handler.get_monthly_activities()

    async def get_descriptive_one(self):
        async with self.handler as handler:
            return await handler.get_descriptive_one()

    async def get_descriptive_two(self):
        async with self.handler as handler:
            return await handler.get_descriptive_two()

    async def main(self):
        await asyncio.gather(
            self.get_monthly_activities(),
            self.get_descriptive_one(),
            self.get_descriptive_two()
        )

    def run(self):
        asyncio.run(self.main())
