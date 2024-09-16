import asyncio
from datetime import datetime, timedelta
from database.db import async_session_maker
from models.spimex_trading import SpimexTradingResults
from services.service import SpimexService


class SpimexParser:

    def __init__(self, year: int, month: int) -> None:
        self.sc = SpimexService()
        self.dates = self._get_dates(year, month)
        self.async_session = async_session_maker

    def _get_dates(self, year: int, month: int, day: int = 1) -> list[str]:

        dates = []
        start_date = datetime(year, month, day).date()
        while True:
            dates.append(start_date.isoformat())
            next_date = start_date + timedelta(days=1)
            if next_date.month != start_date.month:
                break

            start_date = next_date
        return dates

    async def _seve_to_db(self, obj: list[SpimexTradingResults]) -> None:
        async with self.async_session() as session:
            async with session.begin():
                session.add_all(obj)

    async def start(self) -> None:

        await self.sc.download(self.dates)


if __name__ == "__main__":
    parser = SpimexParser(2024, 9)
    asyncio.run(parser.start())
