import os
from datetime import datetime, timedelta

import aiofiles.os
import aiofiles.threadpool as aiof
import pandas as pd
from httpx import AsyncClient
from database.db import async_session_maker
from models.spimex_trading import SpimexTradingResults


class SpimexParser:

    def __init__(self, year: int, month: int) -> None:
        self.dates = self._get_dates(year, month)
        self.async_session = async_session_maker

    def _get_dates(self, year: int, month: int, day: int = 1) -> list[datetime]:
        dates = []
        start_date = datetime(year, month, day).date()
        while True:
            dates.append(start_date)
            next_date = start_date + timedelta(days=1)
            if next_date.month != start_date.month:
                break

            start_date = next_date
        return dates

    async def _download(self, dates: list[datetime]) -> None:
        try:
            async with AsyncClient() as client:
                for date in dates:
                    url = f"https://spimex.com/upload/reports/oil_xls/oil_xls_{date.strftime('%Y%m%d')}162000.xls"
                    response = await client.get(url=url, timeout=5)
                    if response.status_code == 200:
                        async with aiof.open(f"{date}_spimex_data.xls", "wb") as file:
                            await file.write(response.content)
        except Exception as e:
            print(f"Error while download: {e}!")

    def _get_necessary_data(self, file: str) -> pd.DataFrame:
        columns_names = [
            "exchange_product_id",
            "exchange_product_name",
            "delivery_basis_name",
            "volume",
            "total",
            "count",
        ]

        columns_types = {
            "exchange_product_id": str,
            "exchange_product_name": str,
            "delivery_basis_name": str,
            "volume": int,
            "total": int,
            "count": int,
        }
        df = pd.read_excel(file, sheet_name=0, header=6)
        df[df.columns[-1]] = pd.to_numeric(df[df.columns[-1]], errors="coerce")
        df = df[df[df.columns[-1]] > 0]
        df = df.iloc[:-2, [1, 2, 3, 4, 5, -1]]
        df.columns = columns_names
        df = df.astype(columns_types)

        return df

    async def _save_to_db(self, obj: list[SpimexTradingResults]) -> None:
        async with self.async_session() as session:
            async with session.begin():
                session.add_all(obj)

    async def start(self) -> None:
        await self._download(self.dates)
        for date in self.dates:
            if os.path.exists(f"{date}_spimex_data.xls"):
                prepared_obj = []
                df_data = self._get_necessary_data(f"{date}_spimex_data.xls")
                for _, row in df_data.iterrows():
                    obj = SpimexTradingResults(
                        exchange_product_id=row["exchange_product_id"],
                        exchange_product_name=row["exchange_product_name"],
                        oil_id=row["exchange_product_id"][:4],
                        delivery_basis_id=row["exchange_product_id"][4:7],
                        delivery_basis_name=row["delivery_basis_name"],
                        delivery_type_id=row["exchange_product_id"][-1],
                        volume=row["volume"],
                        total=row["total"],
                        count=row["count"],
                        date=date,
                    )
                    prepared_obj.append(obj)
                await self._save_to_db(prepared_obj)
                await aiofiles.os.remove(f"{date}_spimex_data.xls")
