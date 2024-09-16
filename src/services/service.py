from httpx import AsyncClient


class SpimexService:

    async def download(self, dates: list[str]) -> None:
        try:
            async with AsyncClient() as client:
                for date in dates:
                    url = f"https://spimex.com/upload/reports/oil_xls/oil_xls_{date.replace('-', '')}162000.xls"
                    response = await client.get(url=url, timeout=5)
                    if response.status_code == 200:
                        with open(f"{date}_spimex_data.xls", "wb") as file:
                            file.write(response.content)
        except Exception as e:
            print(f"Error while download: {e}!")
