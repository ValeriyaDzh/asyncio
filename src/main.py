import asyncio, time

from database.db import create_db
from services.service import SpimexParser


if __name__ == "__main__":
    parser = SpimexParser(2024, 9)
    asyncio.run(create_db())
    start_time = time.perf_counter()
    asyncio.run(parser.start())
    elapsed = time.perf_counter() - start_time
    print(f"done for: {round(elapsed, 2)} sec")
