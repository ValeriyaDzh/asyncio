import asyncio

from database.db import create_db
from services.service import SpimexParser


if __name__ == "__main__":
    parser = SpimexParser(2024, 9)
    asyncio.run(create_db())
    asyncio.run(parser.start())
