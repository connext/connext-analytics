import asyncio

from src.integrations.metis_block_explorer import \
    main_fetch as metis_main_fetch
from src.integrations.mode_block_explorer import main_fetch as mode_main_fetch

# pipeline Flow:


def main():
    asyncio.run(mode_main_fetch(parallel_fetch=10))
    asyncio.run(metis_main_fetch(parallel_fetch=10))
