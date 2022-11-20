import asyncio
from itertools import chain

from httpx import AsyncClient, Timeout
from loguru import logger

from documents import upload_documents
from scrapy import ChankedQueue, get_all_url_from_html, scrape
from config import Config


def get_new_urls(paths: set[str], visited: set[str]) -> set[str]:
    return {
        f'{Config.url}{path}'
        for path in paths
        if path.startswith('/wiki/') and path not in visited
    }


async def empty_coroutine() -> None:
    return


async def run():
    visited_paths = {f'{Config.url}{Config.start_path}'}
    urls_to_process = ChankedQueue()
    urls_to_process.append((f'{Config.url}{Config.start_path}',))

    upload_documents_task = empty_coroutine()

    session = AsyncClient(timeout=Timeout(100, connect=60))

    while len(visited_paths) < Config.count_documents:

        url_chunk = urls_to_process.popleft()
        scrape_tasks = [scrape(url, session=session, throttler=Config.throttler) for url in url_chunk]
        _, *results = await asyncio.gather(upload_documents_task, *scrape_tasks)

        visited_paths.update(url_chunk)
        paths_in_results = set(
            chain.from_iterable(
                get_all_url_from_html(result.text)
                for result in results
                if result
            )
        )
        urls_to_process.extend(get_new_urls(paths_in_results, visited_paths))

        upload_documents_task = upload_documents(results)

    await upload_documents_task
    await session.aclose()

    logger.debug("finished scraping")


if __name__ == "__main__":
    asyncio.run(run())
