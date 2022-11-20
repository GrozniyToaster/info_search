import asyncio
from itertools import chain

from aiolimiter import AsyncLimiter
from httpx import AsyncClient, Timeout
from loguru import logger

from documents import upload_documents
from scrapy import ChankedQueue, get_all_url_from_html, scrape

URL = 'http://neolurk.org'
MAIN_PAGE = '/wiki/Заглавная_страница'


def filter_urls(urls: set[str], visited: set[str]) -> set[str]:
    return {
        f'{URL}{url}'
        for url in urls
        if url.startswith('/wiki/') and not url.startswith('/w/') and url not in visited
    }


async def run():
    visited = {f'{URL}{MAIN_PAGE}'}
    throttler = AsyncLimiter(max_rate=10, time_period=1)  # 10 tasks/second
    url_to_process = ChankedQueue()
    url_to_process.append((f'{URL}/{MAIN_PAGE}',))
    i = 1
    async with AsyncClient(timeout=Timeout(10, connect=60)) as session:
        while len(visited) < 50:
            url_chunk = url_to_process.popleft()
            tasks = [scrape(url, session=session, throttler=throttler) for url in url_chunk]
            results = await asyncio.gather(*tasks)
            visited.update(url_chunk)
            url_in_results = set(
                chain.from_iterable(
                    get_all_url_from_html(result.text)
                    for result in results
                    if result
                )
            )
            url_to_process.extend(filter_urls(url_in_results, visited))

            await upload_documents(results)

    logger.debug("finished scraping")


if __name__ == "__main__":
    asyncio.run(run())
