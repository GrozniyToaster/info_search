import asyncio
from collections import deque
from itertools import chain, islice
from time import time
from typing import Iterable, TypeVar

from aiolimiter import AsyncLimiter
from bs4 import BeautifulSoup
from httpx import AsyncClient, Response, Timeout, codes
from loguru import logger

T = TypeVar('T')

class ChankedQueue(deque[tuple[T, ...]]):
    def __init__(self, chunk_size: int = 50, *arg, **kwargs) -> None:
        self.chunk_size = chunk_size
        super(ChankedQueue, self).__init__(*arg, **kwargs)

    def extend(self, iterable: Iterable[T]) -> None:  # type: ignore
        real_dequeue = super(ChankedQueue, self)
        start = 0
        if real_dequeue.__bool__():
            last_chunk = real_dequeue.pop()
            stop = self.chunk_size - len(last_chunk)
        else:
            last_chunk = tuple()
            stop = self.chunk_size

        while True:
            next_chunk = tuple(chain(last_chunk, islice(iterable, start, stop)))
            if not next_chunk:
                break

            real_dequeue.append(next_chunk)
            last_chunk = tuple()
            start, stop = stop, stop + self.chunk_size


def get_all_url_from_html(source: str) -> set[str]:
    soup = BeautifulSoup(source, features="html.parser")
    return {a['href'] for a in soup.find_all('a', href=True)}

async def make_request(url: str, session: AsyncClient, throttler: AsyncLimiter) -> Response:
    async with throttler:
        logger.debug('scrapy {}', url)
        return await session.get(url)


async def srape(url: str, session: AsyncClient, throttler: AsyncLimiter) -> Response | None:
    response = await make_request(url, session, throttler)

    if response.status_code == codes.OK:
        return response

    if response.status_code == codes.TOO_MANY_REQUESTS or response.status_code == codes.SERVICE_UNAVAILABLE:
        logger.warning('retry later by {} on url {}', response.status_code, url)
        await asyncio.sleep(1.5)
        response = await make_request(url, session, throttler)

    if response.status_code != codes.OK:
        logger.error('cannot scrape {} code {}', url, response.status_code)
        return None

    return response


URL = 'http://neolurk.org'
MAIN_PAGE = '/wiki/%D0%97%D0%B0%D0%B3%D0%BB%D0%B0%D0%B2%D0%BD%D0%B0%D1%8F_%D1%81%D1%82%D1%80%D0%B0%D0%BD%D0%B8%D1%86%D0%B'



def filter_urls(urls: set[str], visited: set[str]) -> set[str]:
    return {
        f'{URL}{url}'
        for url in urls
        if url.startswith('/wiki/') and not url.startswith('/w/') and url not in visited
    }

async def run():
    _start = time()
    visited = {f'{URL}/{MAIN_PAGE}'}
    throttler = AsyncLimiter(max_rate=10, time_period=1)   # 10 tasks/second
    url_to_process = ChankedQueue()
    url_to_process.append((f'{URL}/{MAIN_PAGE}',))
    i = 1
    async with AsyncClient(timeout=Timeout(10, connect=60)) as session:
        while len(visited) < 50:
            url_chunk = url_to_process.popleft()
            tasks = [srape(url, session=session, throttler=throttler) for url in url_chunk]
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

            for res in results:
                if not res.text:
                    continue
                logger.info(f'{res.http_version}')
                with open(f'./out/{i}', 'w') as f:
                    f.write(res.text)
                i += 1



    print(f"finished scraping in: {time() - _start:.1f} seconds")


if __name__ == "__main__":
    asyncio.run(run())
