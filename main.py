import asyncio
from aiolimiter import AsyncLimiter
from time import time
from httpx import AsyncClient
from itertools import chain, islice
from bs4 import BeautifulSoup
from collections import deque
from typing import Iterable, TypeVar

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


def get_allhref_from_html(source: str) -> set[str]:
    soup = BeautifulSoup(source)
    return {a['href'] for a in soup.find_all('a', href=True)}

async def scrape(url, session, throttler):
    async with throttler:
        return await session.get(url)

URL = 'http://neolurk.org'
MAIN_PAGE = '/wiki/%D0%97%D0%B0%D0%B3%D0%BB%D0%B0%D0%B2%D0%BD%D0%B0%D1%8F_%D1%81%D1%82%D1%80%D0%B0%D0%BD%D0%B8%D1%86%D0%B'



def filter_urls(urls: set[str], visited: set[str]) -> set[str]:
    return {
        f'{URL}{url}'
        for url in urls
        if url.startswith('/wiki/') and  and url not in visited
    }

async def run():
    _start = time()
    visited = {f'{URL}/{MAIN_PAGE}'}
    throttler = AsyncLimiter(max_rate=10, time_period=1)   # 10 tasks/second
    url_to_process = {f'{URL}/{MAIN_PAGE}'}
    i = 1
    async with AsyncClient() as session:
        while len(visited) < 50:
            tasks = [scrape(url, session=session, throttler=throttler) for url in islice(url_to_process, 50 - len(visited))]
            results = await asyncio.gather(*tasks)
            visited |= url_to_process
            url_in_results = set(chain.from_iterable(get_allhref_from_html(result.text) for result in results))
            url_to_process = filter_urls(url_in_results, visited)
            for res in results:
                if not res.text:
                    continue

                with open(f'./out/{i}', 'w') as f:
                    f.write(res.text)
                i += 1



    print(f"finished scraping in: {time() - _start:.1f} seconds")


if __name__ == "__main__":
    asyncio.run(run())
