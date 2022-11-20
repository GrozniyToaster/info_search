import asyncio
from collections import deque
from itertools import chain, islice
from typing import Iterable, TypeVar

from aiolimiter import AsyncLimiter
from bs4 import BeautifulSoup
from httpx import AsyncClient, Response, codes
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


async def scrape(url: str, session: AsyncClient, throttler: AsyncLimiter) -> Response | None:
    response = await make_request(url, session, throttler)

    if response.status_code == codes.OK:
        return response

    if response.status_code == codes.TOO_MANY_REQUESTS or response.status_code == codes.SERVICE_UNAVAILABLE:
        logger.warning('retry later by {} on url {}', response.status_code, url)
        await asyncio.sleep(1.5)
        response = await make_request(url, session, throttler)

    if codes.is_redirect(response.status_code):
        logger.warning('redirect on {} {}', url, response.status_code)
        response = await make_request(response.headers['Location'], session, throttler)

    if response.status_code != codes.OK:
        logger.error('cannot scrape {} code {}', url, response.status_code)
        return None



    return response
