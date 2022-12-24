
from aiolimiter import AsyncLimiter
from pydantic import BaseConfig, HttpUrl


class Config(BaseConfig):
    url: HttpUrl = 'http://neolurk.org'
    start_path: str = '/wiki/Заглавная_страница'

    throttler: AsyncLimiter = AsyncLimiter(max_rate=10, time_period=1)  # 10 tasks/second

    count_documents: int = 65000
