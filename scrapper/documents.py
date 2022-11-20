from httpx import Response
from bs4 import BeautifulSoup
from loguru import logger

from motor.motor_asyncio import AsyncIOMotorClient

client = AsyncIOMotorClient()


async def upload_documents(responses: list[Response | None]) -> None:
    if not responses:
        return

    documents = await client.test.docs.insert_many(
        [
            {
                'path': response.url.path,
                'raw_html': response.text,
                'text':  BeautifulSoup(response.text).get_text()
            }
            for response in responses if response
        ]
    )
    logger.debug('upload to mongodb {} documents', len(documents.inserted_ids))
