import asyncio
import asyncstdlib
from loguru import logger
from helpers.mongodb_connector import mongo_client


async def build_bigram_words_dictionary():
    total_documents = await mongo_client.test.word_bigrams.count_documents({})
    percentage = 0
    async for i, word_with_bigrams in asyncstdlib.enumerate(mongo_client.test.word_bigrams.find()):
        if i / total_documents * 100 > percentage:
            logger.debug('Process {:.0%} documents', i / total_documents)
            percentage += 10

        word = word_with_bigrams['word']
        await asyncio.gather(
            *(
                mongo_client.test.bigram_words.find_one_and_update(
                    {'bigram': bigram},
                    {'$push': {'words': word}},
                    upsert=True,

                )
                for bigram in word_with_bigrams['bigrams']
            )
        )
