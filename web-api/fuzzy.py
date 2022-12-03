from helpers.mongodb_connector import mongo_client
from helpers.bigrams import get_bigrams_of_word
from config import Config

from loguru import logger


class Dimension:

    def __init__(self, word: str) -> None:
        self.words_bigrams = set(get_bigrams_of_word(word))

    def distance_to(self, candidate: str) -> float:
        words_bigrams = self.words_bigrams
        candidates_bigrams = set(get_bigrams_of_word(candidate))

        return len(words_bigrams & candidates_bigrams) / len(words_bigrams | candidates_bigrams)


async def get_fuzzy_word(word: str) -> str:
    bigrams = get_bigrams_of_word(word)
    word_dimension = Dimension(word)
    async for bigram_with_words in mongo_client.test.bigram_words.find({'bigram': {'$in': bigrams}}):
        logger.debug('Start searching fuzzy string in {}', bigram_with_words)
        for candidate in bigram_with_words['words']:
            if word_dimension.distance_to(candidate) >= Config.minimum_distance_for_fuzzy_words:
                return candidate


async def is_dictionary_word(word: str) -> bool:
    return bool(
        await mongo_client.test.word_bigrams.count_documents({'word': word})
    )
