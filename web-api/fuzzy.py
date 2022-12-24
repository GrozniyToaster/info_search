from helpers.mongodb_connector import mongo_client
from helpers.bigrams import get_bigrams_of_word
from config import Config



class Dimension:

    def __init__(self, word: str) -> None:
        self.word = word
        self.words_bigrams = set(get_bigrams_of_word(word))

    def distance_to(self, candidate: str) -> float:
        if abs(len(self.word) - len(candidate)) > 3:
            return 0
        words_bigrams = self.words_bigrams
        candidates_bigrams = set(get_bigrams_of_word(candidate))
        distance = abs(len(words_bigrams) - len(words_bigrams & candidates_bigrams))
        return abs(1 - distance / len(words_bigrams | candidates_bigrams))


async def get_fuzzy_words(word: str) -> list[tuple[str, float]]:
    bigrams = get_bigrams_of_word(word)
    word_dimension = Dimension(word)
    fuzzy_words_rating: set[tuple[str, float]] = set()
    async for bigram_with_words in mongo_client.test.bigram_words.find({'bigram': {'$in': bigrams}}):
        fuzzy_words_rating.update(
            (candidate, word_dimension.distance_to(candidate))
            for candidate in bigram_with_words['words']
            if word_dimension.distance_to(candidate) >= Config.minimum_distance_for_fuzzy_words_for_combine_request
        )

    return [
        (word, rank)
        for word, rank in reversed(sorted(fuzzy_words_rating, key=lambda row: row[1]))
    ]

async def is_dictionary_word(word: str) -> bool:
    return bool(
        await mongo_client.test.word_bigrams.count_documents({'word': word})
    )
