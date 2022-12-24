import asyncio

from helpers.mystem import system
from helpers.stemmer import stemmer

from typing import NamedTuple

from fuzzy import is_dictionary_word, get_fuzzy_words


def get_lexemes(text: str) -> list[str]:
    return [
        stemmer.stem(lexem)
        for lexem in system.lemmatize(text)
        if lexem.strip()
    ]


class EnrichQuery(NamedTuple):
    query: list[str]
    fuzzy_mapping: dict[str, list[tuple[str, float]]]


async def process_lexeme(lexeme: str) -> tuple[str, str]:
    if await is_dictionary_word(lexeme):
        return lexeme, lexeme
    else:
        return lexeme, await get_fuzzy_words(lexeme) or lexeme


async def get_enrich_query(text: str) -> EnrichQuery:
    lexemes = get_lexemes(text)
    query: list[str] = []
    fuzzy_mapping: dict[str, str] = {}
    process_lexeme_tasks = (process_lexeme(lexeme) for lexeme in lexemes)

    for lexeme, processed_lexeme in await asyncio.gather(*process_lexeme_tasks):
        query.append(lexeme)
        if lexeme != processed_lexeme:
            fuzzy_mapping[lexeme] = processed_lexeme

    return EnrichQuery(query, fuzzy_mapping)

