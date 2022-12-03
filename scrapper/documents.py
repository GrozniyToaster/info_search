import asyncio

from itertools import islice
from bs4 import BeautifulSoup
from httpx import Response
from loguru import logger
from motor.motor_asyncio import AsyncIOMotorClient
from pymystem3 import Mystem

system = Mystem()
part_of_speech = {
    'A',  # прилагательное
    'ADV',  # наречие
    'ADVPRO',  # местоименное наречие
    'ANUM',  # числительное-прилагательное
    'COM',  # часть композита - сложного слова
    'NUM',  # числительное
    'S',  # существительное
    'V',  # глагол
    'SPRO',  # местоимение-существительное

    # 'APRO'	    # местоимение-прилагательное
    # 'CONJ'	    # союз
    # 'INTJ'	    # междометие
    # 'PART'	    # частица
    # 'PR'	    # предлог
}


def get_bigrams_of_word(word: str) -> list[str]:
    return [
        first_letter + second_letter
        for first_letter, second_letter in zip(word, islice(word, 1, None))
    ]


def get_standard_symbol(symbol: dict) -> dict:
    symbol['text'] = symbol['text'].lower()
    return symbol


def get_clear_lemmas(text: str) -> list[dict]:
    analyzed_symbols: list[dict] = system.analyze(text)
    return list(
        {
            get_standard_symbol(symbol)
            for symbol in analyzed_symbols
            if is_significant_token(symbol)
        }
    )


def is_significant_grammar(grammar: str) -> bool:
    name = grammar.split('=')[0].split(',')[0]
    return name in part_of_speech


def is_significant_token(token: dict) -> bool:
    match token:
        case {'analysis': [{'gr': str(grammar)}]} if is_significant_grammar(grammar):
            return True
        case _:
            return False


client = AsyncIOMotorClient()


async def upload_documents(responses: list[Response | None]) -> None:
    if not responses:
        return
    words: set[str] = set()
    documents: list[dict] = []
    for response in filter(lambda x: x is not None, responses):
        lemmas = get_clear_lemmas(BeautifulSoup(response.text).get_text())
        documents.append(
            {
                'path': response.url.path,
                'raw_html': response.text,
                'lemmas': lemmas
            }
        )
        words.update(
            lemma['text'] for lemma in lemmas
        )

    inset_bigrams_tasks = (
        client.test.word_bigrams.find_one_and_update(
            {'word': word},
            {'$set': {
                'word': word,
                'bigrams': get_bigrams_of_word(word),
            }},
            upsert=True,
        )
        for word in words
    )

    upload_documents_task = client.test.docs.insert_many(
        [
            {
                'path': response.url.path,
                'raw_html': response.text,
                'lemmas': get_clear_lemmas(BeautifulSoup(response.text).get_text())
            }
            for response in responses if response
        ]
    )

    documents, *_ = await asyncio.gather(upload_documents_task, *inset_bigrams_tasks)

    logger.debug('upload to mongodb {} documents', len(documents.inserted_ids))
