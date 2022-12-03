import asyncio

from bs4 import BeautifulSoup
from httpx import Response
from loguru import logger

from helpers.mongodb_connector import mongo_client

from helpers.mystem import system
from helpers.stemmer import stemmer

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
        word[i: i + 2]
        for i in range(len(word) - 1)
    ]


def get_standard_symbol(symbol: dict) -> dict:
    symbol['text'] = stemmer.stem(symbol['text'])
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
        mongo_client.test.word_bigrams.find_one_and_update(
            {'word': word},
            {'$set': {
                'word': word,
                'bigrams': get_bigrams_of_word(word),
            }},
            upsert=True,
        )
        for word in words
    )

    upload_documents_task = mongo_client.test.docs.insert_many(documents)

    documents, *_ = await asyncio.gather(upload_documents_task, *inset_bigrams_tasks)

    logger.debug('upload to mongodb {} documents', len(documents.inserted_ids))
