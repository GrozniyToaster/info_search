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

    # 'APRO'	    # местоимение-прилагательное
    # 'CONJ'	    # союз
    # 'INTJ'	    # междометие
    # 'PART'	    # частица
    # 'PR'	    # предлог
    # 'SPRO'	    # местоимение-существительное
}


def get_clear_lemmas(text: str) -> list[dict]:
    analyzed_symbols: list[dict] = system.analyze(text)
    return [
        symbol
        for symbol in analyzed_symbols
        if is_significant_token(symbol)
    ]




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

    documents = await client.test.docs.insert_many(
        [
            {
                'path': response.url.path,
                'raw_html': response.text,
                'lemmas': get_clear_lemmas(BeautifulSoup(response.text).get_text())
            }
            for response in responses if response
        ]
    )
    logger.debug('upload to mongodb {} documents', len(documents.inserted_ids))
