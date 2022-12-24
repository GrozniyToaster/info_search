import asyncio
import random

import itertools
import json

import httpx
from fastapi import FastAPI, Request, Form
from fastapi.templating import Jinja2Templates
from bson import ObjectId

from helpers.mongodb_connector import mongo_client

from loguru import logger

from copy import deepcopy
from enrich import get_enrich_query, EnrichQuery
from config import Config
app = FastAPI()
templates = Jinja2Templates(directory="templates/")

EXAMPLE_DOCS = ['http://neolurk.org/wiki/Заглавная_страница', 'http://neolurk.org/wiki/Заглавная_страница']


@app.get('/')
def read_form():
    return 'hello world'


@app.get("/form")
def form_post(request: Request):
    result = ""
    return templates.TemplateResponse('form.html', context={'request': request, 'result': result})


async def get_search_results(queries: list[list[str]]) -> list[str]:
    ids = await asyncio.gather(*(get_search_results_ids(query) for query in queries))
    ids = [ObjectId(id) for id in itertools.chain.from_iterable(ids)]

    results = mongo_client.test.docs.aggregate(
        [
            {'$match': {'_id': {'$in': list(set(ids))}}},
            {'$project': {'path': 1}}
        ]
    )
    res_url = []
    async for result in results:
        res_url.append(f'http://neolurk.org{result["path"]}')
    random.shuffle(res_url)
    return res_url


async def get_search_results_ids(query: list[str]) -> list[str]:
    async with httpx.AsyncClient() as client:
        response = await client.post(
            'http://localhost:8080/search',
            headers={'Content-Type': 'application/x-www-form-urlencoded'},
            content=f'{{"words": { [word for word in sorted(query)] }}}'.replace("'", '"')
        )

        if response.status_code != httpx.codes.OK:
            logger.error('something went wrong', response)
            raise Exception('asd')

        return [doc['$oid'] for doc in response.json()['doc_ids']]


def get_transformed_queries(enrich_query: EnrichQuery) -> list[list[str]]:
    queries: list[list[str]] = [[]]

    for word in enrich_query.query:
        if word not in enrich_query.fuzzy_mapping:
            for query in queries:
                query.append(word)

        elif enrich_query.fuzzy_mapping[word][0][1] > Config.minimum_distance_for_fuzzy_words_for_replace:
            for query in queries:
                query.append(enrich_query.fuzzy_mapping[word][0][0])

        elif enrich_query.fuzzy_mapping[word][0][1] > Config.minimum_distance_for_fuzzy_words_for_combine_request:
            transformed = deepcopy(queries)
            for query in transformed:
                query.append(enrich_query.fuzzy_mapping[word][0][0])

            queries.extend(transformed)

    return queries



@app.post("/form")
async def form_post(request: Request, statement: str = Form(default='')):
    result = EXAMPLE_DOCS if statement else []
    enrich_query = await get_enrich_query(statement)
    queries = get_transformed_queries(enrich_query)
    search_result = await get_search_results(queries)
    logger.debug('Enrich query {}  mapping {}', enrich_query.query, enrich_query.fuzzy_mapping)
    return templates.TemplateResponse('form.html', context={'request': request, 'results': search_result})
