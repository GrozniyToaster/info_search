import json

import httpx
from fastapi import FastAPI, Request, Form
from fastapi.templating import Jinja2Templates
from bson import ObjectId

from helpers.mongodb_connector import mongo_client

from loguru import logger

from enrich import get_enrich_query

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


async def get_search_results(query: list[str]) -> list[str]:
    ids = await get_search_results_ids(query)
    ids = [ObjectId(id) for id in ids]

    results = mongo_client.test.docs.aggregate(
        [
            {'$match': {'_id': {'$in': ids}}},
            {'$project': {'path': 1}}
        ]
    )
    res_url = []
    async for result in results:
        res_url.append(f'http://neolurk.org{result["path"]}')

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


@app.post("/form")
async def form_post(request: Request, statement: str = Form(default='')):
    result = EXAMPLE_DOCS if statement else []
    enrich_query = await get_enrich_query(statement)
    query = [
        enrich_query.fuzzy_mapping[lemma][0]
        if lemma in enrich_query.fuzzy_mapping
        else lemma
        for lemma in enrich_query.query
    ]
    search_result = await get_search_results(query)
    logger.debug('Enrich query {}  mapping {}', enrich_query.query, enrich_query.fuzzy_mapping)
    return templates.TemplateResponse('form.html', context={'request': request, 'results': search_result})
