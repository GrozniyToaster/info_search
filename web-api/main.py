from fastapi import FastAPI, Request, Form
from fastapi.templating import Jinja2Templates

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


@app.post("/form")
async def form_post(request: Request, statement: str = Form(default='')):
    result = EXAMPLE_DOCS if statement else []
    enrich_query = await get_enrich_query(statement)
    logger.debug('Enrich query {}  mapping {}', enrich_query.query, enrich_query.fuzzy_mapping)
    return templates.TemplateResponse('form.html', context={'request': request, 'results': result})
