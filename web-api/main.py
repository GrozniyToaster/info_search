from fastapi import FastAPI, Request, Form
from fastapi.templating import Jinja2Templates

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
def form_post(request: Request, statement: str = Form(default='')):
    result = EXAMPLE_DOCS if statement else []
    return templates.TemplateResponse('form.html', context={'request': request, 'results': result })