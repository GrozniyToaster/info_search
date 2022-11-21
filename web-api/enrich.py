from pymystem3 import Mystem

system = Mystem()


def get_lexems(text: str) -> list[str]:
    return [
        lexem
        for lexem in system.lemmatize(text)
        if lexem.strip()
    ]
