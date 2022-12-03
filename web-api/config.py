from pydantic import BaseConfig

class Config(BaseConfig):

    minimum_distance_for_fuzzy_words: float = 0.8
