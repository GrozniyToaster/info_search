from pydantic import BaseConfig

class Config(BaseConfig):

    minimum_distance_for_fuzzy_words_for_replace: float = 0.8
    minimum_distance_for_fuzzy_words_for_combine_request: float = 0.5
