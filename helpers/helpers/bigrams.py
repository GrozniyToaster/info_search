def get_bigrams_of_word(word: str) -> list[str]:
    return [
        word[i: i + 2]
        for i in range(len(word) - 1)
    ]
