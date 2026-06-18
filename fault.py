import random

def should_corrupt(probability: float) -> bool:
    return random.random() * 100 < probability

def corrupt_message(message: str) -> str:
    return message + "#ERRO"

def maybe_corrupt(message: str, probability: float) -> str:
    if should_corrupt(probability):
        return corrupt_message(message)
    return message