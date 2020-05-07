from itertools import chain, islice
from typing import Iterable, Iterator


def batch(iterable: Iterable, batch_size: int) -> list:
    source_iterable: Iterator = iter(iterable)

    while True:
        batch_iterable: Iterable = islice(source_iterable, batch_size)
        try:
            yield list(chain([next(batch_iterable)], batch_iterable))
        except StopIteration:
            return
