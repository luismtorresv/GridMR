"""
Example MapReduce implementations for testing the GridMR system.
"""

from mapreduce import Mapper, Reducer
from mapreduce.types import KeyValue
from typing import Iterator, Any
import re


class WordCountMapper(Mapper):
    """Classic word count mapper - emits each word with count 1"""

    def map(self, key: Any, value: Any) -> Iterator[KeyValue]:
        # Clean and split the text
        words = re.findall(r"\b\w+\b", str(value).lower())
        for word in words:
            yield KeyValue(key=word, value=1)


class WordCountReducer(Reducer):
    """Classic word count reducer - sums counts for each word"""

    def reduce(self, key: Any, values: Iterator[Any]) -> Iterator[KeyValue]:
        total_count = sum(int(v) for v in values)
        yield KeyValue(key=key, value=total_count)


class CharacterCountMapper(Mapper):
    """Counts characters in text"""

    def map(self, key: Any, value: Any) -> Iterator[KeyValue]:
        for char in str(value).lower():
            if char.isalpha():
                yield KeyValue(key=char, value=1)


class CharacterCountReducer(Reducer):
    """Sums character counts"""

    def reduce(self, key: Any, values: Iterator[Any]) -> Iterator[KeyValue]:
        total_count = sum(int(v) for v in values)
        yield KeyValue(key=key, value=total_count)


class LineLengthMapper(Mapper):
    """Maps line number to line length"""

    def map(self, key: Any, value: Any) -> Iterator[KeyValue]:
        line_length = len(str(value))
        yield KeyValue(key="line_length", value=line_length)


class AverageLengthReducer(Reducer):
    """Calculates average line length"""

    def reduce(self, key: Any, values: Iterator[Any]) -> Iterator[KeyValue]:
        lengths = [int(v) for v in values]
        if lengths:
            average = sum(lengths) / len(lengths)
            yield KeyValue(key="average_line_length", value=round(average, 2))


# Registry for dynamic loading
MAPPER_REGISTRY = {
    "wordcount": WordCountMapper,
    "charcount": CharacterCountMapper,
    "linelength": LineLengthMapper,
}

REDUCER_REGISTRY = {
    "wordcount": WordCountReducer,
    "charcount": CharacterCountReducer,
    "linelength": AverageLengthReducer,
}
