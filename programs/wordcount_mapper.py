"""
WordCount Mapper for GridMR
This file can be loaded dynamically from URLs like:
nfs://shared/gridmr/programs/wordcount_mapper.py
"""

import re
from typing import Iterator, Any
from mapreduce.framework import Mapper
from mapreduce.types import KeyValue


class WordCountMapper(Mapper):
    """
    Mapper that tokenizes input text and emits (word, 1) pairs.
    """

    def map(self, key: Any, value: Any) -> Iterator[KeyValue]:
        """
        Split text into words and emit each word with count 1.

        Args:
            key: Line number (ignored)
            value: Line of text to process

        Yields:
            KeyValue pairs of (word, 1)
        """
        # Convert to string and normalize
        text = str(value).lower()

        # Split into words (remove punctuation and whitespace)
        words = re.findall(r"\b[a-zA-Z]+\b", text)

        # Emit each word with count 1
        for word in words:
            if word:  # Skip empty strings
                yield KeyValue(key=word, value=1)
