"""
WordCount Reducer for GridMR
This file can be loaded dynamically from URLs like:
nfs://shared/gridmr/programs/wordcount_reducer.py
"""

from typing import Iterator, Any
from mapreduce.framework import Reducer
from mapreduce.types import KeyValue


class WordCountReducer(Reducer):
    """
    Reducer that sums up word counts.
    """

    def reduce(self, key: Any, values: Iterator[Any]) -> Iterator[KeyValue]:
        """
        Sum all counts for a given word.

        Args:
            key: The word
            values: Iterator of counts (all should be 1 from mapper)

        Yields:
            KeyValue pair of (word, total_count)
        """
        total_count = 0

        # Sum all values for this key
        for value in values:
            try:
                count = int(value)
                total_count += count
            except (ValueError, TypeError):
                # Skip invalid values
                print(f"⚠️  Invalid count value for word '{key}': {value}")
                continue

        # Only emit if we have a positive count
        if total_count > 0:
            yield KeyValue(key=key, value=total_count)
