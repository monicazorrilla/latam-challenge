# pyright: strict
from typing import List, Tuple
from collections import Counter
import pandas as pd
import gc
from utils.tools import extract_emojis

class EmojiAggregator:
    """Aggregates emoji counts from JSON chunks."""
    
    def __init__(self):
        self.emoji_counter: Counter[str] = Counter()

    def process_chunk(self, chunk: pd.DataFrame) -> None:
        """
        CALL: process_chunk(self, chunk: pd.DataFrame)
        DESCRIPTION: Extract emojis from the 'content' column in the chunk and update the counter.
        RESULT: None
        """
        chunk['emojis'] = chunk['content'].astype(str).map(extract_emojis)
        
        for emojis in chunk['emojis']: # type: ignore
            self.emoji_counter.update(emojis) # type: ignore

        del chunk
        gc.collect()

    def get_top_emojis(self, top_n: int = 10) -> List[Tuple[str, int]]:
        """
        CALL: get_top_emojis(self, top_n: int = 10)
        DESCRIPTION: Returns the top N emojis along with their counts.
        RESULT: List[Tuple[str, int]]
        """
        return self.emoji_counter.most_common(top_n)
