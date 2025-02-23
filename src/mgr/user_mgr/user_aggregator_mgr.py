# pyright: strict
from typing import List, Tuple
from collections import Counter
import pandas as pd
import gc
from utils.tools import extract_mentions

class UserAggregator:
    """Aggregates user mention counts from JSON chunks."""
    
    def __init__(self):
        self.user_counter: Counter[str] = Counter()

    def process_chunk(self, chunk: pd.DataFrame) -> None:
        """
        CALL: process_chunk(self, chunk: pd.DataFrame)
        DESCRIPTION: Processes a DataFrame chunk, extracts mentions from the 'content' column, and updates the counter.
        RESULT: None
        """
        chunk['mentions'] = chunk['content'].fillna("").map(extract_mentions) # type: ignore
        
        for mentions in chunk['mentions']: # type: ignore
            self.user_counter.update(mentions) # type: ignore
        
        del chunk
        gc.collect()

    def get_top_mentions(self, top_n: int = 10) -> List[Tuple[str, int]]:
        """
        CALL: get_top_mentions(self, top_n: int = 10) 
        DESCRIPTION: Returns the top N mentions with their counts.
        RESULT: List[Tuple[str, int]]
        """
        return self.user_counter.most_common(top_n)
