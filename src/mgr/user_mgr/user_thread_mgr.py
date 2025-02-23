# pyright: strict
from mgr.chunk_mgr import JsonChunkReader
from typing import List, Tuple
from collections import Counter
from utils.tools import extract_mentions
import concurrent.futures
import gc
import pandas as pd


class UserThreadAnalyzer:
    """
    Orchestrates the user extraction analysis by reading chunks and aggregating mention counts concurrently.
    """
    
    def __init__(self, file_path: str, chunk_size: int, num_workers: int):
        self.reader = JsonChunkReader(file_path, chunk_size)
        self.num_workers = num_workers

    @staticmethod
    def _process_chunk(chunk: pd.DataFrame) -> Counter[str]:
        """
        CALL: _process_chunk(chunk: pd.DataFrame)
        DESCRIPTION:  Processes a single chunk to extract mentions and returns a Counter.
        RESULT: Counter[str]
        """
        mention_counter: Counter[str] = Counter()
        mentions = chunk['content'].dropna().map(extract_mentions) # type: ignore
        
        for mention_list in mentions:
            mention_counter.update(mention_list)
        
        del chunk
        gc.collect()
        
        return mention_counter
    
    def analyze(self, top_n: int = 10) -> List[Tuple[str, int]]:
        """
        CALL: analyze(self)
        DESCRIPTION: Processes all chunks concurrently, aggregates mention counts,
        and returns the top 10 most common mentions.
        RESULT: List[Tuple[str, int]]
        """
        overall_counter: Counter[str] = Counter()
        
        with concurrent.futures.ThreadPoolExecutor(max_workers=self.num_workers) as executor:
            futures = [
                executor.submit(UserThreadAnalyzer._process_chunk, chunk)
                for chunk in self.reader.read_chunks()
            ]
            for future in concurrent.futures.as_completed(futures):
                overall_counter.update(future.result())
                
        return overall_counter.most_common(top_n)
