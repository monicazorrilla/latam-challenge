# pyright: strict
from typing import List, Tuple
from collections import Counter
import concurrent.futures
import gc
import pandas as pd
from mgr.chunk_mgr import JsonChunkReader
from utils.tools import extract_emojis

class EmojiThreadAnalyzer:
    """
    Orchestrates the emoji extraction analysis by reading chunks and aggregating emoji counts concurrently.
    """

    def __init__(self, file_path: str, chunk_size: int, num_workers: int):
        self.reader = JsonChunkReader(file_path, chunk_size)
        self.num_workers = num_workers

    @staticmethod
    def _process_chunk(chunk: pd.DataFrame) -> Counter[str]:
        """
        CALL: _process_chunk(chunk: pd.DataFrame)
        DESCRIPTION: Processes a single chunk by extracting emojis from the 'content' column.
        Returns a Counter of emojis found in that chunk.
        RESULT: Counter[str]
        """
        local_counter: Counter[str] = Counter()
        chunk['emojis'] = chunk['content'].dropna().map(extract_emojis) # type: ignore

        for emojis in chunk['emojis']: # type: ignore
            local_counter.update(emojis) # type: ignore
            
        del chunk
        gc.collect()
        
        return local_counter

    def analyze(self, top_n: int = 10) -> List[Tuple[str, int]]:
        """
        CALL: analyze(self)
        DESCRIPTION: Processes all chunks concurrently, aggregates emoji counts,
        and returns the top 10 most common emojis.
        RESULT: List[Tuple[str, int]]
        """
        overall_counter: Counter[str] = Counter()
        
        with concurrent.futures.ThreadPoolExecutor(max_workers=self.num_workers) as executor:
            futures = [
                executor.submit(EmojiThreadAnalyzer._process_chunk, chunk)
                for chunk in self.reader.read_chunks()
            ]
            for future in concurrent.futures.as_completed(futures):
                overall_counter.update(future.result())
                
        return overall_counter.most_common(top_n)
