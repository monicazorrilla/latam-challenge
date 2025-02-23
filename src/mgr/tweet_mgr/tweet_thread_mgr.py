# pyright: strict
from mgr.chunk_mgr import JsonChunkReader
from typing import List, Tuple, Dict
from datetime import date
from collections import Counter, defaultdict
import concurrent.futures
import gc
import pandas as pd


class TweetThreadAnalyzer:
    """Processes a JSON file concurrently to aggregate date and user counts."""
    
    def __init__(self, file_path: str, chunk_size: int, num_workers: int):
        self.reader = JsonChunkReader(file_path, chunk_size)
        self.num_workers = num_workers

    @staticmethod
    def _process_chunk(chunk: pd.DataFrame) -> Tuple[Counter, Dict[date, Counter]]: # type: ignore
        """
        CALL: process_chunk(chunk: pd.DataFrame)
        DESCRIPTION: Processes a single chunk:
        - Normalizes the 'date' column.
        - Computes local date counts.
        - Computes per-date user counts.
        RESULT: Tuple[Counter, Dict[date, Counter]]
        """
        chunk['date'] = chunk['date'].dt.date
        local_date_counts = Counter(chunk['date'].value_counts().to_dict()) # type: ignore
        local_date_user_counts = defaultdict(Counter) # type: ignore
        
        # Process each row in the chunk using dropna on 'user'
        for d, user in zip( # type: ignore
            chunk['date'], # type: ignore
            chunk['user'].dropna().map(lambda u: u.get('username')) # type: ignore
        ):
            if user:
                local_date_user_counts[d][user] += 1
        
        del chunk
        gc.collect()
        
        return local_date_counts, local_date_user_counts # type: ignore

    def analyze(self) -> List[Tuple[date, str]]:
        """
        CALL: analyze(self)
        DESCRIPTION:  Processes all chunks concurrently and aggregates the results.
        Returns a list of tuples with the top 10 dates and their most common user.
        RESULT: List[Tuple[date, str]]
        """
        overall_date_counts = Counter() # type: ignore
        overall_date_user_counts = defaultdict(Counter) # type: ignore
        
        with concurrent.futures.ThreadPoolExecutor(max_workers=self.num_workers) as executor:
            futures = [ # type: ignore
                executor.submit(TweetThreadAnalyzer._process_chunk, chunk) # type: ignore
                for chunk in self.reader.read_chunks()
            ]

            for future in concurrent.futures.as_completed(futures): # type: ignore
                local_date_counts, local_date_user_counts = future.result() # type: ignore
                overall_date_counts.update(local_date_counts) # type: ignore
                for d, counter in local_date_user_counts.items(): # type: ignore
                    overall_date_user_counts[d].update(counter) # type: ignore
        
        top_10_dates = overall_date_counts.most_common(10) # type: ignore
        
        results: List[Tuple[date, str]] = []
        
        for d, _ in top_10_dates: # type: ignore
            top_user = overall_date_user_counts[d].most_common(1) # type: ignore
            if top_user:
                results.append((d, top_user[0][0])) # type: ignore
        
        return results
