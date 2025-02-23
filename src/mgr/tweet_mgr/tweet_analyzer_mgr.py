# pyright: strict
from typing import List, Tuple
from mgr.chunk_mgr import JsonChunkReader
from mgr.tweet_mgr.tweet_aggregator_mgr import TweetAggregator
import gc
from datetime import date

class TweetAnalyzer:
    """Orchestrates the analysis by reading chunks and aggregating statistics."""
    
    def __init__(self, reader: JsonChunkReader, aggregator: TweetAggregator):
        self.reader = reader
        self.aggregator = aggregator

    def analyze(self) -> List[Tuple[date, str]]:
        """
        CALL: analyze(self)
        DESCRIPTION: Processes all chunks and computes the results.
        RESULT: List[Tuple[date, str]]
        """
        for chunk in self.reader.read_chunks():
            self.aggregator.process_chunk(chunk)

            del chunk
            gc.collect()

        top_dates = self.aggregator.get_top_dates()
        results: List[Tuple[date, str]] = [] # type: ignore
        
        for date, _ in top_dates:
            top_user = self.aggregator.get_top_user_for_date(date)
            if top_user:
                results.append((date, top_user)) # type: ignore
                
        return results # type: ignore