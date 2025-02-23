# pyright: strict
from typing import List, Tuple
from mgr.chunk_mgr import JsonChunkReader
from mgr.user_mgr.user_aggregator_mgr import UserAggregator

class UserAnalyzer:
    """
    Orchestrates the user mention extraction analysis by reading JSON chunks and
    aggregating mention counts using a UserAggregator.
    """
    
    def __init__(self, reader: JsonChunkReader, aggregator: UserAggregator):
        self.reader = reader
        self.aggregator = aggregator

    def analyze(self) -> List[Tuple[str, int]]:
        """
        CALL: analyze(self)
        DESCRIPTION: Processes all chunks to update the user mention counter and returns the top mentions.
        RESULT: List[Tuple[str, int]]
        """
        for chunk in self.reader.read_chunks():
            self.aggregator.process_chunk(chunk)
            
        return self.aggregator.get_top_mentions()
