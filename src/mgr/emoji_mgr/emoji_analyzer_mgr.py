# pyright: strict
from typing import List, Tuple
from mgr.chunk_mgr import JsonChunkReader
from mgr.emoji_mgr.emoji_aggregator_mgr import EmojiAggregator

class EmojiAnalyzer:
    """
    Orchestrates the emoji extraction analysis by reading chunks and aggregating emoji counts.
    """
    
    def __init__(self, reader: JsonChunkReader, aggregator: EmojiAggregator):
        self.reader = reader
        self.aggregator = aggregator

    def analyze(self) -> List[Tuple[str, int]]:
        """
        CALL: analyze(self)
        DESCRIPTION:  Processes each chunk to extract emojis and then returns the top emojis.
        RESULT: List[Tuple[str, int]]
        """
        for chunk in self.reader.read_chunks():
            self.aggregator.process_chunk(chunk)

        return self.aggregator.get_top_emojis()
