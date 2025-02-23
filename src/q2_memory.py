# pyright: strict
from mgr.emoji_mgr.emoji_aggregator_mgr import EmojiAggregator
from mgr.emoji_mgr.emoji_analyzer_mgr import EmojiAnalyzer
from mgr.chunk_mgr import JsonChunkReader
from utils.constants import SMALL_CHUNK_SIZE
from utils.tools import get_app_args, get_stats_in_memory
from typing import List, Tuple
from pprint import pprint
from memory_profiler import profile  # type: ignore
import cProfile
import gc
import logging


@profile
def q2_memory(file_path: str) -> List[Tuple[str, int]]:
    """
    CALL: q2_memory(file_path: str)
    DESCRIPTION: Processes a JSON file to extract the top 10 most used emojis (Focus on optimizing memory).
    RESULT: List[Tuple[str, int]
    """
    reader = JsonChunkReader(file_path, SMALL_CHUNK_SIZE)
    aggregator = EmojiAggregator()
    analyzer = EmojiAnalyzer(reader, aggregator)
    results = analyzer.analyze()
    
    pprint(results, sort_dicts=False)
    return results

if __name__ == '__main__':
    try:
        app_args = get_app_args()
        file_path = app_args.file_path
        
        gc.collect()
        profiler = cProfile.Profile()
        profiler.enable()

        q2_memory(file_path)

        profiler.disable()
        get_stats_in_memory(profiler)

    except Exception as err:
        logging.error(f"An unexpected error occurred, exception is {str(err)}", exc_info=err)
    finally:
        gc.collect()
