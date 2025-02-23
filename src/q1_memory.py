# pyright: strict
from mgr.tweet_mgr.tweet_aggregator_mgr import TweetAggregator
from mgr.tweet_mgr.tweet_analyzer_mgr import TweetAnalyzer
from mgr.chunk_mgr import JsonChunkReader
from utils.constants import SMALL_CHUNK_SIZE
from utils.tools import get_app_args, get_stats_in_memory
from typing import List, Tuple
from pprint import pprint
from memory_profiler import profile  # type: ignore
from datetime import date
import cProfile
import gc
import argparse
import logging


@profile
def q1_memory(file_path: str) -> List[Tuple[date, str]]:
    """
    CALL: q1_memory(file_path: str)
    DESCRIPTION: Processes a JSON file to extract the top user for each of the top 10 dates (Focus on optimizing memory).
    RESULT: List[Tuple[date, str]]
    """
    reader = JsonChunkReader(file_path, SMALL_CHUNK_SIZE)
    aggregator = TweetAggregator()
    analyzer = TweetAnalyzer(reader, aggregator)
    results = analyzer.analyze()

    pprint(results, sort_dicts=False)
    return results

if __name__ == '__main__':
    try:
        app_args: argparse.Namespace = get_app_args()
        file_path = app_args.file_path

        gc.collect()
        
        profiler = cProfile.Profile()
        profiler.enable()
        
        q1_memory(file_path)
        
        profiler.disable()
        
        get_stats_in_memory(profiler)
        
    except Exception as err:
        logging.error(f"An unexpected error occurred, exception is {str(err)}", exc_info=err)
    finally:
        gc.collect()