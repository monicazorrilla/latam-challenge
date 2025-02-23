# pyright: strict
from mgr.tweet_mgr.tweet_thread_mgr import TweetThreadAnalyzer
from utils.constants import MEDIUM_CHUNK_SIZE
from utils.tools import get_app_args, get_stats_in_memory
from typing import List, Tuple
from pprint import pprint
from datetime import date
from memory_profiler import profile  # type: ignore
import cProfile
import gc
import multiprocessing
import logging


@profile    
def q1_time(file_path: str) -> List[Tuple[date, str]]:
    """
    CALL: q1_time(file_path: str)
    DESCRIPTION: Processes a JSON file concurrently to extract the top user for each of the top 10 dates (Focus on optimizing time).
    RESULT: List[Tuple[date, str]]
    """
    num_workers = multiprocessing.cpu_count()
    analyzer = TweetThreadAnalyzer(file_path, MEDIUM_CHUNK_SIZE, num_workers)
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

        q1_time(file_path)

        profiler.disable()
        get_stats_in_memory(profiler)

    except Exception as err:
        logging.error(f"An unexpected error occurred, exception is {str(err)}", exc_info=err)
    finally:
        gc.collect()
