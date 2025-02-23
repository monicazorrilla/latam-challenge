# pyright: strict
from mgr.user_mgr.user_thread_mgr import UserThreadAnalyzer
from utils.constants import MEDIUM_CHUNK_SIZE
from utils.tools import get_app_args, get_stats_in_memory
from typing import List, Tuple
from pprint import pprint
from memory_profiler import profile  # type: ignore
import cProfile
import gc
import multiprocessing
import argparse
import logging


@profile
def q3_time(file_path: str) -> List[Tuple[str, int]]:
    """
    CALL: q3_time(file_path: str)
    DESCRIPTION: Processes a JSON file to extract the top 10 mentioned users (Focus on optimizing time).
    RESULT: List[Tuple[str, int]
    """
    num_workers = multiprocessing.cpu_count()
    analyzer = UserThreadAnalyzer(file_path, MEDIUM_CHUNK_SIZE, num_workers)
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

        q3_time(file_path)

        profiler.disable()
        get_stats_in_memory(profiler)

    except Exception as err:
        logging.error(f"An unexpected error occurred, exception is {str(err)}", exc_info=err)
    finally:
        gc.collect()
