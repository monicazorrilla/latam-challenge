# pyright: strict
from utils.constants import *
from utils.tools import extract_mentions, get_app_args, get_stats_in_memory
from typing import List, Tuple
from pprint import pprint
from memory_profiler import profile # type: ignore
from collections import Counter
import concurrent.futures
import multiprocessing
import pandas as pd
import cProfile
import gc
import argparse
import logging


def process_chunk(chunk) -> Counter:
    mention_counter = Counter()
    
    mentions = chunk['content'].dropna().map(extract_mentions)

    for mention_list in mentions:
        mention_counter.update(mention_list)

    return mention_counter

@profile
def q3_time(file_path: str) -> List[Tuple[str, int]]:
    chunk_size = MEDIUM_CHUNK_SIZE
    num_workers = multiprocessing.cpu_count()
    mention_counter = Counter()

    with concurrent.futures.ThreadPoolExecutor(max_workers=num_workers) as executor:
        futures = []
        chunks = pd.read_json(file_path, lines=True, chunksize=chunk_size)
        
        for chunk in chunks:
            futures.append(executor.submit(process_chunk, chunk))

        for future in concurrent.futures.as_completed(futures):
            mention_counter.update(future.result())

    top_10_users = mention_counter.most_common(10)
    results: List = []

    for username, count in top_10_users:
        results.append((username, count))

    del chunks
    del mention_counter
    gc.collect()  
    
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
        logging.error(f"Error getting arguments, exception is {str(err)}", exc_info=err)
    finally:
        gc.collect()