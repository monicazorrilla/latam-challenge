# pyright: strict
from utils.constants import *
from utils.tools import get_app_args, get_stats_in_memory
from typing import List, Tuple
from pprint import pprint
from datetime import datetime
from memory_profiler import profile # type: ignore
from collections import Counter, defaultdict
import concurrent.futures
import multiprocessing
import pandas as pd
import cProfile
import gc
import argparse
import logging

def process_chunk(chunk):
    chunk['date'] = chunk['date'].dt.date
    date_counts = Counter(chunk['date'].value_counts().to_dict())
    date_user_counts = defaultdict(Counter)

    for date, user in zip(chunk['date'], chunk['user'].dropna().map(lambda u: u.get('username'))):
        if user:
            date_user_counts[date][user] += 1

    del chunk
    gc.collect() 
    
    return date_counts, date_user_counts

@profile    
def q1_time(file_path: str) -> List[Tuple[datetime.date, str]]:
    chunk_size = MEDIUM_CHUNK_SIZE
    num_workers = multiprocessing.cpu_count()
    date_counts = Counter()  
    date_user_counts = defaultdict(Counter)  
        
    with concurrent.futures.ThreadPoolExecutor(max_workers=num_workers) as executor: 
        futures = []
        chunks = pd.read_json(file_path, lines=True, chunksize=chunk_size)
        
        for chunk in chunks:
            futures.append(executor.submit(process_chunk, chunk))
    
        for future in concurrent.futures.as_completed(futures):
            local_date_counts, local_date_user_counts = future.result()
            date_counts.update(local_date_counts)
            for date, user_counts in local_date_user_counts.items():
                date_user_counts[date].update(user_counts)
    
    top_10_dates = date_counts.most_common(10)
    results: List = []
    
    for date, _ in top_10_dates:
        top_user = date_user_counts[date].most_common(1)
        if top_user:
            user_name, _ = top_user[0]
            results.append((date, user_name))

    del chunks
    del date_counts
    del local_date_counts
    del date_user_counts
    del local_date_user_counts
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
        
        q1_time(file_path)
        
        profiler.disable()
        
        get_stats_in_memory(profiler)

    except Exception as err:
        logging.error(f"Error getting arguments, exception is {str(err)}", exc_info=err)
    finally:
        gc.collect()