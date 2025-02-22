# pyright: strict
from utils.constants import *
from utils.tools import get_app_args, get_stats_in_memory
from typing import List, Tuple
from pprint import pprint
from datetime import datetime
from memory_profiler import profile # type: ignore
from collections import Counter, defaultdict
import pandas as pd
import cProfile
import gc
import argparse
import logging
    
@profile
def q1_memory(file_path: str) -> List[Tuple[datetime.date, str]]:
    chunk_size = SMALL_CHUNK_SIZE
    chunks = pd.read_json(file_path, lines=True, chunksize=chunk_size)
    date_counts = Counter() 
    date_user_counts = defaultdict(Counter)
    
    for chunk in chunks:
        chunk['date'] = chunk['date'].dt.date
        
        date_counts.update(chunk['date'].value_counts().to_dict())
        
        for date, user in zip(chunk['date'], chunk['user'].apply(lambda u: u.get('username'))):
            if user: 
                date_user_counts[date][user] += 1
            
        del chunk
        gc.collect()
        
    top_10_dates = date_counts.most_common(10)
    results: List = []
    
    for date, _ in top_10_dates:
        top_user = date_user_counts[date].most_common(1)
        if top_user:
            user_name, _ = top_user[0]
            results.append((date, user_name))

    del chunks
    del date_counts
    del date_user_counts
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
        
        q1_memory(file_path)
        
        profiler.disable()
        
        get_stats_in_memory(profiler)
        
    except Exception as err:
        logging.error(f"Error getting arguments, exception is {str(err)}", exc_info=err)
    finally:
        gc.collect()