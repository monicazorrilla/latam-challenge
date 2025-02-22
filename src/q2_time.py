# pyright: strict
from utils.constants import *
from utils.tools import extract_emojis, get_app_args, get_stats_in_memory
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


# Function to process a chunk and count emojis
def process_chunk(chunk) -> Counter:
    emoji_counter = Counter()
    
    # Extract emojis efficiently
    chunk['emojis'] = chunk['content'].dropna().map(extract_emojis)
    
    # Update emoji count
    for emojis in chunk['emojis']:
        emoji_counter.update(emojis)

    return emoji_counter

@profile
def q2_time(file_path: str) -> List[Tuple[str, int]]:
    chunk_size = MEDIUM_CHUNK_SIZE
    num_workers = multiprocessing.cpu_count()
    
    # Initialize global counters
    emoji_counter = Counter()
    
    with concurrent.futures.ThreadPoolExecutor(max_workers=num_workers) as executor:
        futures = []
        chunks = pd.read_json(file_path, lines=True, chunksize=chunk_size)
        
        for chunk in chunks:
            futures.append(executor.submit(process_chunk, chunk))
    
        for future in concurrent.futures.as_completed(futures):
            emoji_counter.update(future.result())

    # Get the top 10 most used emojis
    top_10_emojis = emoji_counter.most_common(10)
    results: List = []

    for emoji_char, count in top_10_emojis:
        results.append((emoji_char, count))

    # Clear large objects to prevent memory buildup
    del chunks
    del emoji_counter
    gc.collect()  # Force garbage collection
    
    pprint(results, sort_dicts=False)
    return results
    
if __name__ == '__main__':
    try:
        app_args: argparse.Namespace = get_app_args()
        file_path = app_args.file_path

        # Force garbage collection before running the function
        gc.collect()
        
        # Create a profiler instance
        profiler = cProfile.Profile()
        profiler.enable()
        
        # Run the function
        q2_time(file_path)
        
        profiler.disable()
        
        # Print the stats output
        get_stats_in_memory(profiler)
        
    except Exception as err:
        logging.error(f"Error getting arguments, exception is {str(err)}", exc_info=err)
    finally:
        # Force memory cleanup after script execution**
        gc.collect()