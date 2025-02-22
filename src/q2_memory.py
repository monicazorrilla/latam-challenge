# pyright: strict
from utils.constants import *
from utils.tools import extract_emojis, get_app_args, get_stats_in_memory
from typing import List, Tuple
from pprint import pprint
from memory_profiler import profile # type: ignore
from collections import Counter
import pandas as pd
import cProfile
import gc
import argparse
import logging


@profile
def q2_memory(file_path: str)-> List[Tuple[str, int]]:
    chunk_size = SMALL_CHUNK_SIZE
    chunks = pd.read_json(file_path, lines=True, chunksize=chunk_size)
    
    # Initialize global counters
    emoji_counter = Counter()
    
    for chunk in chunks:
        # Extract emojis from content
        chunk['emojis'] = chunk['content'].astype(str).map(extract_emojis)

        # Update the counter with emojis from each chunk
        for emojis in chunk['emojis']:
            emoji_counter.update(emojis)
        
        # Force memory cleanup after each chunk
        del chunk
        gc.collect()  # Force garbage collection

    # Get the top 10 most used emojis
    top_10_emojis = emoji_counter.most_common(10)
    results: List = []

    for emoji_char, count in top_10_emojis:
        results.append((emoji_char, count))

    # Clear large objects to prevent memory buildup
    del chunks
    del emoji_counter
    gc.collect() 
    
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
        q2_memory(file_path)
        
        profiler.disable()
        
        # Print the stats output
        get_stats_in_memory(profiler)
        
    except Exception as err:
        logging.error(f"Error getting arguments, exception is {str(err)}", exc_info=err)
    finally:
        # Force memory cleanup after script execution**
        gc.collect()