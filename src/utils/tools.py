# src/utils/tools.py
from typing import Any
from cProfile import Profile
import argparse
import pstats
import io
import re
import emoji

def extract_mentions(text: Any) -> list[Any]:
    """
    CALL: extract_emojis(text: Any)
    DESCRIPTION: This method extracts mentions from a tweet.
    RESULT: list[Any]
    """
    # Regular expression to extract mentions (@username)
    mention_pattern = re.compile(r"@(\w+)")
    return mention_pattern.findall(text) if isinstance(text, str) else []

def extract_emojis(text: Any) -> list[Any]:
    """
    CALL: extract_emojis(text: Any)
    DESCRIPTION: This method extracts emojis from text.
    RESULT: list[Any]
    """
    return [char for char in text if char in emoji.EMOJI_DATA] if isinstance(text, str) else []

def get_app_args() -> argparse.Namespace:
    """
    CALL: get_app_args()
    DESCRIPTION: This method defines how command-line arguments should be parsed.
    RESULT: argparse.Namespace
    """
    parser = argparse.ArgumentParser()
    parser.add_argument("-file_path", type=str, help="file_path")
    return parser.parse_args()

def get_stats_in_memory(profiler: Profile) -> None:
    """
    CALL: get_app_args()
    DESCRIPTION: This method print the total execution time of the script.
    RESULT:None
    """
    # Capture the stats output
    s = io.StringIO()
    stats = pstats.Stats(profiler, stream=s)
    stats.strip_dirs().sort_stats(pstats.SortKey.CUMULATIVE)
    
    # Extract total execution time
    total_time = stats.total_tt  
    
    print(f"Total execution time: {total_time:.6f} seconds")
