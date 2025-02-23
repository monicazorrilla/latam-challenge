# pyright: strict
from typing import List, Tuple
from collections import Counter, defaultdict
from datetime import date
import pandas as pd

class TweetAggregator:
    """Aggregates date counts and user occurrences per date."""
    
    def __init__(self):
        self.date_counts: Counter = Counter() #type: ignore
        self.date_user_counts: defaultdict = defaultdict(Counter) #type: ignore

    def process_chunk(self, chunk: pd.DataFrame) -> None:
        """
        CALL: process_chunk(self, chunk: pd.DataFrame)
        DESCRIPTION: Processes a DataFrame chunk and updates counters.
        RESULT: None
        """
        chunk['date'] = chunk['date'].dt.date
        self.date_counts.update(chunk['date'].value_counts().to_dict()) #type: ignore

        for date, user in zip(chunk['date'], chunk['user'].apply(lambda u: u.get('username'))): #type: ignore
            if user:
                self.date_user_counts[date][user] += 1#type: ignore

    def get_top_dates(self, top_n: int = 10) -> List[Tuple[date, int]]:
        """
        CALL: get_top_dates(self, top_n: int = 10)
        DESCRIPTION: Returns the top N dates with the highest counts.
        RESULT: List[Tuple[date, int]]
        """
        return self.date_counts.most_common(top_n) #type: ignore

    def get_top_user_for_date(self, date: date) -> str:
        """
        CALL: get_top_user_for_date(self, date: date)
        DESCRIPTION: Returns the most common user for a given date.
        RESULT: str
        """
        top_user: str = self.date_user_counts[date].most_common(1) #type: ignore
        if top_user:
            return top_user[0][0] #type: ignore
        return ""


