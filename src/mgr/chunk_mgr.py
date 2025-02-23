#pyright : strict
# mgr/chunk_mgr.py
import pandas as pd

class JsonChunkReader:
    """Reads a JSON file in chunks."""
    def __init__(self, file_path: str, chunk_size: int):
        self.file_path = file_path
        self.chunk_size = chunk_size

    def read_chunks(self):
        """Generator that yields DataFrame chunks from the JSON file."""
        return pd.read_json(self.file_path, lines=True, chunksize=self.chunk_size) #type: ignore