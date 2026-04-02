"""Data loader for Parquet/CSV/JSON."""
from __future__ import annotations
import logging
logger = logging.getLogger(__name__)
class DataLoader:
    def load_parquet(self, path: str) -> object:
        import pyarrow.parquet as pq
        return pq.read_table(path)
    def load_csv(self, path: str) -> object:
        import pyarrow.csv as acsv
        return acsv.read_csv(path)
