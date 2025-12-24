import importlib.resources
from functools import lru_cache
from threading import Lock

import pandas as pd


class GeoBlocks:
    """Load Geo blocks dataframe class"""

    _df: pd.DataFrame | None = None
    _lock: Lock = Lock()
    _file_path: str = "geo_blocks-0.2.parquet"

    @classmethod
    @lru_cache(maxsize=None)
    def get_geoblocks_df(cls) -> pd.DataFrame:
        """Returns the Geo blocks dataframe"""
        with cls._lock:
            if not cls._df:
                with importlib.resources.files("pystac_monty").joinpath(cls._file_path).open("rb") as f:
                    cls._df = pd.read_parquet(f, engine="pyarrow")

        assert cls._df.columns.size == 5  # Total columns in the parquet file
        return cls._df
