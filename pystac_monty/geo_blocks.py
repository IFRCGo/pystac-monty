import importlib.resources
from functools import lru_cache

import pandas as pd


class GeoBlocks:
    """Load Geo blocks dataframe class"""

    _file_path: str = "geo_blocks-0.2.parquet"
    _file_col_size: int = 5

    @classmethod
    @lru_cache(maxsize=None)
    def get_geoblocks_df(cls) -> pd.DataFrame:
        """Returns the Geo blocks dataframe"""
        with importlib.resources.files("pystac_monty").joinpath(cls._file_path).open("rb") as f:
            df = pd.read_parquet(f, engine="pyarrow")

        if df.columns.size != cls._file_col_size:
            raise ValueError("Unexpected number of columns in parquet file")
        return df
