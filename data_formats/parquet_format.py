##
# @file parquet_format.py
# @author Marián Tarageľ (xtarag01)

from .data_format import DataFormat
import dask.dataframe as dd
import pandas as pd

class Parquet(DataFormat):
    """I/O operations with Parquet data format"""

    format_name = "Parquet"
    filetype = "parquet"
    
    def __init__(self) -> None:
        self.filename = f"tmp/test.{self.filetype}"
        self.pathname = f"{self.filename}/part.*.{self.filetype}"

    def save(self, data_set: pd.DataFrame, compression=None, complevel=None):
        data_set.to_parquet(self.filename, index=False, engine="pyarrow", compression=compression)

    def parallel_save(self, data_set):
        dask_df = dd.from_delayed([data_set])
        dd.to_parquet(dask_df, self.filename, write_index=False, engine="pyarrow")

    def read(self) -> pd.DataFrame:
        return pd.read_parquet(self.filename)

    def parallel_read(self) -> pd.DataFrame:
        return dd.read_parquet(self.pathname).compute()
