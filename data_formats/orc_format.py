##
# @file orc_format.py
# @author Marián Tarageľ (xtarag01)

from .data_format import DataFormat
import dask.dataframe as dd
import pandas as pd

class Orc(DataFormat):
    """I/O operations with ORC data format"""

    format_name = "ORC"
    filetype = "orc"
    
    def __init__(self) -> None:
        self.filename = f"tmp/test.{self.filetype}"
        self.pathname = f"{self.filename}/part.*.{self.filetype}"

    def save(self, data_set: pd.DataFrame, compression="uncompressed", complevel=None):
        data_set.to_orc(self.filename, index=False, engine_kwargs={"compression": compression})

    def parallel_save(self, data_set: pd.DataFrame, n: int):
        dask_df = dd.from_pandas(data_set, npartitions=n)
        dd.to_orc(dask_df, self.filename, write_index=False, compute=True)

    def read(self) -> pd.DataFrame:
        return pd.read_orc(self.filename)

    def parallel_read(self) -> pd.DataFrame:
        return dd.read_orc(self.pathname).compute()
