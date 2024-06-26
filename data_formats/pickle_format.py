##
# @file pickle_format.py
# @author Marián Tarageľ (xtarag01)

from .data_format import DataFormat
import pandas as pd

class Pickle(DataFormat):
    """I/O operations with Pickle data format"""

    format_name = "Pickle"
    filetype = "pkl"
    
    def __init__(self) -> None:
        self.filename = f"tmp/test.{self.filetype}"

    def save(self, data_set: pd.DataFrame, compression=None, complevel=None):
        data_set.to_pickle(self.filename, compression={"method": compression, "level": complevel})

    def read(self, compression="infer") -> pd.DataFrame:
        return pd.read_pickle(self.filename, compression=compression)