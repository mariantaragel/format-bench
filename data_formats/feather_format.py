##
# @file feather_format.py
# @author Marián Tarageľ (xtarag01)

from .data_format import DataFormat
import pandas as pd

class Feather(DataFormat):
    """I/O operations with Feather data format"""

    format_name = "Feather"
    filetype = "feather"

    def __init__(self) -> None:
        self.filename = f"tmp/test.{self.filetype}"

    def save(self, data_set: pd.DataFrame, compression="uncompressed", complevel=None):
        data_set.to_feather(self.filename, compression=compression, compression_level=complevel)

    def read(self) -> pd.DataFrame:
        return pd.read_feather(self.filename, use_threads=False)