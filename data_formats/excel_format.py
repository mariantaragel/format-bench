##
# @file excel_format.py
# @author Marián Tarageľ (xtarag01)

from .data_format import DataFormat
import pandas as pd

class Excel(DataFormat):
    """I/O operations with CSV data format"""

    format_name = "Excel"
    filetype = "xlsx"

    def __init__(self) -> None:
        self.filename = f"tmp/test.{self.filetype}"

    def save(self, data_set: pd.DataFrame, compression=None, complevel=None):
        data_set.to_excel(self.filename, index=False)

    def read(self) -> pd.DataFrame:
        return pd.read_excel(self.filename)