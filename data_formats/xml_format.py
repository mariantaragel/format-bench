##
# @file xml_format.py
# @author Marián Tarageľ (xtarag01)

from .data_format import DataFormat
import pandas as pd

class Xml(DataFormat):
    """I/O operations with XML data format"""

    format_name = "XML"
    filetype = "xml"

    complevel: int

    def __init__(self) -> None:
        self.filename = f"tmp/test.{self.filetype}"

    def save(self, data_set: pd.DataFrame, compression=None, complevel=None):
        data_set.to_xml(self.filename, index=False, compression={"method": compression, "level": complevel})

    def read(self, compression="infer") -> pd.DataFrame:
        return pd.read_xml(self.filename, compression=compression)