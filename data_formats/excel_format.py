from .data_format import DataFormat
import pandas as pd

class Excel(DataFormat):

    format_name = "Excel"
    filetype = "xlsx"

    def __init__(self, compression=None) -> None:
        super().__init__(compression)
        self.filename = f"tmp/test.{self.filetype}"

    def save(self, data_set: pd.DataFrame):
        data_set.to_excel(self.filename, index=False)

    def read(self) -> pd.DataFrame:
        return pd.read_excel(self.filename)