from .data_format import DataFormat
import pandas as pd

class Feather(DataFormat):

    format_name = "Feather"
    filetype = "feather"

    compression_level: int

    def __init__(self, compression="uncompressed", compression_level=None) -> None:
        super().__init__(compression)
        self.filename = f"tmp/test.{self.filetype}"
        self.compression_level = compression_level

    def save(self, data_set: pd.DataFrame):
        data_set.to_feather(self.filename, compression=self.compression, compression_level=self.compression_level)

    def read(self) -> pd.DataFrame:
        return pd.read_feather(self.filename, use_threads=False)