from .data_format import DataFormat
import pandas as pd

class Hdf5Fixed(DataFormat):

    format_name = "HDF5.fixed"
    filetype = "h5"

    complevel: int

    def __init__(self, compression="zlib", complevel=None) -> None:
        super().__init__(compression)
        self.filename = f"tmp/test.{self.filetype}"
        self.complevel = complevel

    def save(self, data_set: pd.DataFrame):
        data_set.to_hdf(
            self.filename,
            index=False,
            key="data",
            format="fixed",
            complib=self.compression,
            complevel=self.complevel
        )

    def read(self) -> pd.DataFrame:
        return pd.read_hdf(self.filename)