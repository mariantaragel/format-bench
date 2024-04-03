from .data_format import DataFormat
import dask.dataframe as dd
import pandas as pd

class Csv(DataFormat):

    format_name = "CSV"
    filetype = "csv"

    complevel: int

    def __init__(self, compression=None, complevel=None) -> None:
        super().__init__(compression)
        self.filename = f"tmp/test.{self.filetype}"
        self.pathname = f"{self.filename}/*.part"
        self.complevel = complevel

    def save(self, data_set: pd.DataFrame):
        data_set.to_csv(self.filename, index=False, compression={"method": self.compression, "level": self.complevel})

    def parallel_save(self, data_set: pd.DataFrame):
        dask_df = dd.from_pandas(data_set, npartitions=4)
        dd.to_csv(dask_df, self.filename, index=False)

    def read(self) -> pd.DataFrame:
        return pd.read_csv(self.filename, compression=self.compression)

    def parallel_read(self) -> pd.DataFrame:
        return dd.read_csv(self.pathname).compute()