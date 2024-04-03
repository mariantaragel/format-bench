from .data_format import DataFormat
import dask.dataframe as dd
import pandas as pd

class Parquet(DataFormat):

    format_name = "Parquet"
    filetype = "parquet"
    
    def __init__(self, compression=None) -> None:
        super().__init__(compression)
        self.filename = f"tmp/test.{self.filetype}"
        self.pathname = f"{self.filename}/part.*.{self.filetype}"

    def save(self, data_set: pd.DataFrame):
        data_set.to_parquet(self.filename, index=False, engine="pyarrow", compression=self.compression)

    def parallel_save(self, data_set: pd.DataFrame):
        dask_df = dd.from_pandas(data_set, npartitions=4)
        dd.to_parquet(dask_df, self.filename, engine="pyarrow", write_index=False)

    def read(self) -> pd.DataFrame:
        return pd.read_parquet(self.filename)

    def parallel_read(self) -> pd.DataFrame:
        return dd.read_parquet(self.pathname).compute()