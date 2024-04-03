from .data_format import DataFormat
import dask.dataframe as dd
import pandas as pd

class Orc(DataFormat):

    format_name = "ORC"
    filetype = "orc"
    
    def __init__(self, compression="uncompressed") -> None:
        super().__init__(compression)
        self.filename = f"tmp/test.{self.filetype}"
        self.pathname = f"{self.filename}/part.*.{self.filetype}"

    def save(self, data_set: pd.DataFrame):
        data_set.to_orc(self.filename, index=False, engine_kwargs={"compression": self.compression})

    def parallel_save(self, data_set: pd.DataFrame):
        dask_df = dd.from_pandas(data_set, npartitions=4)
        dd.to_orc(dask_df, self.filename, write_index=False, compute=True)

    def read(self) -> pd.DataFrame:
        return pd.read_orc(self.filename)

    def parallel_read(self) -> pd.DataFrame:
        return dd.read_orc(self.pathname).compute()