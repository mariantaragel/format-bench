from .data_format import DataFormat
import dask.dataframe as dd
import pandas as pd
import dask
import glob
import os

class Hdf5Table(DataFormat):

    format_name = "HDF5.table"
    filetype = "h5"
    
    complevel: int

    def __init__(self, compression="zlib", complevel=None) -> None:
        super().__init__(compression)
        self.filename = f"tmp/test.{self.filetype}"
        self.pathname = f"tmp/test.*.{self.filetype}"
        self.complevel = complevel

    def save(self, data_set: pd.DataFrame):
        data_set.to_hdf(
            self.filename,
            index=False,
            key="data",
            format="table",
            complib=self.compression,
            complevel=self.complevel
        )

    def parallel_save(self, data_set: pd.DataFrame):
        dask.config.set({"dataframe.convert-string": False})
        dask_df = dd.from_pandas(data_set, npartitions=4)
        dd.to_hdf(
            dask_df,
            self.pathname,
            key="data",
            format="table",
            index=False,
            compute=True
        )

    def read(self) -> pd.DataFrame:
        return pd.read_hdf(self.filename, key="data")

    def parallel_read(self) -> pd.DataFrame:
        return dd.read_hdf(self.pathname, key="data").compute()
    
    def remove_all_files(self):
        files = glob.glob(self.pathname)

        for filename in files:
            if os.path.exists(filename):
                os.remove(filename)