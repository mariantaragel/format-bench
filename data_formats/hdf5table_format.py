##
# @file hdf5table_format.py
# @author Marián Tarageľ (xtarag01)

from .data_format import DataFormat
import dask.dataframe as dd
import pandas as pd
import dask
import glob
import os

class Hdf5Table(DataFormat):
    """I/O operations with HDF5 table data format"""

    format_name = "HDF5.table"
    filetype = "h5"

    def __init__(self) -> None:
        self.filename = f"tmp/test.{self.filetype}"
        self.pathname = f"tmp/test.*.{self.filetype}"

    def save(self, data_set: pd.DataFrame, compression="zlib", complevel=None):
        data_set.to_hdf(
            self.filename,
            index=False,
            key="data",
            format="table",
            complib=f"blosc:{compression}",
            complevel=complevel
        )

    def parallel_save(self, data_set: pd.DataFrame, n: int):
        dask.config.set({"dataframe.convert-string": False})
        dask_df = dd.from_pandas(data_set, npartitions=n)
        dd.to_hdf(
            dask_df,
            self.pathname,
            index=False,
            key="data",
            format="table",
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
