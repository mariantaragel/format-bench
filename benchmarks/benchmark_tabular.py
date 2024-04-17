from data_formats import Csv, Json, Xml, Hdf5Fixed, Hdf5Table, Parquet, Feather, Orc, Pickle, Excel, Lance, Avro
from .benchmark_utils import BenchmarkUtils
import multiprocessing
import pandas as pd

class TabularBenchmarks:

    @staticmethod
    def benchmark_save(format, ds: pd.DataFrame, results):
        time = BenchmarkUtils.measure_time(lambda: format.save(ds))
        peak_mem = BenchmarkUtils.get_peak_memory()
        results["save_time (s)"] = round(time, 2)
        results["save_peak_mem (MB)"] = round(peak_mem / 1000, 2)

    @staticmethod
    def benchmark_read(format, results):
        time = BenchmarkUtils.measure_time(lambda: format.read())
        peak_mem = BenchmarkUtils.get_peak_memory()
        results["read_peak_mem (MB)"] = round(peak_mem / 1000, 2)
        results["read_time (s)"] = round(time, 2)

    def run(self, ds: pd.DataFrame) -> pd.DataFrame:
        formats_tabular = [Csv(), Json(), Xml(), Hdf5Fixed(), Hdf5Table(), Parquet(),
                        Feather(), Orc(), Pickle(), Excel(), Lance(), Avro()]
        manager = multiprocessing.Manager()
        results_tabular = []
        
        BenchmarkUtils.setup()
        
        for format in formats_tabular:
            results = manager.dict()
            results["format_name"] = format.format_name
            
            p_save = multiprocessing.Process(target=self.benchmark_save, args=(format, ds, results))
            p_save.start()
            p_save.join()

            results["file_size (MB)"] = round(format.size() / 1_000_000, 2)
            
            p_read = multiprocessing.Process(target=self.benchmark_read, args=(format, results))
            p_read.start()
            p_read.join()

            format.remove()
            results_tabular.append(dict(results))

        BenchmarkUtils.teardown()

        return pd.DataFrame(results_tabular)