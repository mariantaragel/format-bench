##
# @file benchmark_tabular.py
# @author Marián Tarageľ (xtarag01)
# @brief Tabular benchmark suite

from data_formats import Csv, Json, Xml, Hdf5Fixed, Hdf5Table, Parquet, Feather, Orc, Pickle, Excel, Lance, Avro
from .benchmark_utils import BenchmarkUtils
import multiprocessing
import pandas as pd

class TabularBenchmarks:

    @staticmethod
    def benchmark_save(format, ds: pd.DataFrame, results):
        peak_mem_before = BenchmarkUtils.get_peak_memory()
        time = BenchmarkUtils.measure_time(lambda: format.save(ds))
        peak_mem_after = BenchmarkUtils.get_peak_memory()
        results["save_time (s)"] = round(time, 2)
        peak_mem = peak_mem_after - peak_mem_before
        results["save_peak_mem (MB)"] = round(peak_mem / 1000, 2)

    @staticmethod
    def benchmark_read(format, results):
        peak_mem_before = BenchmarkUtils.get_peak_memory()
        time = BenchmarkUtils.measure_time(lambda: format.read())
        peak_mem_after = BenchmarkUtils.get_peak_memory()
        peak_mem = peak_mem_after - peak_mem_before
        results["read_peak_mem (MB)"] = round(peak_mem / 1000, 2)
        results["read_time (s)"] = round(time, 2)

    def run(self, ds: pd.DataFrame) -> pd.DataFrame:
        formats_tabular = [Csv(), Json(), Xml(), Hdf5Fixed(), Hdf5Table(), Parquet(),
                            Feather(), Orc(), Pickle(), Excel(), Lance(), Avro()]
        manager = multiprocessing.Manager()
        results_tabular = []
        
        BenchmarkUtils.setup()
        
        for i, format in enumerate(formats_tabular):
            
            progress = round((i + 1) / len(formats_tabular) * 100, 2)
            print(f"[{progress:.2f} %] Benchmarking {format.format_name}")

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
