from data_formats import Csv, Json, Hdf5Table, Parquet, Orc
from .benchmark_utils import BenchmarkUtils
from distributed import Client
import multiprocessing
import pandas as pd

class ParallelBenchmarks:

    @staticmethod
    def benchmark_save(format, ds, results):
        time = BenchmarkUtils.measure_time(lambda: format.parallel_save(ds))
        peak_mem = BenchmarkUtils.get_peak_memory()
        results["save_time (s)"] = round(time, 2)
        results["save_peak_mem (kB)"] = round(peak_mem / 1000, 2)

    @staticmethod
    def benchmark_read(format, results):
        time = BenchmarkUtils.measure_time(lambda: format.parallel_read())
        peak_mem = BenchmarkUtils.get_peak_memory()
        results["read_peak_mem (kB)"] = round(peak_mem / 1000, 2)
        results["read_time (ms)"] = round(time, 2)

    def run(self, ds: pd.DataFrame):
        Client('127.0.0.1:8787')

        formats_tabular = [Csv(), Json(), Hdf5Table(), Parquet(), Orc()]
        manager = multiprocessing.Manager()
        results_tabular = []

        BenchmarkUtils.setup()

        for format in formats_tabular:
            results = manager.dict()
            results["format_name"] = format.format_name
            
            p_save = multiprocessing.Process(target=self.benchmark_save, args=(format, ds, results))
            p_save.start()
            p_save.join()

            results["file_size (MB)"] = round(format.size_all_files() / 1_000_000, 2)
            
            p_read = multiprocessing.Process(target=self.benchmark_read, args=(format, results))
            p_read.start()
            p_read.join()

            format.remove_all_files()
            results_tabular.append(dict(results))

        BenchmarkUtils.teardown()

        return pd.DataFrame(results_tabular)