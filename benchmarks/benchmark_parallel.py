from data_formats import Csv, Json, Hdf5Table, Parquet, Orc
from dask.distributed import Client, LocalCluster
from .benchmark_utils import BenchmarkUtils
import multiprocessing
import pandas as pd

class ParallelBenchmarks:

    @staticmethod
    def benchmark_save(format, ds, results):
        peak_mem_before = BenchmarkUtils.get_peak_memory()
        time = BenchmarkUtils.measure_time(lambda: format.parallel_save(ds))
        peak_mem_after = BenchmarkUtils.get_peak_memory()
        results["save_time (s)"] = round(time, 2)
        peak_mem = peak_mem_after - peak_mem_before
        results["save_peak_mem (MB)"] = round(peak_mem / 1000, 2)

    @staticmethod
    def benchmark_read(format, results):
        peak_mem_before = BenchmarkUtils.get_peak_memory()
        time = BenchmarkUtils.measure_time(lambda: format.parallel_read())
        peak_mem_after = BenchmarkUtils.get_peak_memory()
        peak_mem = peak_mem_after - peak_mem_before
        results["read_peak_mem (MB)"] = round(peak_mem / 1000, 2)
        results["read_time (s)"] = round(time, 2)

    def run(self, ds: pd.DataFrame):
        cluster = LocalCluster()
        client = Client(cluster)
        scatter_ds = client.scatter(ds)

        formats_tabular = [Parquet()]
        manager = multiprocessing.Manager()
        results_tabular = []

        BenchmarkUtils.setup()

        for i, format in enumerate(formats_tabular):
            results = manager.dict()
            print(f"[{round((i+1) / len(formats_tabular) * 100, 2)} %] benchmarking {format.format_name}")
            results["format_name"] = format.format_name
            
            p_save = multiprocessing.Process(target=self.benchmark_save, args=(format, scatter_ds, results))
            p_save.start()
            p_save.join()

            results["file_size (MB)"] = round(format.size_all_files() / 1_000_000, 2)
            
            p_read = multiprocessing.Process(target=self.benchmark_read, args=(format, results))
            p_read.start()
            p_read.join()

            format.remove_all_files()
            results_tabular.append(dict(results))

        BenchmarkUtils.teardown()
        client.close()

        return pd.DataFrame(results_tabular)


