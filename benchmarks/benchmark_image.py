from image_storage import PngImage, Base64String, Hdf5Image, ParquetImage, Sqlite, LmdbImage
from .benchmark_utils import BenchmarkUtils
import multiprocessing
import pandas as pd

class ImageBenchmarks:

    @staticmethod
    def benchmark_save(format, images: list, labels: list, results):
        time = BenchmarkUtils.measure_time(lambda: format.save(images, labels))
        peak_mem = BenchmarkUtils.get_peak_memory()
        results["save_time (s)"] = round(time, 2)
        results["save_peak_mem (MB)"] = round(peak_mem / 1000, 2)

    @staticmethod
    def benchmark_read(format, results):
        time = BenchmarkUtils.measure_time(lambda: format.read())
        peak_mem = BenchmarkUtils.get_peak_memory()
        results["read_peak_mem (MB)"] = round(peak_mem / 1000, 2)
        results["read_time (s)"] = round(time, 2)

    def run(self, images: list, labels: list) -> pd.DataFrame:
        formats_image = [PngImage(), Base64String(), Hdf5Image(), ParquetImage(), Sqlite(), LmdbImage()]
        manager = multiprocessing.Manager()
        results_image = []
        
        BenchmarkUtils.setup()

        for format in formats_image:
            results = manager.dict()
            results["format_name"] = format.format_name

            p_save = multiprocessing.Process(target=self.benchmark_save, args=(format, images, labels, results))
            p_save.start()
            p_save.join()

            results["file_size (MB)"] = round(format.size() / 1_000_000, 2)

            p_read = multiprocessing.Process(target=self.benchmark_read, args=(format, results))
            p_read.start()
            p_read.join()

            format.remove()
            results_image.append(dict(results))

        BenchmarkUtils.teardown()

        return pd.DataFrame(results_image)