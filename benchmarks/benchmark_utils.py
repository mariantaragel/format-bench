from pathlib import Path
import resource
import timeit

class BenchmarkUtils:

    @staticmethod
    def setup():
        Path("./tmp").mkdir(exist_ok=True)

    @staticmethod
    def teardown():
        Path("./tmp").rmdir()

    @staticmethod
    def measure_time(code: callable) -> float:
        return timeit.timeit(code, number=1)

    @staticmethod
    def get_peak_memory() -> int:
        return resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
