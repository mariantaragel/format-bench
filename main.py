from benchmarks import TabularBenchmarks
from benchmarks import ParallelBenchmarks
from benchmarks import ImageBenchmarks
from data_generator import DataSet
import sys

data_gen = DataSet()

if sys.argv[1] == "--tabular":
    benchmark = TabularBenchmarks()
    ds = data_gen.gen_data_set(10_000, 6, 18, 0, 0, 0, 6)
    results = benchmark.run(ds)

elif sys.argv[1] == "--parallel":
    benchmark = ParallelBenchmarks()
    ds = data_gen.gen_data_set(1000, 6, 18, 0, 0, 0, 6)
    results = benchmark.run(ds)

elif sys.argv[1] == "--image":
    benchmark = ImageBenchmarks()
    ds = data_gen.load_imagenet_100(6000)
    results = benchmark.run(ds.images, ds.labels)

#results.to_csv("results/results.csv", index=False)
print(results)