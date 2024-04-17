from benchmarks import TabularBenchmarks
from benchmarks import ParallelBenchmarks
from benchmarks import ImageBenchmarks
from data_generator import Generator
import sys

generator = Generator()

if sys.argv[1] == "--tabular":
    benchmark = TabularBenchmarks()
    ds = generator.gen_dataset("test", 1000, 6, 18, 6, 0, 0, 0)
    results = benchmark.run(ds.df)

elif sys.argv[1] == "--parallel":
    benchmark = ParallelBenchmarks()
    ds = generator.gen_dataset("test", 1000, 6, 18, 6, 0, 0, 0)
    results = benchmark.run(ds.df)

elif sys.argv[1] == "--image":
    benchmark = ImageBenchmarks()
    ds = generator.load_imagenet_100(50)
    results = benchmark.run(ds.images, ds.labels)

results.to_csv("results.csv", index=False)
print(results)