from benchmarks import TabularBenchmarks
from benchmarks import ParallelBenchmarks
from benchmarks import ImageBenchmarks
from benchmarks import CompressionBenchmarks
from data_generator import Generator
import sys

if __name__ == '__main__':

    generator = Generator()

    if sys.argv[1] == "--tabular":
        benchmark = TabularBenchmarks()
        ds = generator.load_webface10M()
        #ds = generator.gen_dataset("eq", 1_000_000, 2, 2, 2, 2, 0, 0)
        results = benchmark.run(ds.df)

    elif sys.argv[1] == "--compression":
        benchmark = CompressionBenchmarks()
        ds = generator.load_webface10M()
        #ds = generator.gen_dataset("eq", 1000, 2, 2, 2, 2, 0, 0)
        results = benchmark.run(ds.df, "zstd")

    elif sys.argv[1] == "--parallel":
        benchmark = ParallelBenchmarks()
        #ds = generator.load_webface10M()
        ds = generator.gen_dataset("eq", 1024, 2, 2, 2, 2, 0, 0)
        results = benchmark.run(ds.df)

    elif sys.argv[1] == "--image":
        benchmark = ImageBenchmarks()
        #ds = generator.load_cifar_10(50_000)
        ds = generator.load_imagenet_100(126_689)
        results = benchmark.run(ds.images, ds.labels)

    results.to_csv("results.csv", index=False)
    print(results)
