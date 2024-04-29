##
# @file main.py
# @author Marián Tarageľ (xtarag01)
# @brief Main script of the benchmarks

from visualize import make_report, show_results
from benchmarks import CompressionBenchmarks
from benchmarks import TabularBenchmarks
from benchmarks import ImageBenchmarks
from data_generator import Generator
import argparse
import sys

if __name__ == '__main__':

    parser = argparse.ArgumentParser(prog="main.py", description="Benchmark of data formats")
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--tabular", dest="tabular", action="store_true", help="Run Tabular benchmark suite.")
    group.add_argument("--compression", dest="compression", action="store_true", help="Run Compression benchmark suite.")
    group.add_argument("--image", dest="image", action="store_true", help="Run Image benchmark suite.")
    parser.add_argument("--webface", dest="webface_path", metavar="<path>", action="store", required=False,
                        help="Run benchmarks with the Webaface10M dataset. The <path> is path to the Webface10M dataset.")
    parser.add_argument("--report", dest="report", action="store_true", required=False, help="Generate report from results.")
    
    args = parser.parse_args()
    generator = Generator()

    if args.tabular:
        benchmark = TabularBenchmarks()
        print("· Preparing Tabular benchmark")
        if args.webface_path:
            datasets = [
                generator.load_webface10M(args.webface_path)
            ]
        else:
            datasets = [
                generator.gen_dataset("eq", 1_000_000, 2, 2, 2, 2, 0, 0),
                generator.gen_dataset("int", 1_000_000, 5, 1, 1, 1, 0, 0),
                generator.gen_dataset("float", 1_000_000, 1, 5, 1, 1, 0, 0),
                generator.gen_dataset("bool", 1_000_000, 1, 1, 5, 1, 0, 0),
                generator.gen_dataset("str", 1_000_000, 1, 1, 1, 5, 0, 0)
            ]

        for dataset in datasets:
            print(f"· Running Tabular benchmark on dataset {dataset}")
            results = benchmark.run(dataset.df)

            location = f"tabular_{dataset}.csv"
            report_path = None
            if args.report:
                report_path = f"tabular_{dataset}.pdf"
            show_results(results, location, report_path)

    if args.compression:
        benchmark = CompressionBenchmarks()
        print("· Preparing Compression benchmark")
        if args.webface_path:
            datasets = [generator.load_webface10M(args.webface_path)]
        else:
            datasets = [generator.gen_dataset("eq", 1_000_000, 2, 2, 2, 2, 0, 0)]
        compressions = ["lz4", "zstd"]
        complevel = 1

        for dataset in datasets:
            for compression in compressions:
                print(f"· Running Compression benchmark with codec {compression} on dataset {dataset}")
                results = benchmark.run(dataset.df, compression, complevel)

                location = f"compression_{dataset}_{compression}.csv"
                report_path = None
                if args.report:
                    report_path = f"compression_{dataset}_{compression}.pdf"
                show_results(results, location, report_path)

    if args.image:
        benchmark = ImageBenchmarks()
        print("· Preparing Image benchmark")
        datasets = [
            generator.load_cifar_10(50_000),
            #generator.load_imagenet_100(126_689)
        ]
        for dataset in datasets:
            print(f"· Running Image benchmark on dataset {dataset}")
            results = benchmark.run(dataset.images, dataset.labels)

            location = f"image_{dataset}.csv"
            report_path = None
            if args.report:
                report_path = f"image_{dataset}.pdf"
            show_results(results, location, report_path)