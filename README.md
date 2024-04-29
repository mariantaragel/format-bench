# FormatBench
FormatBench is a Python benchmark of data formats. This project aims to evaluate different data formats for storing tabular and image data.

Check out also: [ASV FormatBench](https://github.com/mariantaragel/asv-format-bench)

## Usage
```
python3 main.py (--tabular|--compression|--image) [--webface <path>] [--report]
```
`--tabular` - run tabular benchmark suite<br/>
`--compression` - run compression benchmark suite<br/>
`--image` - run image benchmark suite<br/>
`--webface` - run benchmarks with the Webface10M dataset; `<path>` is a path to the Webface10M dataset<br/>
`--report` - generate report from the benchmark results<br/>

## Examples of usage
Run tabular benchmarks: `python3 main.py --tabular`

Run image benchmarks and create report: `python3 main.py --image --report`

Run compression benchmarks with the Webface10M dataset: `python3 main.py --compression --webface ~/synthetic_webface10M.h5`

Run tabular benchmarks with the Webface10M dataset and create report: `python3 main.py --tabular --webface ~/synthetic_webface10M.h5 --report`

## Related publication
TARAGEĽ, Marián. *Column-oriented and Image Data Format Benchmarks*. Brno, 2024. Bachelor’s thesis. Brno University of Technology, Faculty of Information Technology. Supervisor Ing. Jakub Špaňhel

## Acknowledgements
I would like to convey my gratitude to Ing. Jakub Špaňhel for his supervision. I also express my thanks to my consultant Ing. Petr Chmelař. Both of them provided me with support and advice during the work on this thesis. Last but not least, I would like to thank the external submitter, the Innovatrics company, for their professional help.
