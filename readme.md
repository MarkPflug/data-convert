# `data-convert`: a .NET AOT experiment.

This project implements a command-line tool that can convert between a few data file formats: .csv, .xlsx, .parquet. This was created to experiment with the ahead-of-time (AOT) compilation feature in the latest versions of .NET.

On my Windows machine, the AOT compilation produces a 3.2MB executable. There are couple tricks involved in producing this size. First, `<XmlResolverIsNetworkingEnabledByDefault>` is set to `false`, which allows trimming unused code from the .NET XML library which is used to process .xlsx files. This reduces the executable size from ~10MB to ~7MB. Second, the [PublishAotCompressed](https://github.com/MichalStrehovsky/PublishAotCompressed) package is used to apply compression to the executable. This reduces the 7MB to the final ~3.2MB. Presumably, this introduces a bit of CPU overhead to decompress the executable at runtime. In use I wasn't able to observe any noticable delay, but it might be measurable with benchmarking.

This project uses [Sylvan.Data.Csv](https://github.com/MarkPflug/Sylvan), [Sylvan.Data.Excel](https://github.com/MarkPflug/Sylvan.Data.Excel), [Sylvan.Data.XBase](https://github.com/MarkPflug/Sylvan.Data.XBase), and [Parquet.Net](https://github.com/aloneguid/parquet-dotnet) libraries.

## Comparison

To compare this implementation to other languages that support AOT compilation, I've measured converting a ~6mb .xslx to .csv with OSS Rust and Go projects that also support the conversion. I've compiled all the projects locally, and noted the command used to compile. Not being familiar with Go or Rust, it's possible that there are compiler options that might influence those projects that I'm not using.

Metrics were captured using [Sylvan.Tools.ProcessInfo](https://github.com/MarkPflug/Sylvan.Tools.ProcessInfo).

| Language | Project | Command | ExeSize | Memory | Duration |
| --- | --- | --- | --- | --- | --- |
| C# | this project | `dotnet publish` | 3.2MB | 20.1MB | 00:00:00.6272401 |
| Rust | [boycce/xlsx-csv-rust](https://github.com/boycce/xlsx-csv-rust) | `cargo build -r` | 874KB | 74.1MB |  00:00:00.8500445 |
| GO | [tealeg/xlsx2csv](https://github.com/tealeg/xlsx2csv) | `go build` | 3.7MB | 398.3MB | 00:00:03.2214833 |

It would be unfair to draw any conclusions from these results other than that the C# Excel and CSV library implementations appear to be pretty competitive with those used by these other projects. While the C# executable is a bit larger than the Rust implementaiton, it is also more feature-rich, as it supports omnidirectional conversion for .csv, .xlsx, .xlsb, and .parquet files.

# Conclusion

.NET AOT is pretty cool, and seems competitive with what other languages offer.
