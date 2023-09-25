# `data-convert`: a .NET AOT experiment.

This project is a command-line tool that can convert between a few data file formats: .csv, .xlsx, .parquet. This was created to experiment with the ahead-of-time (AOT) compilation feature in the latest versions of .NET.

On my Windows machine, the AOT compilation produces a 3.2MB executable. There are couple tricks involved in producing this size. First, `<XmlResolverIsNetworkingEnabledByDefault>` is set to `false`, which allows trimming unused code from the .NET XML library which is used to process .xlsx files. This reduces the executable size from ~10MB to ~7MB. Second, the [PublishAotCompressed](https://github.com/MichalStrehovsky/PublishAotCompressed) package is used to apply compression to the executable. This reduces the 7MB to the final ~3.2MB. Presumably, this introduces a bit of CPU overhead to decompress the executable at runtime. In use I wasn't able to observe any noticable delay, but it might be measurable with benchmarking.

This project uses [Sylvan.Data.Csv](https://github.com/MarkPflug/Sylvan), [Sylvan.Data.Excel](https://github.com/MarkPflug/Sylvan.Data.Excel), and [Parquet.Net](https://github.com/aloneguid/parquet-dotnet) libraries. These libraries are not AOT-ready, and produce some AOT warnings that I have completely ignored.

## Comparison

To compare this implementation to other languages that support AOT compilation, I've measured converting a ~6mb .xslx to .csv with OSS Rust and Go projects that also support the conversion. I've compiled all the projects locally, and noted the command used to compile. Not being familiar with Go or Rust, it's possible that there are compiler options that might influence those projects that I'm not using.

C#, this project:
Compile: `dotnet publish`
Exe size: 3.2MB
Memory: 22MB
CPU Total: 00:00:01.1406250

GO, [tealeg/xlsx2csv](https://github.com/tealeg/xlsx2csv):
Compile: `go build`
Exe size: 3.7MB
Memory: 413MB
CPU Total: 00:00:04.4218750

Rust, [zitsen/xlsx2csv.rs](https://github.com/zitsen/xlsx2csv.rs):
Compile: `cargo build -r`
Exe size: 3.7MB
Memory: 359MB
CPU Total: 00:04:29.7343750

It would be unfair to draw any conclusions from these results other than that my C# Excel and CSV library implementations appear to be well optimized in comparison to those used by these other projects. I'm not experienced with either Go or Rust and so unfit to (and uninterested in) assessing how those implementations might be improved. It is also notable that the C# implementation has smaller executable size while being more feature-rich: it supports omnidirectional conversion for .csv, .xlsx, .xlsb, and .parquet files.

# Conclusion

.NET AOT is pretty cool stuff.
