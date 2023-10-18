using Parquet;
using Sylvan.Data;
using Sylvan.Data.Csv;
using Sylvan.Data.Excel;
using Sylvan.Data.XBase;
using System.Data.Common;
using System.Diagnostics;

internal class Program
{
    static int Main(string[] args)
    {
        if (args.Length < 2)
        {
            Console.WriteLine("Usage:");
            Console.WriteLine("data-convert [input] [output]");
            Console.WriteLine("Supported formats:");
            Console.WriteLine(".csv .xlsx .xlsb .parquet");
            return 1;
        }

        var inputFile = args[0];
        var outputFile = args[1];

        var inExt = Path.GetExtension(inputFile);
        var outExt = Path.GetExtension(outputFile);

        static Schema Analyze(DbDataReader reader)
        {
            Console.WriteLine("Analyzing input data schema");
            // TODO: make configurable
            var a = new SchemaAnalyzer(new SchemaAnalyzerOptions { AnalyzeRowCount = 100000 });
            var sw = Stopwatch.StartNew();
            var schema = a.Analyze(reader).GetSchema();
            sw.Stop();
            Console.WriteLine($"Schema analyzed. {sw.Elapsed}");
            return schema;
        }

        switch (inExt.ToLowerInvariant())
        {
            case ".xls":
            case ".xlsx":
            case ".xlsb":
            case ".csv":
            case ".parq":
            case ".parquet":
            case ".dbf":
                // okay
                break;
            default:
                Console.WriteLine("Unsupported input file type " + outExt);
                return 2;
        }

        switch (outExt.ToLowerInvariant())
        {
            case ".xlsx":
            case ".xlsb":
            case ".csv":
            case ".parq":
            case ".parquet":
                // okay
                break;
            default:
                Console.WriteLine("Unsupported output file type " + outExt);
                return 3;
        }
        Console.WriteLine($"Converting {inputFile} to {outputFile}");
        var sw = Stopwatch.StartNew();
        DbDataReader reader;
        Schema schema;
        long count = 0;
        switch (inExt.ToLowerInvariant())
        {
            case ".csv":                
                using (var r = CsvDataReader.Create(inputFile))
                {
                    schema = Analyze(r);
                }
                var csvSchema = new CsvSchema(schema);
                reader = CsvDataReader.Create(inputFile, new CsvDataReaderOptions { Schema = csvSchema });
                break;
            case ".dbf":
                reader = XBaseDataReader.Create(inputFile);
                break;
            case ".xlsx":
            case ".xlsb":
            case ".xls":

                IExcelSchemaProvider excelSchema;
                if (outExt.ToLowerInvariant() != ".csv")
                {
                    using (var r = ExcelDataReader.Create(inputFile))
                    {
                        schema = Analyze(r);
                    }
                    excelSchema = new ExcelSchema(true, schema);
                }
                else
                {
                    excelSchema = ExcelSchema.Dynamic;
                }
                reader = ExcelDataReader.Create(inputFile, new ExcelDataReaderOptions { Schema = excelSchema });
                break;
            case ".parq":
            case ".parquet":
                var inputStream = File.OpenRead(inputFile);
                var pr = new ParquetReader(inputStream);
                reader = new ParquetDataReader(pr);
                break;
            default:
                Console.WriteLine("Unsupported input file type " + inExt);
                return 2;
        }

        switch (outExt.ToLowerInvariant())
        {
            case ".xlsx":
            case ".xlsb":
                using (var w = ExcelDataWriter.Create(outputFile))
                {
                    more:
                    var result = w.Write(reader);
                    count += result.RowsWritten - 1; // reports "header" row as a record
                    if (!result.IsComplete) goto more;
                }
                break;
            case ".csv":
                var opt = new CsvDataWriterOptions { MaxBufferSize = 0x100000 };
                using (var w = CsvDataWriter.Create(outputFile, opt))
                {
                    count = w.Write(reader);
                }
                break;
            case ".parq":
            case ".parquet":
                using (var os = File.Create(outputFile))
                {
                    count = reader.WriteParquet(os);
                }
                break;
            default:
                Console.WriteLine("Unsupported output file type " + outExt);                
                return 3;
        }
        sw.Stop();

        Console.WriteLine($"Wrote {count} records in {sw.Elapsed}.");
        return 0;
    }
}
