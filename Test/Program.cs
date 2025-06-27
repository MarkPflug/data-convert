using Parquet;
using Parquet.Schema;
using Sylvan.Data;
using Sylvan.Data.XBase;
using System.Data;
using System.Data.Common;
using System.Text;

class S
{
    public int Id { get; set; }
    public string? Name { get; set; }
    public double? Value { get; set; }
}

class Program
{
    static void Main()
    {

        Encoding.RegisterProvider(CodePagesEncodingProvider.Instance);
        var x = Encoding.Default;
        var r = XBaseDataReader.Create("C:/data/dbf/github/yellowfeather/dbfdatareader/dbase_83.dbf", "C:/data/dbf/github/yellowfeather/dbfdatareader/dbase_83.dbt");

        for (int i = 0; i < r.FieldCount; i++)
        {
            Console.WriteLine($"{i} {r.GetName(i)}");
        }
        r.Read();
        r.Read();

        var xx = r.GetString(11);

    }

    static void DebugParq() { 
        // used to debug some Parquet.Net behavior and perf issues.
        var d = new[]
        {
            new S{Id = 1, Name = "a", Value = 3.14 },
            new S{Id = 2, Name = null, Value = 5 },
            new S{Id = 3, Name = "c", Value = null },
        };

        var r = d.AsDataReader();
        var ms = new MemoryStream();
        var count = r.WriteParquet(ms);
        ms.Seek(0, SeekOrigin.Begin);

        var pr = ParquetReader.CreateAsync(ms).Result;
        var dr = pr.AsDataReader();
        var dt = new DataTable();
        dt.Load(dr);
        
        var s = dr.GetColumnSchema();
        foreach (var col in s)
        {
            Console.WriteLine(col.DataTypeName + " " + col.AllowDBNull);
        }
        while (dr.Read())
        {
            for (int i = 0; i < dr.FieldCount; i++)
            {
                var v = dr.GetValue(i);
                Console.WriteLine(v);
            }
        }
        var rgr = pr.OpenRowGroupReader(0);
        var c0 = (DataField)pr.Schema[0];
        var c1 = (DataField)pr.Schema[1];
        var c2 = (DataField)pr.Schema[2];
        var aa = rgr.ReadColumnAsync(c0).Result;
        var bb = rgr.ReadColumnAsync(c1).Result;
        var cc = rgr.ReadColumnAsync(c2).Result;
        var t = pr.ReadEntireRowGroupAsync().Result;
    }
}
