using Parquet;
using Parquet.Data;
using Sylvan.Data;
using System.Data;
using System.Data.Common;

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

        var pr = new ParquetReader(ms);
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
        var aa = rgr.ReadColumn(c0);
        var bb = rgr.ReadColumn(c1);
        var cc = rgr.ReadColumn(c2);
        var t = pr.ReadAsTable();
    }
}
