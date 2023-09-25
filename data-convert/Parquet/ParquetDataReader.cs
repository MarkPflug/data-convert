using Parquet;
using Parquet.Data;
using System.Collections;
using System.Collections.ObjectModel;
using System.Data.Common;

/// <summary>
/// Extensions for ParquetReader.
/// </summary>
public static class ParquetReaderExtensions
{
    /// <summary>
    /// Gets a DbDataReader to access the contents of a ParquetReader.
    /// </summary>
    /// <param name="reader">The ParquetReader to read.</param>
    /// <returns>A DbDataReader instance.</returns>
    public static DbDataReader AsDataReader(this ParquetReader reader)
    {
        return new ParquetDataReader(reader);
    }
}

sealed class ParquetDataReader : DbDataReader, IDbColumnSchemaGenerator
{
    readonly ParquetReader reader;
    ParquetRowGroupReader rowGroup;
    int groupIdx;
    int groupOffset;

    int rowIdx;
    bool hasRows;
    DataColumn[] columns;
    DataField[] dfs;
    ReadOnlyCollection<DbColumn> schema;
    int fieldCount;

    public ParquetDataReader(ParquetReader reader)
    {
        this.reader = reader;
        this.rowIdx = -1;
        this.groupIdx = -1;
        var s = reader.Schema;
        this.fieldCount = s.Fields.Count;
        this.columns = new DataColumn[FieldCount];
        var schemaColumns = new DbColumn[fieldCount];
        this.dfs = new DataField[fieldCount]; 
        for (int i = 0; i < fieldCount; i++)
        {
            // TODO: what's a Field vs a DataField?
            var df = (DataField)s.Fields[i];
            dfs[i] = df;
            schemaColumns[i] = new ParquetDbColumn(df, i);
        }

        this.schema = new ReadOnlyCollection<DbColumn>(schemaColumns);
        bool hasGroup = NextRowGroup();
        this.hasRows = hasGroup && this.rowGroup.RowCount > 0;
    }

    bool NextRowGroup()
    {
        this.groupIdx++;
        if (groupIdx >= reader.RowGroupCount)
        {
            return false;
        }
        this.rowGroup = reader.OpenRowGroupReader(this.groupIdx);
        this.groupOffset = -1;
        for (int i = 0; i < fieldCount; i++)
        {
            this.columns[i] = rowGroup.ReadColumn(dfs[i]);
        }
        return true;
    }

    class ParquetDbColumn : DbColumn
    {
        public ParquetDbColumn(DataField field, int ordinal)
        {
            this.ColumnName = field.Name;
            this.AllowDBNull = field.HasNulls;
            this.ColumnOrdinal = ordinal;
            this.DataType = field.ClrType;
            if (this.DataType == typeof(DateTimeOffset))
                this.DataType = typeof(DateTime);
            if (this.DataType == typeof(byte[]))
            {
                this.DataType = typeof(string);
                this.AllowDBNull = false;
            }
            this.DataTypeName = field.DataType.ToString();
        }

        public override string ToString()
        {
            return $"{ColumnName} ({DataType})";
        }
    }

    public override object this[int ordinal] => GetValue(ordinal);

    public override object this[string name] => GetValue(GetOrdinal(name));

    public override int Depth => 0;

    public override int FieldCount => fieldCount;

    public override bool HasRows => hasRows;

    public override bool IsClosed => rowIdx == -1;

    public override int RecordsAffected => 0;

    T? MaybeGetValue<T>(int ordinal) where T : struct
    {
        var data = this.columns[ordinal].Data;
        if (data is T?[] n)
        {
            return n[groupOffset];            
        } 
        else
        {
            return ((T[])data)[groupOffset];
        }
    }

    bool IsNull<T>(int ordinal) where T : struct
    {
        var data = this.columns[ordinal].Data;
        if (data is T?[] n)
        {
            return n[groupOffset].HasValue == false;
        }
        return false;
    }

    T GetValue<T>(int ordinal) where T : struct
    {
        return MaybeGetValue<T>(ordinal).Value;
    }

    T GetObject<T>(int ordinal) where T : class
    {
        var col = this.columns[ordinal];       
        var data = col.Data;
        T value = ((T[])data)[groupOffset];
        if (value == null)
        {
            throw new InvalidCastException();
        }
        return value;
    }

    public override bool GetBoolean(int ordinal)
    {
        return GetValue<bool>(ordinal);
    }

    public override byte GetByte(int ordinal)
    {
        return GetValue<byte>(ordinal);
    }

    public override long GetBytes(int ordinal, long dataOffset, byte[] buffer, int bufferOffset, int length)
    {
        var data = ((byte[][])this.columns[ordinal].Data)[groupOffset];
        Array.Copy(data, dataOffset, buffer, bufferOffset, length);
        return length;
    }

    public override char GetChar(int ordinal)
    {
        return GetValue<char>(ordinal);
    }

    public override long GetChars(int ordinal, long dataOffset, char[] buffer, int bufferOffset, int length)
    {
        throw new NotImplementedException();
    }

    public override string GetDataTypeName(int ordinal)
    {
        return schema[ordinal].DataTypeName;
    }

    public override DateTime GetDateTime(int ordinal)
    {
        var dto = GetValue<DateTimeOffset>(ordinal);
        return dto.UtcDateTime;
    }

    public override decimal GetDecimal(int ordinal)
    {
        return GetValue<decimal>(ordinal);
    }

    public override double GetDouble(int ordinal)
    {
        return GetValue<double>(ordinal);
    }

    public override IEnumerator GetEnumerator()
    {
        throw new NotImplementedException();
    }

    public override Type GetFieldType(int ordinal)
    {
        return schema[ordinal].DataType;
    }

    public override float GetFloat(int ordinal)
    {
        return GetValue<float>(ordinal);
    }

    public override Guid GetGuid(int ordinal)
    {
        return GetValue<Guid>(ordinal);
    }

    public override short GetInt16(int ordinal)
    {
        return GetValue<short>(ordinal);
    }

    public override int GetInt32(int ordinal)
    {
        return GetValue<int>(ordinal);
    }

    public override long GetInt64(int ordinal)
    {
        return GetValue<long>(ordinal);
    }

    public override string GetName(int ordinal)
    {
        return schema[ordinal].ColumnName;
    }

    public override int GetOrdinal(string name)
    {
        for (int i = 0; i < schema.Count; i++)
        {
            if (schema[i].ColumnName == name)
                return i;
        }
        return -1;
    }

    public override string GetString(int ordinal)
    {
        string value = ((string[])this.columns[ordinal].Data)[groupOffset];
        if (value == null)
        {
            throw new InvalidCastException();
        }
        return value;
    }

    object OrNull<T>(int ord) where T : struct
    {
        var v = MaybeGetValue<T>(ord);
        if (v.HasValue)
            return v.Value;
        return DBNull.Value;
    }

    public override object GetValue(int ordinal)
    {
        var s = dfs[ordinal];
        switch (s.DataType)
        {
            case DataType.Boolean:
                return OrNull<bool>(ordinal);
            case DataType.Byte:
                return OrNull<byte>(ordinal);
            case DataType.Int16:
                return OrNull<short>(ordinal);
            case DataType.Int32:
                return OrNull<int>(ordinal);
            case DataType.Int64:
                return OrNull<long>(ordinal);
            case DataType.Short:
                return OrNull<float>(ordinal);
            case DataType.Double:
                return OrNull<double>(ordinal);
            case DataType.TimeSpan:
                throw new NotImplementedException();
            case DataType.DateTimeOffset:
                var dto = MaybeGetValue<DateTimeOffset>(ordinal);
                return dto.HasValue ? (object)dto.Value.UtcDateTime : DBNull.Value;
            case DataType.Decimal:
                return OrNull<decimal>(ordinal);
            case DataType.String:
                {
                    string? value = ((string[])this.columns[ordinal].Data)[groupOffset];
                    return (object)value ?? DBNull.Value;
                }
            case DataType.ByteArray:
                {
                    byte[] value = ((byte[][])this.columns[ordinal].Data)[groupOffset];
                    return value == null ? DBNull.Value : value;
                }
            default:
                throw new NotSupportedException();
        }
    }

    // TODO: this class is internal. Maybe make it public so we can embellish?
    public DateTimeOffset GetDateTimeOffset(int ordinal)
    {
        return GetValue<DateTimeOffset>(ordinal);
    }

    public override int GetValues(object[] values)
    {
        if (values == null) throw new ArgumentNullException(nameof(values));
        int c = Math.Min(values.Length, fieldCount);
        for (int i = 0; i < c; i++)
        {
            values[i] = GetValue(i);
        }
        return c;
    }

    public override bool IsDBNull(int ordinal)
    {
        var s = dfs[ordinal];
        if (s.HasNulls == false) return false;

        switch (s.DataType)
        {
            case DataType.Boolean:
                return IsNull<bool>(ordinal);
            case DataType.Byte:
                return IsNull<byte>(ordinal);
            case DataType.Int16:
                return IsNull<short>(ordinal);
            case DataType.Int32:
                return IsNull<int>(ordinal);
            case DataType.Int64:
                return IsNull<long>(ordinal);
            case DataType.Short:
                return IsNull<float>(ordinal);
            case DataType.Double:
                return IsNull<double>(ordinal);
            case DataType.TimeSpan:
                throw new NotImplementedException();
            case DataType.DateTimeOffset:
                return IsNull<DateTimeOffset>(ordinal);
            case DataType.Decimal:
                return IsNull<decimal>(ordinal);
            case DataType.String:
                return ((string[])this.columns[ordinal].Data)[groupOffset] == null;
            case DataType.ByteArray:
                return ((byte[][])this.columns[ordinal].Data)[groupOffset] == null;
            default:
                throw new NotSupportedException();
        }
    }

    public override bool NextResult()
    {
        this.rowIdx = -1;
        return false;
    }

    const int End = -2;

    public override bool Read()
    {
        if (rowIdx == End)
        {
            return false;
        }
        groupOffset++;
        if (groupOffset >= rowGroup.RowCount)
        {
            if (NextRowGroup())
            {
                groupOffset = 0;
            }
            else
            {
                this.rowIdx = End;
                return false;
            }
        }
        return true;
    }

    public ReadOnlyCollection<DbColumn> GetColumnSchema()
    {
        return schema;
    }
}
