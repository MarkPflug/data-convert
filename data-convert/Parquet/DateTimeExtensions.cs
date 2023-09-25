
static class DateTimeExtension
{
    // SQL doesn't store a "kind" or any timezone information with date values
    // when processing dates, we need to prevent the parquet.net library from
    // converting to the local timezone, because it only supports DateTimeOffset
    public static DateTimeOffset AsDateTimeOffsetUtc(this DateTime dt)
    {
        // to avoid DateTimeOffset from applying a "local" timezone conversion
        var utc = new DateTime(dt.Ticks, DateTimeKind.Utc);
        var dto = (DateTimeOffset)utc;
        return dto;
    }
}
