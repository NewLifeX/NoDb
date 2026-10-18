using NewLife.NoDb;

var path = Path.Combine(Path.GetTempPath(), "nodb-aot-" + Guid.NewGuid().ToString("N") + ".db");
try
{
    using var db = new ListDb(path, false);
    db.Add([1, 2, 3]);

    var data = db.Get(0);
    if (data == null || data.Length != 3) return 1;

    db.Set(0, [4, 5, 6]);
    var updated = db.Get(0);
    if (updated == null || updated.Length != 3 || updated[0] != 4) return 1;

    Console.WriteLine("NoDb AOT smoke passed.");
    return 0;
}
finally
{
    try { File.Delete(path); } catch { }
}
