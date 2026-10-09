using ICSharpCode.SharpZipLib.Zip;
using System.Text;

namespace LaLune.ToTS.Crypto;

/// <summary>
/// Упаковка/распаковка одного файла в ZIP с AES-256 паролем (SharpZipLib).
/// </summary>
public static class ZipWithPassword
{
    /// <summary>
    /// Упаковывает один файл (из bytes) в zip с паролем.
    /// </summary>
    public static byte[] PackSingle(string fileName, byte[] data, string password)
    {
        using var ms = new MemoryStream();

        using (var zip = ZipFile.Create(ms))
        {
            zip.BeginUpdate();
            zip.Password = password;
            zip.SetLevel(9);
            zip.Add(new StaticDataSource(data), fileName);
            zip.CommitUpdate();
        }

        return ms.ToArray();
    }

    /// <summary>
    /// Распаковывает единственный файл из zip с паролем.
    /// </summary>
    public static byte[] UnpackSingle(byte[] zipBytes, string password)
    {
        using var ms = new MemoryStream(zipBytes);
        using var zip = new ZipFile(ms);
        zip.Password = password;

        foreach (ZipEntry entry in zip)
        {
            if (!entry.IsFile) continue;

            using var es = zip.GetInputStream(entry);
            using var outMs = new MemoryStream();
            es.CopyTo(outMs);
            return outMs.ToArray();
        }

        throw new InvalidDataException("ZIP не содержит файлов");
    }

    /// <summary>
    /// Достаёт имя первого файла из zip (для отладки).
    /// </summary>
    public static string? GetFirstFileName(byte[] zipBytes, string password)
    {
        using var ms = new MemoryStream(zipBytes);
        using var zip = new ZipFile(ms);
        zip.Password = password;

        foreach (ZipEntry entry in zip)
            if (entry.IsFile) return entry.Name;

        return null;
    }

    private sealed class StaticDataSource : IStaticDataSource
    {
        private readonly byte[] _data;
        public StaticDataSource(byte[] data) => _data = data;
        public Stream GetSource() => new MemoryStream(_data, writable: false);
    }
}
