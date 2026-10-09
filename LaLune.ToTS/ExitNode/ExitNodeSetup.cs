using System.Security.Cryptography;
using LaLune.ToTS.Crypto;

namespace LaLune.ToTS.ExitNode;

/// <summary>
/// Генерация конфига для exit_node:
///   1. Генерирует случайный ToTS-ключ (everySymbol + Symbols).
///   2. Сохраняет .db в temp.
///   3. Упаковывает .db в ZIP с паролем (AES-256).
///   4. Кодирует ZIP в base64.
///   5. Печатает tots://... в консоль.
///
/// Также возвращает TotsCipher для немедленного использования в exit_node.
/// </summary>
public static class ExitNodeSetup
{
    public sealed class Result
    {
        public required TotsCipher Cipher { get; init; }
        public required string Config { get; init; }
        public required long HandshakeGroup { get; init; }
    }

    /// <summary>
    /// Генерирует конфиг. Группу handshake нужно создать заранее в MAX
    /// и передать её ID.
    /// </summary>
    public static Result Generate(long handshakeGroup, string password)
    {
        var rng = RandomNumberGenerator.Create();
        var rand = new Random(BitConverter.ToInt32(NextBytes(rng, 4)));

        var (every, symbols) = TotsKeyDb.GenerateRandom(rand);

        // Сохраняем .db во временный файл.
        var tmpDir = Path.Combine(Path.GetTempPath(),
            "tots_exit_" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(tmpDir);
        var dbPath = Path.Combine(tmpDir, TotsKeyDb.DbFileName);
        TotsKeyDb.Write(dbPath, every, symbols);

        // ZIP с паролем.
        var dbBytes = File.ReadAllBytes(dbPath);
        var zipBytes = ZipWithPassword.PackSingle(TotsKeyDb.DbFileName, dbBytes, password);

        // Чистим temp.
        try { Directory.Delete(tmpDir, recursive: true); } catch { }

        // base64 → конфиг.
        var keyB64 = Convert.ToBase64String(zipBytes);
        // URL-encode: меняем '+' на '%2B', '=' уже валиден, '/' валиден, но обезопасим.
        var keyEncoded = Uri.EscapeDataString(keyB64);

        var config =
            $"tots://handshake_group={handshakeGroup}" +
            $"&password={Uri.EscapeDataString(password)}" +
            $"&key={keyEncoded}";

        var cipher = new TotsCipher(every, symbols);

        return new Result
        {
            Cipher = cipher,
            Config = config,
            HandshakeGroup = handshakeGroup,
        };
    }

    private static byte[] NextBytes(RandomNumberGenerator rng, int n)
    {
        var b = new byte[n];
        rng.GetBytes(b);
        return b;
    }
}
