namespace LaLune.ToTS.Cli;

/// <summary>
/// Разбор конфига вида:
///   tots://handshake_group=-79867987848556&amp;password=enderunity73@&amp;key=BASE64
///
/// Извлекает:
///   HandshakeGroup — ID группы MAX
///   Password       — пароль для распаковки ZIP
///   Key            — base64 ZIP-архива с .db-ключом
/// </summary>
public sealed class TotsConfig
{
    public required long HandshakeGroup { get; init; }
    public required string Password { get; init; }
    public required byte[] KeyZip { get; init; } // сырые байты zip

    public static TotsConfig Parse(string config)
    {
        if (string.IsNullOrWhiteSpace(config))
            throw new ArgumentException("Config пуст");

        var s = config.Trim();
        const string prefix = "tots://";
        if (!s.StartsWith(prefix, StringComparison.OrdinalIgnoreCase))
            throw new ArgumentException("Config должен начинаться с tots://");

        s = s.Substring(prefix.Length);

        var parts = s.Split('&', StringSplitOptions.RemoveEmptyEntries);
        var map = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
        foreach (var p in parts)
        {
            var eq = p.IndexOf('=');
            if (eq <= 0) continue;
            var k = p[..eq].Trim();
            var v = p[(eq + 1)..];
            map[k] = v;
        }

        if (!map.TryGetValue("handshake_group", out var groupRaw))
            throw new ArgumentException("В конфиге нет handshake_group");

        if (!long.TryParse(groupRaw, out var group))
            throw new ArgumentException($"handshake_group не число: {groupRaw}");

        if (!map.TryGetValue("password", out var password))
            throw new ArgumentException("В конфиге нет password");

        if (!map.TryGetValue("key", out var keyB64))
            throw new ArgumentException("В конфиге нет key");

        // key — это URL-encoded base64, decode от & и пробелов
        string keyB64Clean;
        try
        {
            keyB64Clean = Uri.UnescapeDataString(keyB64.Replace('+', ' ').Replace(' ', '+'));
        }
        catch
        {
            keyB64Clean = keyB64;
        }

        byte[] zipBytes;
        try
        {
            zipBytes = Convert.FromBase64String(keyB64Clean);
        }
        catch (Exception ex)
        {
            throw new ArgumentException("key — не валидный base64", ex);
        }

        return new TotsConfig
        {
            HandshakeGroup = group,
            Password = password,
            KeyZip = zipBytes,
        };
    }
}
