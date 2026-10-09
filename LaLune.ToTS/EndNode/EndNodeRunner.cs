using LaLune.ToTS.Cli;
using LaLune.ToTS.Common;
using LaLune.ToTS.Crypto;

namespace LaLune.ToTS.EndNode;

/// <summary>
/// Точка входа end_node.
///
/// Пока реализовано:
///   1. Разбор конфига tots://...
///   2. Распаковка ZIP → .db (в temp).
///   3. Инициализация ToTS-шифра.
///   4. Проверка: зашифровать/расшифровать тестовую строку.
///
/// TODO (следующие этапы):
///   - SOCKS5 UDP ASSOCIATE сервер.
///   - MAX-сессия (Playwright, headless или стрим-логин).
///   - Звонки в группах, получение TURN-кредов.
///   - Handshake-сообщение в MAX-группу.
///   - Мост socks5 ↔ TURN.
/// </summary>
public static class EndNodeRunner
{
    public static async Task RunAsync(EndNodeOptions opts)
    {
        Log.Info("═══ LaLune.ToTS :: end_node ═══");
        Log.Info($"  session-path : {opts.SessionPath}");
        Log.Info($"  listen       : {opts.ListenAddress}");
        Log.Info($"  debug        : {opts.Debug}");
        Log.Info("");

        // 1. Разбор конфига.
        Log.Info("→ Разбираю конфиг ...");
        var cfg = TotsConfig.Parse(opts.Config);
        Log.Info($"  handshake_group : {cfg.HandshakeGroup}");
        Log.Info($"  password        : {new string('*', cfg.Password.Length)}");
        Log.Info($"  key zip size    : {cfg.KeyZip.Length} bytes");
        Log.Info("");

        // 2. Распаковка ZIP → .db во временную папку.
        Log.Info("→ Распаковываю ключ ...");
        byte[] dbBytes;
        try
        {
            dbBytes = ZipWithPassword.UnpackSingle(cfg.KeyZip, cfg.Password);
        }
        catch (Exception ex)
        {
            Log.Error($"Не удалось распаковать ZIP (неверный пароль?): {ex.Message}");
            return;
        }
        Log.Info($"  .db размер : {dbBytes.Length} bytes");

        var tmpDir = Path.Combine(Path.GetTempPath(), "tots_end_" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(tmpDir);
        var dbPath = Path.Combine(tmpDir, TotsKeyDb.DbFileName);
        await File.WriteAllBytesAsync(dbPath, dbBytes);
        Log.Info($"  .db путь   : {dbPath}");
        Log.Info("");

        // 3. Чтение .db → ToTS-шифр.
        Log.Info("→ Инициализирую ToTS-шифр ...");
        var (every, symbols) = TotsKeyDb.Read(dbPath);
        var cipher = new TotsCipher(every, symbols);
        Log.Info($"  every_symbol : {cipher.EverySymbol}");
        Log.Info($"  symbols      : {symbols.Count}");
        Log.Info("");

        // 4. Тест шифра.
        Log.Info("→ Тест шифра ...");
        var testJson = "{\"turn\":[\"turn:1.2.3.4:19302?transport=udp\"],\"uuid\":\"550e8400-e29b-41d4-a716-446655440000\"}";
        var enc = cipher.EncryptString(testJson);
        var dec = cipher.DecryptString(enc);

        Log.Info($"  исходная длина : {testJson.Length}");
        Log.Info($"  шифр длина     : {enc.Length}");
        Log.Info($"  round-trip OK  : {(dec == testJson ? "✓" : "✗")}");

        if (dec != testJson)
        {
            Log.Error($"round-trip не совпал!");
            Log.Error($"  orig: {testJson}");
            Log.Error($"  dec : {dec}");
            return;
        }

        Log.Info("");
        Log.Info("TODO: SOCKS5 UDP ASSOCIATE, MAX-сессия, звонки, handshake, мост.");
        Log.Info("Нажми Enter для выхода (в будущем здесь будет основной цикл).");
        Console.ReadLine();

        // Чистим temp.
        try { Directory.Delete(tmpDir, recursive: true); } catch { }

        await Task.CompletedTask;
    }
}
