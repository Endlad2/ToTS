using LaLune.ToTS.Cli;
using LaLune.ToTS.Common;

namespace LaLune.ToTS.ExitNode;

/// <summary>
/// Точка входа exit_node.
///
/// Пока реализовано:
///   1. Генерация ключа/конфига.
///   2. Печать конфига в консоль.
///
/// TODO (следующие этапы):
///   - MAX-сессия (Playwright headless, стрим-логин).
///   - Чтение handshake-группы (push'ы от MAX).
///   - Расшифровка сообщений.
///   - TURN-клиент: подключение к чужим TURN'ам end_node'а.
///   - Пропуск UDP-трафика в интернет.
///   - Отправка "Start" обратно.
/// </summary>
public static class ExitNodeRunner
{
    public static async Task RunAsync(ExitNodeOptions opts)
    {
        Log.Info("═══ LaLune.ToTS :: exit_node ═══");
        Log.Info($"  session-path : {opts.SessionPath}");
        Log.Info($"  debug        : {opts.Debug}");
        Log.Info("");

        // Шаг 1: спросить у пользователя ID handshake-группы.
        // Здесь мы предполагаем, что группа уже создана и пользователь знает её ID.
        Console.Write("Введи ID handshake-группы (например, -79867987848556): ");
        var groupRaw = Console.ReadLine()?.Trim();
        if (string.IsNullOrEmpty(groupRaw) || !long.TryParse(groupRaw, out var group))
        {
            Log.Error("Некорректный ID группы.");
            return;
        }

        // Шаг 2: сгенерировать конфиг.
        Log.Info("→ Генерирую ToTS-ключ (every_symbol + Symbols) ...");
        var result = ExitNodeSetup.Generate(group, opts.Password);

        Log.Info("");
        Log.Info("═══════════════════════════════════════════════════════════");
        Log.Info(" Конфиг для end_node (скопируй целиком одной строкой):");
        Log.Info("═══════════════════════════════════════════════════════════");
        Console.WriteLine(result.Config);
        Log.Info("═══════════════════════════════════════════════════════════");
        Log.Info("");
        Log.Info($"  handshake_group : {result.HandshakeGroup}");
        Log.Info($"  every_symbol    : {result.Cipher.EverySymbol}");
        Log.Info("");

        // Шаг 3: ждём (в следующих этапах — MAX-сессия, чтение handshake, TURN).
        Log.Info("TODO: MAX-сессия, чтение handshake-группы, TURN-приём, поток.");
        Log.Info("Нажми Enter для выхода (в будущем здесь будет основной цикл).");
        Console.ReadLine();

        await Task.CompletedTask;
    }
}
