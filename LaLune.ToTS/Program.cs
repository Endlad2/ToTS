using System.CommandLine;
using LaLune.ToTS.Cli;
using LaLune.ToTS.Common;
using LaLune.ToTS.EndNode;
using LaLune.ToTS.ExitNode;

// ─────────────────────────────────────────────────────────────────────────────
//  LaLune.ToTS — Tunnel over TURN / Socks
//
//  Режимы:
//    --end_node   Клиент VPN. Поднимает SOCKS5 UDP ASSOCIATE,
//                 инициирует handshake через MAX-сообщения,
//                 гоняет трафик через TURN.
//
//    --exit_node  Сервер VPN. Слушает handshake-группу MAX,
//                 подключается к TURN'ам клиента,
//                 пропускает трафик в интернет.
//
//  Дополнительные:
//    --config        tots://... (обязательно для end_node)
//    --password      пароль для генерации конфига (обязательно для exit_node)
//    --session-path  папка persistent-профиля Playwright
//    --listen        адрес SOCKS5 (по умолчанию 127.0.0.1:1080)
//    --debug         подробные логи
// ─────────────────────────────────────────────────────────────────────────────

var endNodeOpt = new Option<bool>("--end_node", "Запустить как end_node (клиент VPN).");
var exitNodeOpt = new Option<bool>("--exit_node", "Запустить как exit_node (сервер VPN).");

var configOpt = new Option<string?>("--config", "Конфиг tots://... для end_node.");
var passwordOpt = new Option<string?>("--password", "Пароль для генерации конфига на exit_node.");
var sessionPathOpt = new Option<string?>("--session-path", "Папка persistent-профиля Playwright.");
var listenOpt = new Option<string>("--listen", () => "127.0.0.1:1080", "Адрес SOCKS5 для end_node.");
var debugOpt = new Option<bool>("--debug", "Подробные логи.");

var root = new RootCommand("LaLune.ToTS — tunnel over TURN via MAX messages")
{
    endNodeOpt, exitNodeOpt, configOpt, passwordOpt, sessionPathOpt, listenOpt, debugOpt,
};

root.SetHandler(async (ctx) =>
{
    var pr = ctx.ParseResult;
    var isEnd = pr.GetValueForOption(endNodeOpt);
    var isExit = pr.GetValueForOption(exitNodeOpt);
    var cfg = pr.GetValueForOption(configOpt);
    var pwd = pr.GetValueForOption(passwordOpt);
    var sp = pr.GetValueForOption(sessionPathOpt);
    var listen = pr.GetValueForOption(listenOpt);
    var debug = pr.GetValueForOption(debugOpt);

    if (!isEnd && !isExit)
    {
        Console.Error.WriteLine("Укажи --end_node или --exit_node.");
        Environment.Exit(1);
    }
    if (isEnd && isExit)
    {
        Console.Error.WriteLine("Нельзя одновременно --end_node и --exit_node.");
        Environment.Exit(1);
    }

    if (sp is null)
    {
        sp = Path.Combine(AppContext.BaseDirectory, "session");
    }
    Directory.CreateDirectory(sp);

    Log.Enabled = debug;

    try
    {
        if (isExit)
        {
            if (string.IsNullOrEmpty(pwd))
            {
                Console.Error.WriteLine("Для --exit_node обязателен --password.");
                Environment.Exit(1);
            }
            await ExitNodeRunner.RunAsync(new ExitNodeOptions
            {
                Password = pwd,
                SessionPath = sp,
                Debug = debug,
            });
        }
        else
        {
            if (string.IsNullOrEmpty(cfg))
            {
                Console.Error.WriteLine("Для --end_node обязателен --config.");
                Environment.Exit(1);
            }
            await EndNodeRunner.RunAsync(new EndNodeOptions
            {
                Config = cfg,
                SessionPath = sp,
                ListenAddress = listen,
                Debug = debug,
            });
        }
    }
    catch (Exception ex)
    {
        Console.Error.WriteLine($"Fatal: {ex}");
        Environment.Exit(1);
    }
});

return await root.InvokeAsync(args);
