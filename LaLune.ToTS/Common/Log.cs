namespace LaLune.ToTS.Common;

public static class Log
{
    public static bool Enabled { get; set; }
    private static readonly object Lock = new();

    public static void Info(string msg)
    {
        lock (Lock)
            Console.WriteLine($"[{Ts()}] {msg}");
    }

    public static void Debug(string msg)
    {
        if (!Enabled) return;
        lock (Lock)
            Console.WriteLine($"[{Ts()}] [debug] {msg}");
    }

    public static void Warn(string msg)
    {
        lock (Lock)
            Console.WriteLine($"[{Ts()}] [warn] {msg}");
    }

    public static void Error(string msg)
    {
        lock (Lock)
            Console.Error.WriteLine($"[{Ts()}] [error] {msg}");
    }

    private static string Ts() => DateTime.UtcNow.ToString("HH:mm:ss.fff");
}
