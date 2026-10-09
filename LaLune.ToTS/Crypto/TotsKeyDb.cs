using Microsoft.Data.Sqlite;
using System.Text;

namespace LaLune.ToTS.Crypto;

/// <summary>
/// Работа с .db-ключом (SQLite):
///   Таблица every_symbol: одна строка, одно значение (int 3..10).
///   Таблица Symbols: (symbol TEXT, value TEXT) — 64 строки.
///     symbol — один символ из алфавита base64.
///     value  — строка длины every_symbol.
/// </summary>
public static class TotsKeyDb
{
    public const string DbFileName = "tots_key.db";

    /// <summary>
    /// Создаёт новый ключ: генерирует everySymbol и все 64 записи Symbols.
    /// </summary>
    public static (int everySymbol, Dictionary<char, string> symbols) GenerateRandom(
        Random rng)
    {
        int every = rng.Next(3, 11); // 3..10 включительно
        var symbols = new Dictionary<char, string>(64);
        var used = new HashSet<string>(StringComparer.Ordinal);

        const string chars = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";

        foreach (var c in TotsCipher.Base64Alphabet)
        {
            string val;
            int guard = 0;
            do
            {
                var arr = new char[every];
                for (int i = 0; i < every; i++)
                    arr[i] = chars[rng.Next(chars.Length)];
                val = new string(arr);
                guard++;
                if (guard > 10000)
                    throw new InvalidOperationException("Не удалось сгенерировать уникальное значение");
            } while (used.Contains(val));

            used.Add(val);
            symbols[c] = val;
        }

        return (every, symbols);
    }

    /// <summary>
    /// Сохраняет ключ в SQLite-файл по указанному пути.
    /// </summary>
    public static void Write(string dbPath, int everySymbol,
        IReadOnlyDictionary<char, string> symbols)
    {
        if (File.Exists(dbPath)) File.Delete(dbPath);

        using var conn = new SqliteConnection($"Data Source={dbPath}");
        conn.Open();

        using (var cmd = conn.CreateCommand())
        {
            cmd.CommandText = @"
                CREATE TABLE every_symbol (value INTEGER NOT NULL);
                CREATE TABLE Symbols (
                    symbol TEXT NOT NULL PRIMARY KEY,
                    value  TEXT NOT NULL
                );
                CREATE INDEX idx_symbols_value ON Symbols(value);
            ";
            cmd.ExecuteNonQuery();
        }

        using (var tx = conn.BeginTransaction())
        {
            using (var cmd = conn.CreateCommand())
            {
                cmd.Transaction = tx;
                cmd.CommandText = "INSERT INTO every_symbol(value) VALUES (@v)";
                cmd.Parameters.AddWithValue("@v", everySymbol);
                cmd.ExecuteNonQuery();
            }

            foreach (var kv in symbols)
            {
                using var cmd = conn.CreateCommand();
                cmd.Transaction = tx;
                cmd.CommandText =
                    "INSERT INTO Symbols(symbol, value) VALUES (@s, @v)";
                cmd.Parameters.AddWithValue("@s", kv.Key.ToString());
                cmd.Parameters.AddWithValue("@v", kv.Value);
                cmd.ExecuteNonQuery();
            }

            tx.Commit();
        }
    }

    /// <summary>
    /// Читает ключ из SQLite-файла.
    /// </summary>
    public static (int everySymbol, Dictionary<char, string> symbols) Read(string dbPath)
    {
        if (!File.Exists(dbPath))
            throw new FileNotFoundException("Ключевой .db не найден", dbPath);

        using var conn = new SqliteConnection($"Data Source={dbPath}");
        conn.Open();

        int every;
        using (var cmd = conn.CreateCommand())
        {
            cmd.CommandText = "SELECT value FROM every_symbol LIMIT 1";
            var res = cmd.ExecuteScalar();
            if (res is null or DBNull)
                throw new InvalidDataException("Таблица every_symbol пуста");
            every = Convert.ToInt32(res);
        }

        var symbols = new Dictionary<char, string>(64);
        using (var cmd = conn.CreateCommand())
        {
            cmd.CommandText = "SELECT symbol, value FROM Symbols";
            using var r = cmd.ExecuteReader();
            while (r.Read())
            {
                var s = r.GetString(0);
                var v = r.GetString(1);
                if (s.Length != 1)
                    throw new InvalidDataException($"symbol должен быть одним символом, получено: '{s}'");
                symbols[s[0]] = v;
            }
        }

        if (symbols.Count != TotsCipher.Base64Alphabet.Length)
            throw new InvalidDataException(
                $"Ожидалось {TotsCipher.Base64Alphabet.Length} символов, получено {symbols.Count}");

        return (every, symbols);
    }
}
