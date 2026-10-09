using System.Text;

namespace LaLune.ToTS.Crypto;

/// <summary>
/// ToTS-шифр: символьная замена base64 → строки фиксированной длины.
///
/// Свойства:
///   - Алфавит шифруется: ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/=
///   - Каждому символу соответствует строка длины everySymbol (3..10).
///   - Все записи в таблице имеют ОДИНАКОВУЮ длину.
///   - Шифрование: base64 payload → проход по символам → конкатенация замен.
///   - Дешифрование: зашифрованная строка → резка по everySymbol символов →
///     обратный lookup → склейка в base64-строку → base64-decode.
///
/// Скорость:
///   - Шифрование: string[] по индексу char (ASCII &lt; 128) → одно чтение массива на символ.
///   - Дешифрование: Dictionary&lt;string, char&gt; с прямым lookup по подстроке фикс. длины.
///   - Оба проходят за один проход O(n).
/// </summary>
public sealed class TotsCipher
{
    public const string Base64Alphabet =
        "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/=";

    private const int AsciiTableSize = 128;

    private readonly int _everySymbol;

    // Прямые таблицы шифрования по индексу символа.
    // Для ASCII &lt; 128.
    private readonly string?[] _encTable = new string?[AsciiTableSize];

    // Обратная таблица дешифрования.
    private readonly Dictionary<string, char> _decTable;

    public int EverySymbol => _everySymbol;

    public TotsCipher(int everySymbol, IReadOnlyDictionary<char, string> symbols)
    {
        if (everySymbol < 3 || everySymbol > 10)
            throw new ArgumentOutOfRangeException(nameof(everySymbol),
                "everySymbol должен быть 3..10");

        if (symbols.Count != Base64Alphabet.Length)
            throw new ArgumentException(
                $"Ожидалось {Base64Alphabet.Length} символов в Symbols, получено {symbols.Count}");

        _everySymbol = everySymbol;

        // Проверяем, что все значения имеют правильную длину.
        foreach (var kv in symbols)
        {
            if (kv.Value.Length != everySymbol)
                throw new ArgumentException(
                    $"Значение для '{kv.Key}' имеет длину {kv.Value.Length}, ожидалось {everySymbol}");
        }

        // Кэшируем в массив по индексу символа.
        foreach (var c in Base64Alphabet)
        {
            if (!symbols.TryGetValue(c, out var val))
                throw new ArgumentException($"Нет записи для символа '{c}' в Symbols");

            if (c < AsciiTableSize)
                _encTable[c] = val;
        }

        // Обратная таблица.
        _decTable = new Dictionary<string, char>(StringComparer.Ordinal);
        foreach (var c in Base64Alphabet)
        {
            var val = symbols[c];
            if (_decTable.ContainsKey(val))
                throw new ArgumentException($"Дублирующееся значение в Symbols: '{val}'");
            _decTable[val] = c;
        }
    }

    /// <summary>
    /// Шифрует сырые байты:
    ///   bytes → base64 → посимвольная замена → зашифрованная строка.
    /// </summary>
    public string Encrypt(ReadOnlySpan<byte> data)
    {
        var b64 = Convert.ToBase64String(data);
        return EncryptBase64(b64);
    }

    /// <summary>
    /// Шифрует строку:
    ///   string → utf8 → base64 → посимвольная замена.
    /// </summary>
    public string EncryptString(string text)
    {
        var bytes = Encoding.UTF8.GetBytes(text);
        return Encrypt(bytes);
    }

    /// <summary>
    /// Шифрует уже готовую base64-строку (без base64-encode).
    /// </summary>
    public string EncryptBase64(string base64)
    {
        var sb = new StringBuilder(base64.Length * _everySymbol);
        foreach (var c in base64)
        {
            if (c < AsciiTableSize)
            {
                var rep = _encTable[c];
                if (rep is not null)
                {
                    sb.Append(rep);
                    continue;
                }
            }
            throw new ArgumentException($"Символ вне алфавита шифра: 0x{(int)c:X2} '{c}'");
        }
        return sb.ToString();
    }

    /// <summary>
    /// Дешифрует зашифрованную строку в сырые байты:
    ///   зашифрованная → резка по everySymbol → обратный lookup → base64 → bytes.
    /// </summary>
    public byte[] Decrypt(string encrypted)
    {
        var b64 = DecryptToBase64(encrypted);
        return Convert.FromBase64String(b64);
    }

    /// <summary>
    /// Дешифрует в UTF-8 строку.
    /// </summary>
    public string DecryptString(string encrypted)
    {
        var bytes = Decrypt(encrypted);
        return Encoding.UTF8.GetString(bytes);
    }

    /// <summary>
    /// Дешифрует в base64-строку (без base64-decode).
    /// </summary>
    public string DecryptToBase64(string encrypted)
    {
        if (encrypted.Length % _everySymbol != 0)
            throw new ArgumentException(
                $"Длина зашифрованной строки ({encrypted.Length}) не кратна everySymbol ({_everySymbol})");

        var outLen = encrypted.Length / _everySymbol;
        var chars = new char[outLen];

        for (int i = 0, pos = 0; i < outLen; i++, pos += _everySymbol)
        {
            var chunk = encrypted.Substring(pos, _everySymbol);
            if (!_decTable.TryGetValue(chunk, out var c))
                throw new ArgumentException($"Неизвестный шифр-блок: '{chunk}'");
            chars[i] = c;
        }

        return new string(chars);
    }
}
