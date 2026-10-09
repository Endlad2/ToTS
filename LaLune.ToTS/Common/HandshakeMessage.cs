using System.Text.Json;
using System.Text.Json.Serialization;

namespace LaLune.ToTS.Common;

/// <summary>
/// Сообщение handshake, передаваемое через MAX-группу.
/// Весь JSON шифруется ToTS-шифром и уходит как ЕДИНСТВЕННАЯ строка.
/// </summary>
public sealed class HandshakeMessage
{
    /// <summary>От end_node: список TURN-конфигов (по одному на звонок).</summary>
    [JsonPropertyName("turn")]
    public List<string>? Turn { get; set; }

    /// <summary>UUID транзакции. Генерируется end_node'ом.</summary>
    [JsonPropertyName("uuid")]
    public string? Uuid { get; set; }

    /// <summary>От exit_node: статус. "Start" — готов, начинай гнать трафик.</summary>
    [JsonPropertyName("status")]
    public string? Status { get; set; }

    private static readonly JsonSerializerOptions Opts = new()
    {
        DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull,
    };

    public string ToJson() => JsonSerializer.Serialize(this, Opts);

    public static HandshakeMessage? FromJson(string json)
    {
        try { return JsonSerializer.Deserialize<HandshakeMessage>(json, Opts); }
        catch { return null; }
    }
}
