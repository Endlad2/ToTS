namespace LaLune.ToTS.Cli;

public sealed class EndNodeOptions
{
    public required string Config { get; init; }
    public required string SessionPath { get; init; }
    public required string ListenAddress { get; init; }
    public bool Debug { get; init; }
}

public sealed class ExitNodeOptions
{
    public required string Password { get; init; }
    public required string SessionPath { get; init; }
    public bool Debug { get; init; }
}
