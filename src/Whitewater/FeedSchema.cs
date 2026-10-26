using System.Text.Json.Nodes;

namespace Whitewater;

/// <summary>
/// One side of a Feed's schema metadata — format plus optional
/// subject/definition text. Formats: <c>json</c>, <c>jsonschema</c>,
/// <c>avro</c>, <c>protobuf</c>, <c>text</c>, <c>bytes</c>, or
/// <c>custom.&lt;name&gt;</c>. Records
/// stay schema-neutral bytes on the wire; declarations are catalog
/// metadata for tooling and compatibility — not yet an enforced
/// registry.
/// </summary>
public sealed record SchemaDecl(string Format, string? Subject = null, string? Definition = null)
{
    internal JsonObject ToWire()
    {
        var node = new JsonObject { ["format"] = Format };
        if (Subject is not null) node["subject"] = Subject;
        if (Definition is not null) node["definition"] = Definition;
        return node;
    }

    internal static SchemaDecl? From(JsonNode? node)
        => node is JsonObject row && row["format"] is { } format
            ? new SchemaDecl(
                format.GetValue<string>(),
                row["subject"]?.GetValue<string>(),
                row["definition"]?.GetValue<string>())
            : null;
}

/// <summary>
/// The declared key/value schema pair — either side may be null. A
/// Feed's declaration overrides the named sides of its Domain's
/// default.
/// </summary>
public sealed record FeedSchema(SchemaDecl? Key = null, SchemaDecl? Value = null)
{
    internal JsonObject ToWire()
    {
        var node = new JsonObject();
        if (Key is not null) node["key"] = Key.ToWire();
        if (Value is not null) node["value"] = Value.ToWire();
        return node;
    }

    internal static FeedSchema? From(JsonNode? node)
    {
        if (node is not JsonObject row)
        {
            return null;
        }
        var key = SchemaDecl.From(row["key"]);
        var value = SchemaDecl.From(row["value"]);
        return key is null && value is null ? null : new FeedSchema(key, value);
    }
}
