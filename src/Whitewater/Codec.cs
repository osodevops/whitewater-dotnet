using System.Text;
using System.Text.Json;

namespace Whitewater;

/// <summary>
/// A bidirectional object ↔ wire-bytes serialization — the wire always
/// carries <see cref="byte"/>[]; a Codec keeps that plumbing inside the
/// SDK so application code works with its own types:
///
/// <code>
/// var orders = client.Feed&lt;string, Order&gt;("orders.events",
///     Codecs.Utf8, Codecs.Json&lt;Order&gt;());
/// var writer = await orders.WriterAsync("ingest");
/// await writer.AppendAsync("order-1", new Order("order-1", 42));
/// </code>
///
/// <see cref="Codecs.Utf8"/> for text keys/payloads,
/// <see cref="Codecs.Bytes"/> for raw binary, <see cref="Codecs.Json{T}"/>
/// for any System.Text.Json-mapped type, and
/// <see cref="Codecs.Custom{T}"/> for anything else (protobuf, msgpack,
/// application framing).
/// </summary>
public interface ICodec<T>
{
    /// <summary>Serialize one value to wire bytes.</summary>
    byte[] Encode(T value);
    /// <summary>Deserialize wire bytes to a value.</summary>
    T Decode(byte[] data);
}

/// <summary>Built-in codecs — see <see cref="ICodec{T}"/>.</summary>
public static class Codecs
{
    private static readonly JsonSerializerOptions WebOptions =
        new(JsonSerializerDefaults.Web);

    /// <summary>UTF-8 text — the common case for keys and
    /// JSON/XML/text payloads.</summary>
    public static ICodec<string> Utf8 { get; } = new Utf8Codec();

    /// <summary>Identity codec — raw bytes through to the wire.</summary>
    public static ICodec<byte[]> Bytes { get; } = new BytesCodec();

    /// <summary>System.Text.Json-mapped type — objects round-trip
    /// through <see cref="JsonSerializerDefaults.Web"/> (camelCase
    /// wire names, case-insensitive reads), or caller-supplied
    /// options.</summary>
    public static ICodec<T> Json<T>(JsonSerializerOptions? options = null)
        => new JsonCodec<T>(options ?? WebOptions);

    /// <summary>A caller-supplied pair — protobuf, msgpack,
    /// application framing:
    /// <code>Codecs.Custom&lt;User&gt;(u => u.ToByteArray(), User.Parser.ParseFrom)</code>
    /// </summary>
    public static ICodec<T> Custom<T>(Func<T, byte[]> encode, Func<byte[], T> decode)
        => new CustomCodec<T>(encode, decode);

    /// <summary>A codec chosen by type: <see cref="string"/> is UTF-8,
    /// <see cref="byte"/>[] is raw identity, everything else is
    /// System.Text.Json-mapped — so <c>client.Feed&lt;string, Order&gt;</c>
    /// or <c>StoreFor</c> just works without spelling Json&lt;Order&gt;().</summary>
    public static ICodec<T> For<T>()
    {
        var type = typeof(T);
        if (type == typeof(string)) return (ICodec<T>)Utf8;
        if (type == typeof(byte[])) return (ICodec<T>)Bytes;
        return Json<T>();
    }

    /// <summary>Encode a text Metadata map to wire bytes.</summary>
    public static IReadOnlyDictionary<string, byte[]> TextMetadata(
        IReadOnlyDictionary<string, string>? metadata)
    {
        if (metadata is null) return new Dictionary<string, byte[]>();
        var encoded = new Dictionary<string, byte[]>(metadata.Count);
        foreach (var (name, value) in metadata)
            encoded[name] = Encoding.UTF8.GetBytes(value);
        return encoded;
    }

    private sealed class Utf8Codec : ICodec<string>
    {
        public byte[] Encode(string value) => Encoding.UTF8.GetBytes(value);
        public string Decode(byte[] data) => Encoding.UTF8.GetString(data);
    }

    private sealed class BytesCodec : ICodec<byte[]>
    {
        public byte[] Encode(byte[] value) => value;
        public byte[] Decode(byte[] data) => data;
    }

    private sealed class JsonCodec<T>(JsonSerializerOptions? options) : ICodec<T>
    {
        public byte[] Encode(T value) => JsonSerializer.SerializeToUtf8Bytes(value, options);
        public T Decode(byte[] data) => JsonSerializer.Deserialize<T>(data, options)!;
    }

    private sealed class CustomCodec<T>(
        Func<T, byte[]> encode, Func<byte[], T> decode) : ICodec<T>
    {
        public byte[] Encode(T value) => encode(value);
        public T Decode(byte[] data) => decode(data);
    }
}
