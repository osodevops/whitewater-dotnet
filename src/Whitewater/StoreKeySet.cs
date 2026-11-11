namespace Whitewater;

/// <summary>
/// Strongly-typed or inline secondary-key postings for a
/// <see cref="StateStore"/> or <see cref="Store{TKey,TValue}"/>
/// <c>PutAsync</c>. Inherit <see cref="AbstractStoreKeySet"/> to give
/// a store's secondary keys C# properties, static <c>With(...)</c>
/// factories, fluent chaining, and <c>Of(k =&gt; ...)</c> builders.
/// </summary>
public interface IStoreKeySet
{
    /// <summary>Named-key dictionary emitted into the mutation request.</summary>
    IReadOnlyDictionary<string, object?> ToDictionary();
}

/// <summary>
/// Base class for typed <see cref="StateStore"/> secondary-key sets:
/// <code>
/// public sealed class PassportKeySet : AbstractStoreKeySet
/// {
///     public string? Passport   { get =&gt; Get&lt;string&gt;("passport");   set =&gt; Put("passport", value); }
///     public string? Department { get =&gt; Get&lt;string&gt;("department"); set =&gt; Put("department", value); }
///
///     public PassportKeySet WithPassport(string v)   =&gt; Put&lt;PassportKeySet&gt;("passport", v);
///     public PassportKeySet WithDepartment(string v) =&gt; Put&lt;PassportKeySet&gt;("department", v);
///
///     public static PassportKeySet With(string passport, string department)
///         =&gt; new PassportKeySet().WithPassport(passport).WithDepartment(department);
///
///     public static PassportKeySet Of(Action&lt;PassportKeySet&gt; builder)
///     {
///         var ks = new PassportKeySet();
///         builder(ks);
///         return ks;
///     }
/// }
/// </code>
/// </summary>
public abstract class AbstractStoreKeySet : IStoreKeySet
{
    private readonly Dictionary<string, object?> _keys = new(StringComparer.Ordinal);

    /// <summary>Record a secondary-key value and return <c>this</c> cast to <typeparamref name="TSelf"/>.</summary>
    public TSelf Put<TSelf>(string name, object? value) where TSelf : AbstractStoreKeySet
    {
        Put(name, value);
        return (TSelf)this;
    }

    /// <summary>Record a secondary-key value and return <c>this</c> for chaining.</summary>
    public AbstractStoreKeySet Put(string name, object? value)
    {
        if (string.IsNullOrWhiteSpace(name))
        {
            throw new ArgumentException("secondary key name must not be blank", nameof(name));
        }
        _keys[name] = value;
        return this;
    }

    /// <summary>Read back a previously set secondary-key value, or null.</summary>
    public object? Get(string name)
        => _keys.TryGetValue(name, out var value) ? value : null;

    /// <summary>Read back a typed secondary-key value, or default.</summary>
    protected T? Get<T>(string name)
        => _keys.TryGetValue(name, out var value) && value is T typed ? typed : default;

    /// <summary>Indexer access to secondary keys by name.</summary>
    public object? this[string name]
    {
        get => Get(name);
        set => Put(name, value);
    }

    /// <summary>True when no secondary keys have been populated.</summary>
    public bool IsEmpty => _keys.Count == 0;

    /// <inheritdoc />
    public IReadOnlyDictionary<string, object?> ToDictionary() => _keys;
}

/// <summary>General-purpose <see cref="AbstractStoreKeySet"/> and static factory helpers.</summary>
public class StoreKeySet : AbstractStoreKeySet
{
    /// <summary>Single named secondary key.</summary>
    public static StoreKeySet Of(string key, object? value)
    {
        var ks = new StoreKeySet();
        ks.Put(key, value);
        return ks;
    }

    /// <summary>Two named secondary keys.</summary>
    public static StoreKeySet Of(string k1, object? v1, string k2, object? v2)
    {
        var ks = new StoreKeySet();
        ks.Put(k1, v1).Put(k2, v2);
        return ks;
    }

    /// <summary>Three named secondary keys.</summary>
    public static StoreKeySet Of(string k1, object? v1, string k2, object? v2, string k3, object? v3)
    {
        var ks = new StoreKeySet();
        ks.Put(k1, v1).Put(k2, v2).Put(k3, v3);
        return ks;
    }

    /// <summary>Build an ad-hoc key set with an <see cref="Action{T}"/> callback.</summary>
    public static StoreKeySet Of(Action<StoreKeySet> builder)
    {
        ArgumentNullException.ThrowIfNull(builder);
        var ks = new StoreKeySet();
        builder(ks);
        return ks;
    }

    /// <summary>Build a typed <typeparamref name="TKeySet"/> with an <see cref="Action{T}"/> callback.</summary>
    public static TKeySet Of<TKeySet>(Action<TKeySet> builder) where TKeySet : IStoreKeySet, new()
    {
        ArgumentNullException.ThrowIfNull(builder);
        var ks = new TKeySet();
        builder(ks);
        return ks;
    }
}
