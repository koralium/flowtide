// Licensed under the Apache License, Version 2.0.
using System.Text.Json;
using System.Text.Json.Nodes;
using Json.Schema;

namespace FlowtideDotNet.Substrait.Dialect.Tests;

internal sealed partial class DialectCatalog
{
    public CatalogDocument Dialect { get; }
    public IReadOnlyList<CatalogDocument> LocalExtensions { get; }
    public IReadOnlyList<CatalogDocument> UpstreamExtensions { get; }
    private readonly Dictionary<string, CatalogDocument> _sources;
    private readonly Dictionary<string, JsonSchema> _schemas;

    private DialectCatalog(CatalogDocument dialect, IReadOnlyList<CatalogDocument> local,
        IReadOnlyList<CatalogDocument> upstream, Dictionary<string, JsonSchema> schemas)
    {
        Dialect = dialect;
        LocalExtensions = local;
        UpstreamExtensions = upstream;
        _schemas = schemas;
        // The two historical local io.substrait copies are validated, but references to
        // upstream URNs always resolve against the pinned upstream definitions.
        _sources = upstream.Concat(local.Where(doc => !doc.Urn.StartsWith("extension:io.substrait:", StringComparison.Ordinal)))
            .ToDictionary(doc => doc.Urn, StringComparer.Ordinal);
    }

    public static DialectCatalog Load(string root)
    {
        CatalogDocument Read(string path) => CatalogDocument.Load(System.IO.Path.Combine(root, path), root);
        var local = Directory.GetFiles(System.IO.Path.Combine(root, "substrait", "extensions"), "*.yaml")
            .Order().Select(path => CatalogDocument.Load(path, root)).ToArray();
        var upstream = Directory.GetFiles(System.IO.Path.Combine(root, "Upstream", "Substrait", "extensions"), "*.yaml")
            .Order().Select(path => CatalogDocument.Load(path, root)).ToArray();
        var schemas = new Dictionary<string, JsonSchema>();
        foreach (var name in new[] { "dialect", "simple_extensions" })
        {
            var document = Read($"Upstream/Substrait/text/{name}_schema.yaml");
            var registry = new SchemaRegistry { Fetch = (uri, _) => throw new InvalidOperationException($"Unexpected external schema reference: {uri}") };
            schemas[name] = JsonSchema.Build(JsonSerializer.SerializeToElement(document.Data),
                new BuildOptions { Dialect = Json.Schema.Dialect.Draft202012, SchemaRegistry = registry });
        }
        return new(Read("substrait/dialect.yaml"), local, upstream, schemas);
    }

    public DialectCatalog WithDialect(JsonObject data) => new(new(Dialect.Path, data), LocalExtensions, UpstreamExtensions, _schemas);

    public DialectCatalog WithExtension(CatalogDocument document) => new(Dialect,
        LocalExtensions.Select(item => item.Path == document.Path ? document : item).ToArray(), UpstreamExtensions, _schemas);

    public IEnumerable<ValidationIssue> ValidateSchema(CatalogDocument document, bool dialect)
    {
        var result = _schemas[dialect ? "dialect" : "simple_extensions"].Evaluate(
            JsonSerializer.SerializeToElement(document.Data), new EvaluationOptions { OutputFormat = OutputFormat.Hierarchical });
        return SchemaErrors(result).Select(error => new ValidationIssue(document.Path, error.Pointer, error.Message));
    }

    private static IEnumerable<(string Pointer, string Message)> SchemaErrors(EvaluationResults result)
    {
        if (result.IsValid) yield break;
        if (result.Errors != null)
            foreach (var error in result.Errors) yield return (result.InstanceLocation.ToString(), $"{error.Key}: {error.Value}");
        if (result.Details != null)
            foreach (var child in result.Details)
                foreach (var error in SchemaErrors(child)) yield return error;
    }

    public IEnumerable<FunctionEntry> Functions()
    {
        foreach (var category in new[] { "scalar", "aggregate", "window" })
        {
            var section = $"supported_{category}_functions";
            var index = 0;
            foreach (var entry in Dialect.Data[section].Objects())
                yield return new(category, $"/{section}/{index++}", entry);
        }
        var tableIndex = 0;
        foreach (var entry in (Dialect.Data["metadata"]?["flowtide_table_functions"]?["supported_functions"]).Objects())
            yield return new("table", $"/metadata/flowtide_table_functions/supported_functions/{tableIndex++}", entry);
    }

    private CatalogDocument? Resolve(CatalogDocument owner, string alias)
    {
        var urn = owner.Data["dependencies"] is JsonObject dependencies ? dependencies[alias].Text() : null;
        return urn != null && _sources.TryGetValue(urn, out var source) ? source : null;
    }

    private static IEnumerable<JsonObject> Definitions(CatalogDocument document, string category) => category == "table"
        ? (document.Data["metadata"]?["table_functions"]).Objects()
        : document.Data[$"{category}_functions"].Objects();

    private static string Signature(JsonObject implementation) => string.Join("_", implementation["args"].Objects().Select(arg =>
        arg.ContainsKey("options") ? "req" : arg["value"] is JsonObject || arg["type"] is JsonObject ? "struct"
        : TypeExpression.Parse((arg["value"] ?? arg["type"]).Text() ?? throw new InvalidDataException("Function argument has no type.")).ShortName));

    private static bool SupportsOptions(JsonObject impl, JsonObject? options) => options == null || options.All(option =>
        (impl["options"]?[option.Key]?["values"]).Strings().Contains(option.Value.Text() ?? "", StringComparer.Ordinal));

    public IEnumerable<ValidationIssue> ValidateReferences()
    {
        foreach (var owner in LocalExtensions.Prepend(Dialect))
        {
            if (owner.Data["dependencies"] is not JsonObject dependencies) continue;
            foreach (var dependency in dependencies)
                if (Resolve(owner, dependency.Key) == null)
                    yield return new(owner.Path, $"/dependencies/{JsonCatalog.Pointer(dependency.Key)}", $"Unresolved extension URN '{dependency.Value.Text()}'. Pin upstream definitions or add the local extension.");
        }
        foreach (var entry in Functions())
        {
            var alias = entry.Data["source"].Text() ?? "";
            var name = entry.Data["name"].Text() ?? "";
            var source = Resolve(Dialect, alias);
            if (source == null)
            {
                yield return new(Dialect.Path, entry.Pointer + "/source", $"Unresolved source '{alias}'.");
                continue;
            }
            var category = entry.Category;
            if (category == "window")
            {
                var declaredKind = entry.Data["metadata"]?["definition_kind"].Text();
                if (declaredKind == "AGGREGATE_FUNCTION") category = "aggregate";
                else if (declaredKind == null && !Definitions(source, "window").Any(function => function["name"].Text() == name)) category = "aggregate";
                else if (declaredKind is not null and not "WINDOW_FUNCTION")
                {
                    yield return new(Dialect.Path, entry.Pointer, $"Invalid window definition_kind '{declaredKind}'.");
                    continue;
                }
            }
            var implementations = Definitions(source, category).Where(function => function["name"].Text() == name)
                .SelectMany(function => function["impls"].Objects()).ToArray();
            if (implementations.Length == 0)
            {
                yield return new(Dialect.Path, entry.Pointer + "/name", $"No {category} definition '{name}' in {source.Path}.");
                continue;
            }
            foreach (var signature in entry.Data["supported_impls"].Strings())
            {
                var matching = implementations.Where(impl => Signature(impl) == signature).ToArray();
                if (matching.Length == 0)
                    yield return new(Dialect.Path, entry.Pointer + "/supported_impls", $"Unknown signature '{name}:{signature}' in {source.Path}.");
                else if (!matching.Any(impl => SupportsOptions(impl, entry.Data["required_options"] as JsonObject)))
                    yield return new(Dialect.Path, entry.Pointer + "/required_options", $"Unsupported option selection for '{name}:{signature}' in {source.Path}.");
                else if (entry.Data["variadic"] is JsonObject variadic && !matching.Any(impl => SupportsVariadic(impl, variadic)))
                    yield return new(Dialect.Path, entry.Pointer + "/variadic", $"Variadic bounds exceed the definition of '{name}:{signature}'.");
            }
        }
        var typeIndex = 0;
        foreach (var type in Dialect.Data["supported_types"].Objects())
        {
            if (type["type"].Text() == "USER_DEFINED")
            {
                var source = Resolve(Dialect, type["source"].Text() ?? "");
                if (source == null || !source.Data["types"].Objects().Any(definition => definition["name"].Text() == type["name"].Text()))
                    yield return new(Dialect.Path, $"/supported_types/{typeIndex}", $"Unresolved user-defined type '{type["source"].Text()}.{type["name"].Text()}'.");
            }
            typeIndex++;
        }
    }

    private static bool SupportsVariadic(JsonObject impl, JsonObject requested)
    {
        if (impl["variadic"] is not JsonObject available) return false;
        var min = requested["min"]?.GetValue<int>() ?? available["min"]?.GetValue<int>() ?? 0;
        var max = requested["max"]?.GetValue<int>() ?? available["max"]?.GetValue<int>() ?? int.MaxValue;
        return min >= (available["min"]?.GetValue<int>() ?? 0) && max >= min && max <= (available["max"]?.GetValue<int>() ?? int.MaxValue);
    }
}
