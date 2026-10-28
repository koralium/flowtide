// Licensed under the Apache License, Version 2.0.
using System.Text.Json.Nodes;

namespace FlowtideDotNet.Substrait.Dialect.Tests;

internal sealed partial class DialectCatalog
{
    public IEnumerable<ValidationIssue> ValidateTypes()
    {
        foreach (var document in LocalExtensions.Concat(UpstreamExtensions))
        {
            foreach (var (pointer, node) in TypeLocations(document))
                foreach (var issue in ValidateType(document, pointer, node)) yield return issue;
        }
    }

    private static IEnumerable<(string Pointer, JsonNode? Node)> TypeLocations(CatalogDocument document)
    {
        foreach (var category in new[] { "scalar", "aggregate", "window", "table" })
        {
            var section = category == "table" ? "/metadata/table_functions" : $"/{category}_functions";
            var functionIndex = 0;
            foreach (var function in Definitions(document, category))
            {
                var implIndex = 0;
                foreach (var impl in function["impls"].Objects())
                {
                    var prefix = $"{section}/{functionIndex}/impls/{implIndex++}";
                    var argIndex = 0;
                    foreach (var arg in impl["args"].Objects())
                    {
                        foreach (var key in new[] { "value", "type" })
                            if (arg.ContainsKey(key)) yield return ($"{prefix}/args/{argIndex}/{key}", arg[key]);
                        argIndex++;
                    }
                    foreach (var key in new[] { "return", "intermediate" })
                        if (impl.ContainsKey(key)) yield return ($"{prefix}/{key}", impl[key]);
                    var columnIndex = 0;
                    foreach (var column in impl["output_columns"].Objects())
                    {
                        yield return ($"{prefix}/output_columns/{columnIndex}/value", column["value"]);
                        if (column["fields"] is JsonObject fields)
                            foreach (var field in fields)
                                yield return ($"{prefix}/output_columns/{columnIndex}/fields/{JsonCatalog.Pointer(field.Key)}", field.Value);
                        columnIndex++;
                    }
                }
                functionIndex++;
            }
        }
        foreach (var (section, key) in new[] { ("types", "structure"), ("type_variations", "parent") })
        {
            var index = 0;
            foreach (var type in document.Data[section].Objects())
            {
                if (type.ContainsKey(key)) yield return ($"/{section}/{index}/{key}", type[key]);
                index++;
            }
        }
    }

    private IEnumerable<ValidationIssue> ValidateType(CatalogDocument owner, string pointer, JsonNode? node)
    {
        if (node is JsonObject fields) // Substrait named-struct syntactic sugar.
        {
            foreach (var field in fields)
                foreach (var issue in ValidateType(owner, pointer + "/" + JsonCatalog.Pointer(field.Key), field.Value))
                    yield return issue;
            yield break;
        }
        ParsedType? parsed = null;
        string? error = null;
        try { parsed = TypeExpression.Parse(node.Text() ?? throw new InvalidDataException("Expected a type expression or named struct.")); }
        catch (InvalidDataException exception) { error = exception.Message; }
        if (error != null) { yield return new(owner.Path, pointer, error); yield break; }
        foreach (var reference in parsed!.References)
        {
            var source = reference.Alias == null ? owner : Resolve(owner, reference.Alias);
            if (source == null || !source.Data["types"].Objects().Any(type => type["name"].Text() == reference.Name))
                yield return new(owner.Path, pointer, $"Unresolved user-defined type '{reference.Alias}.u!{reference.Name}'.");
        }
    }
}
