// Licensed under the Apache License, Version 2.0.
using System.Text.Json.Nodes;

namespace FlowtideDotNet.Substrait.Dialect.Tests;

public class CatalogValidationTests
{
    private static readonly Lazy<DialectCatalog> Loaded = new(() => DialectCatalog.Load(AppContext.BaseDirectory));
    internal static DialectCatalog Catalog => Loaded.Value;
    internal static void Valid(IEnumerable<ValidationIssue> issues)
    {
        var errors = issues.ToArray();
        Assert.True(errors.Length == 0, string.Join(Environment.NewLine, errors.Select(error => error.ToString())));
    }

    [Fact]
    public void Dialect_and_extensions_match_pinned_upstream_schemas()
    {
        Valid(Catalog.ValidateSchema(Catalog.Dialect, dialect: true)
            .Concat(Catalog.LocalExtensions.Concat(Catalog.UpstreamExtensions).SelectMany(doc => Catalog.ValidateSchema(doc, dialect: false))));
    }

    [Fact]
    public void Sources_names_signatures_options_and_variadic_bounds_resolve() => Valid(Catalog.ValidateReferences());

    [Fact]
    public void Extension_type_expressions_follow_the_official_grammar_and_resolve_user_types() => Valid(Catalog.ValidateTypes());

    [Fact]
    public void Custom_table_function_metadata_is_consistent() => Valid(Catalog.ValidateTableMetadata());

    [Theory]
    [InlineData("source", "missing_source", "/source")]
    [InlineData("name", "missing_function", "/name")]
    [InlineData("supported_impls", "missing_signature", "/supported_impls")]
    [InlineData("required_options", "missing_option", "/required_options")]
    public void Invalid_function_reference_reports_the_file_and_location(string key, string invalidValue, string expectedPointer)
    {
        var data = (JsonObject)Catalog.Dialect.Data.DeepClone();
        var function = data["supported_scalar_functions"]![0]!;
        function[key] = key switch
        {
            "supported_impls" => new JsonArray(invalidValue),
            "required_options" => new JsonObject { [invalidValue] = "invalid" },
            _ => JsonValue.Create(invalidValue)
        };
        Assert.Contains(Catalog.WithDialect(data).ValidateReferences(), error => error.File == "substrait/dialect.yaml"
            && error.Pointer == "/supported_scalar_functions/0" + expectedPointer);
    }

    [Fact]
    public void Unknown_option_value_is_rejected()
    {
        var data = (JsonObject)Catalog.Dialect.Data.DeepClone();
        var function = data["supported_scalar_functions"].Objects().First(entry => entry["required_options"] is JsonObject { Count: > 0 });
        var options = (JsonObject)function["required_options"]!;
        options[options.First().Key] = "invalid_value";
        Assert.Contains(Catalog.WithDialect(data).ValidateReferences(), error => error.Pointer.EndsWith("/required_options"));
    }

    [Fact]
    public void Variadic_bounds_cannot_exceed_the_extension_definition()
    {
        var data = (JsonObject)Catalog.Dialect.Data.DeepClone();
        var function = data["supported_scalar_functions"].Objects().First(entry => entry["variadic"] != null);
        function["variadic"] = new JsonObject { ["min"] = 2, ["max"] = 1 };
        Assert.Contains(Catalog.WithDialect(data).ValidateReferences(), error => error.Pointer.EndsWith("/variadic"));
    }

    [Fact]
    public void Invalid_shapes_are_rejected_by_the_schema()
    {
        var data = (JsonObject)Catalog.Dialect.Data.DeepClone();
        data["supported_expressions"]![0]!["expression"] = "UNKNOWN_EXPRESSION";
        data["supported_scalar_functions"]![0]!["supported_impls"] = new JsonArray();
        var errors = Catalog.WithDialect(data).ValidateSchema(new("substrait/dialect.yaml", data), dialect: true).ToArray();
        Assert.Contains(errors, error => error.Pointer.StartsWith("/supported_expressions/0"));
        Assert.Contains(errors, error => error.Pointer.EndsWith("/supported_impls"));
    }

    [Theory]
    [InlineData("flowtide_types.u!missing", "Unresolved user-defined type")]
    [InlineData("unknown_alias.u!dynamic", "Unresolved user-defined type")]
    [InlineData("list<any>?", "Invalid Substrait type")]
    public void Invalid_extension_types_have_actionable_diagnostics(string expression, string message)
    {
        var original = Catalog.LocalExtensions.First(doc => doc.Data["dependencies"]?["flowtide_types"] != null && doc.Data["scalar_functions"] != null);
        var data = (JsonObject)original.Data.DeepClone();
        data["scalar_functions"]![0]!["impls"]![0]!["return"] = expression;
        Assert.Contains(Catalog.WithExtension(new(original.Path, data)).ValidateTypes(), error => error.File == original.Path
            && error.Pointer == "/scalar_functions/0/impls/0/return" && error.Message.Contains(message));
    }

    [Fact]
    public void Table_metadata_checks_outputs_and_constant_arguments()
    {
        var original = Catalog.LocalExtensions.First(doc => doc.Path.EndsWith("functions_datetime.yaml"));
        var data = (JsonObject)original.Data.DeepClone();
        var function = data["metadata"]!["table_functions"]![0]!;
        function["metadata"]!["output_column_count"] = 99;
        function["metadata"]!["literal_arguments"] = new JsonArray(0); // Timestamp input is not constant.
        var errors = Catalog.WithExtension(new(original.Path, data)).ValidateTableMetadata().ToArray();
        Assert.Contains(errors, error => error.Pointer.EndsWith("/output_columns"));
        Assert.Contains(errors, error => error.Pointer.EndsWith("/literal_arguments"));
    }

    [Fact]
    public void Upstream_snapshot_is_pinned_and_all_manifest_files_are_present()
    {
        var root = Path.Combine(AppContext.BaseDirectory, "Upstream", "Substrait");
        var manifest = JsonNode.Parse(File.ReadAllText(Path.Combine(root, "upstream.json")))!;
        Assert.Equal("https://github.com/substrait-io/substrait", manifest["repository"].Text());
        Assert.Matches("^[a-f0-9]{40}$", manifest["revision"].Text()!);
        foreach (var file in manifest["files"].Strings()) Assert.True(File.Exists(Path.Combine(root, file)), $"Missing pinned upstream file: {file}");
    }
}
