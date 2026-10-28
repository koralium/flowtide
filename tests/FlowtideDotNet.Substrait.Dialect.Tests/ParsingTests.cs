// Licensed under the Apache License, Version 2.0.
namespace FlowtideDotNet.Substrait.Dialect.Tests;

public class ParsingTests
{
    [Fact]
    public void Yaml_preserves_quoted_scalars_and_uses_JSON_compatible_plain_scalars()
    {
        var document = YamlJson.Load("""
            quoted_number: "123"
            quoted_bool: 'true'
            quoted_null: "null"
            explicit_string: !!str 123
            flag: true
            number: 7
            fraction: 0.25
            absent: null
            option: ON
            other_option: OFF
            """);
        Assert.Equal("123", document["quoted_number"]!.GetValue<string>());
        Assert.Equal("true", document["quoted_bool"]!.GetValue<string>());
        Assert.Equal("null", document["quoted_null"]!.GetValue<string>());
        Assert.Equal("123", document["explicit_string"]!.GetValue<string>());
        Assert.True(document["flag"]!.GetValue<bool>());
        Assert.Equal(7, document["number"]!.GetValue<int>());
        Assert.Equal(0.25m, document["fraction"]!.GetValue<decimal>());
        Assert.Null(document["absent"]);
        Assert.Equal("ON", document["option"]!.GetValue<string>());
        Assert.Equal("OFF", document["other_option"]!.GetValue<string>());
    }

    [Fact]
    public void Yaml_aliases_work_without_sharing_mutable_JSON_nodes()
    {
        var document = YamlJson.Load("first: &name { value: i64 }\nsecond: *name");
        document["first"]!["value"] = "string";
        Assert.Equal("i64", document["second"]!["value"].Text());
    }

    [Theory]
    [InlineData("key: 1\nkey: 2")]
    [InlineData("first: 1\n---\nsecond: 2")]
    [InlineData("null")]
    [InlineData("[]")]
    [InlineData("recursive: &self { child: *self }")]
    public void Ambiguous_or_non_mapping_yaml_is_rejected(string yaml) => Assert.ThrowsAny<Exception>(() => YamlJson.Load(yaml));

    [Theory]
    [InlineData("list?<any1>", "list")]
    [InlineData("decimal?<P,S>", "dec")]
    [InlineData("precision_timestamp_tz<7>", "ptstz")]
    [InlineData("map<string, list<i64>>", "map")]
    [InlineData("nstruct<amount: decimal<38,2>, count: i64>", "nstruct")]
    [InlineData("flowtide_types.u!dynamic?", "u!dynamic")]
    [InlineData("any2", "any")]
    public void Official_type_grammar_and_signature_names_are_supported(string expression, string shortName) =>
        Assert.Equal(shortName, TypeExpression.Parse(expression).ShortName);

    [Fact]
    public void Type_grammar_supports_return_type_programs()
    {
        var parsed = TypeExpression.Parse("""
            P = min(38, P1 + P2 + 1)
            S = min(38, S1 + S2)
            decimal<P,S>
            """);
        Assert.Equal("dec", parsed.ShortName);
    }

    [Fact]
    public void Type_grammar_collects_nested_dependency_references()
    {
        var parsed = TypeExpression.Parse("map<string, list<flowtide_types.u!dynamic>>");
        var reference = Assert.Single(parsed.References);
        Assert.Equal("flowtide_types", reference.Alias);
        Assert.Equal("dynamic", reference.Name);
    }

    [Theory]
    [InlineData("list<any>?")]
    [InlineData("decimal<P,S>?")]
    [InlineData("map<string>")]
    [InlineData("i64 @")]
    [InlineData("")]
    public void Malformed_types_are_rejected(string expression) => Assert.Throws<InvalidDataException>(() => TypeExpression.Parse(expression));
}
