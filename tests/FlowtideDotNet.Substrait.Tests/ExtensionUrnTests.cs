// Licensed under the Apache License, Version 2.0 (the "License")
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

using FlowtideDotNet.Substrait.Expressions;
using FlowtideDotNet.Substrait.Expressions.Literals;
using FlowtideDotNet.Substrait.FunctionExtensions;
using FlowtideDotNet.Substrait.Relations;
using FlowtideDotNet.Substrait.Sql;
using FlowtideDotNet.Substrait.Type;
using Google.Protobuf;
using Protobuf = Substrait.Protobuf;

namespace FlowtideDotNet.Substrait.Tests;

public class ExtensionUrnTests
{
    private static Plan Sql(string sql)
    {
        var builder = new SqlPlanBuilder();
        builder.Sql(sql);
        return builder.GetPlan();
    }

    [Fact]
    public void Extension_fields_use_the_modern_Substrait_wire_numbers()
    {
        Assert.Equal(8, Protobuf.Plan.ExtensionUrnsFieldNumber);
        Assert.Equal(8, Protobuf.ExtendedExpression.ExtensionUrnsFieldNumber);
        Assert.Null(Protobuf.Plan.Descriptor.FindFieldByNumber(1));
        Assert.Null(Protobuf.ExtendedExpression.Descriptor.FindFieldByNumber(1));
        Assert.Equal(4, Protobuf.SimpleExtensionDeclaration.Types.ExtensionFunction.ExtensionUrnReferenceFieldNumber);
        Assert.Equal(4, Protobuf.SimpleExtensionDeclaration.Types.ExtensionType.ExtensionUrnReferenceFieldNumber);
        Assert.Equal(4, Protobuf.SimpleExtensionDeclaration.Types.ExtensionTypeVariation.ExtensionUrnReferenceFieldNumber);
    }

    [Fact]
    public void Shared_extension_namespaces_are_deduplicated_and_survive_binary_and_JSON()
    {
        var plan = Sql("INSERT INTO output SELECT lower('A'), upper('B'), round(1), ceil(1);");
        var wire = SubstraitSerializer.Serialize(plan);
        Assert.Single(wire.ExtensionUrns.Where(extension => extension.Urn == FunctionsString.Urn));
        Assert.Equal(wire.ExtensionUrns.Count, wire.ExtensionUrns.Select(extension => extension.Urn).Distinct().Count());
        Assert.All(wire.ExtensionUrns, extension => Assert.StartsWith("extension:", extension.Urn));
        Assert.Equal(plan, new SubstraitDeserializer().Deserialize(Protobuf.Plan.Parser.ParseFrom(wire.ToByteArray())));
        var json = SubstraitSerializer.SerializeToJson(plan);
        Assert.Contains("extensionUrns", json);
        Assert.DoesNotContain("extensionUris", json);
        Assert.DoesNotContain(".yaml", json);
        Assert.Equal(plan, SubstraitDeserializer.DeserializeFromJson(json));
    }

    [Theory]
    [InlineData("INSERT INTO output SELECT lower('ABC');", "lower", FunctionsString.Urn)]
    [InlineData("INSERT INTO output SELECT trim(' a ');", "trim", FunctionsString.FlowtideUrn)]
    [InlineData("INSERT INTO output SELECT from_json('{}');", "from_json", FunctionsString.FlowtideUrn)]
    [InlineData("INSERT INTO output SELECT round(1.5);", "round", FunctionsRounding.FlowtideUrn)]
    [InlineData("INSERT INTO output SELECT ceil(1.5);", "ceil", FunctionsRounding.Urn)]
    [InlineData("INSERT INTO output SELECT 'abc' LIKE 'a%';", "like", FunctionsString.Urn)]
    [InlineData("INSERT INTO output SELECT 'a_c' LIKE 'a!_c' ESCAPE '!';", "like", FunctionsString.FlowtideUrn)]
    [InlineData("CREATE TABLE input_data (value BIGINT); INSERT INTO output SELECT sum(value) FROM input_data;", "sum", FunctionsArithmetic.Urn)]
    [InlineData("CREATE TABLE input_data (value DECIMAL(10,2)); INSERT INTO output SELECT sum(value) FROM input_data;", "sum", FunctionsArithmetic.FlowtideUrn)]
    [InlineData("CREATE TABLE input_data (value ANY); INSERT INTO output SELECT sum(value) FROM input_data;", "sum", FunctionsArithmetic.FlowtideUrn)]
    [InlineData("CREATE TABLE input_data (value BIGINT); INSERT INTO output SELECT avg(value) FROM input_data;", "avg", FunctionsArithmetic.FlowtideUrn)]
    [InlineData("CREATE TABLE input_data (value DOUBLE); INSERT INTO output SELECT avg(value) FROM input_data;", "avg", FunctionsArithmetic.Urn)]
    [InlineData("CREATE TABLE input_data (value TIMESTAMP); INSERT INTO output SELECT min(value) FROM input_data;", "min", FunctionsDatetime.StandardUrn)]
    [InlineData("CREATE TABLE input_data (value DECIMAL(10,2)); INSERT INTO output SELECT max(value) FROM input_data;", "max", FunctionsArithmetic.DecimalUrn)]
    [InlineData("CREATE TABLE input_data (value BIGINT); INSERT INTO output SELECT count(DISTINCT value) FROM input_data;", "count_distinct", FunctionsAggregateGeneric.FlowtideUrn)]
    [InlineData("CREATE TABLE input_data (value BIGINT); INSERT INTO output SELECT lead(value) OVER (ORDER BY value) FROM input_data;", "lead", FunctionsArithmetic.Urn)]
    [InlineData("CREATE TABLE input_data (value BIGINT); INSERT INTO output SELECT lead(value, 1) OVER (ORDER BY value) FROM input_data;", "lead", FunctionsArithmetic.FlowtideUrn)]
    [InlineData("CREATE TABLE input_data (value BIGINT); INSERT INTO output SELECT last_value(value) IGNORE NULLS OVER (ORDER BY value) FROM input_data;", "last_value", FunctionsArithmetic.FlowtideUrn)]
    [InlineData("INSERT INTO output SELECT value FROM UNNEST(list(1,2)) value;", "unnest", FunctionsTableGeneric.Urn)]
    public void SQL_selects_the_extension_contract_for_the_function(string sql, string function, string expectedUrn)
    {
        var plan = Sql(sql);
        var wire = SubstraitSerializer.Serialize(plan);
        var declaration = Assert.Single(wire.Extensions.Where(extension => extension.ExtensionFunction?.Name == function)).ExtensionFunction;
        var reference = Assert.Single(wire.ExtensionUrns.Where(extension => extension.ExtensionUrnAnchor == declaration.ExtensionUrnReference));
        Assert.Equal(expectedUrn, reference.Urn);
    }

    [Fact]
    public void Identically_named_functions_in_different_namespaces_keep_distinct_anchors()
    {
        var plan = new Plan
        {
            Relations = [new ProjectRelation
            {
                Input = new ReadRelation
                {
                    NamedTable = new NamedTable { Names = ["input_data"] },
                    BaseSchema = new NamedStruct { Names = ["value"] }
                },
                Expressions = new[] { FunctionsString.Urn, "extension:org.example:strings" }
                    .Select(urn => (Expression)new ScalarFunction
                    {
                        ExtensionUrn = urn,
                        ExtensionName = "lower",
                        Arguments = [new StringLiteral { Value = "A" }]
                    }).ToList()
            }]
        };
        var wire = SubstraitSerializer.Serialize(plan);
        Assert.Equal(2, wire.ExtensionUrns.Count);
        Assert.Equal(2, wire.Extensions.Select(extension => extension.ExtensionFunction.FunctionAnchor).Distinct().Count());
        Assert.Equal(plan, new SubstraitDeserializer().Deserialize(wire));
    }

    [Theory]
    [InlineData("INSERT INTO output SELECT lower('A'), from_json('{}');")]
    [InlineData("CREATE TABLE input_data (value ANY); INSERT INTO output SELECT sum(value), count(DISTINCT value) FROM input_data;")]
    [InlineData("CREATE TABLE input_data (value BIGINT); INSERT INTO output SELECT lead(value, 1) OVER (ORDER BY value) FROM input_data;")]
    [InlineData("INSERT INTO output SELECT value FROM UNNEST(list(1,2)) value;")]
    public void Scalar_aggregate_window_and_table_bindings_survive_JSON(string sql)
    {
        var plan = Sql(sql);
        var original = SubstraitSerializer.Serialize(plan);
        var restored = SubstraitSerializer.Serialize(SubstraitDeserializer.DeserializeFromJson(SubstraitSerializer.SerializeToJson(plan)));
        Assert.Equal(original.ExtensionUrns, restored.ExtensionUrns);
        Assert.Equal(original.Extensions, restored.Extensions);
    }

    [Fact]
    public void Dynamic_and_named_struct_types_share_the_Flowtide_type_namespace()
    {
        var plan = new Plan
        {
            Relations = [new ProjectRelation
            {
                Input = new ReadRelation
                {
                    NamedTable = new NamedTable { Names = ["input_data"] },
                    BaseSchema = new NamedStruct
                    {
                        Names = ["payload"],
                        Struct = new Struct { Types = [new AnyType()] }
                    }
                },
                Expressions = [new CastExpression
                {
                    Expression = new NullLiteral(),
                    Type = new NamedStruct
                    {
                        Names = ["value"],
                        Struct = new Struct { Types = [new AnyType()] },
                        Nullable = true
                    }
                }]
            }]
        };
        var wire = SubstraitSerializer.Serialize(plan);
        var extension = Assert.Single(wire.ExtensionUrns);
        Assert.Equal(FunctionsTypes.Urn, extension.Urn);
        Assert.Contains(wire.Extensions, declaration => declaration.ExtensionType?.Name == FunctionsTypes.Dynamic);
        Assert.Contains(wire.Extensions, declaration => declaration.ExtensionType?.Name == FunctionsTypes.NamedStruct);
        Assert.Equal(plan, new SubstraitDeserializer().Deserialize(wire));
        extension.Urn = "extension:org.example:unrelated_types";
        var error = Assert.Throws<NotImplementedException>(() => new SubstraitDeserializer().Deserialize(wire));
        Assert.Contains(extension.Urn, error.Message);
    }

    [Theory]
    [InlineData("/functions_string.yaml")]
    [InlineData("https://example.com/functions.yaml")]
    [InlineData("")]
    [InlineData("extension::functions")]
    [InlineData("extension:org.example:")]
    public void Legacy_and_malformed_extension_identifiers_are_rejected(string urn) =>
        Assert.Throws<ArgumentException>(() => ExtensionUrn.Validate(urn));

    [Fact]
    public void Legacy_URI_JSON_is_not_accepted_as_a_modern_plan() =>
        Assert.Throws<InvalidProtocolBufferException>(() => SubstraitDeserializer.DeserializeFromJson(
            """{"extensionUris":[{"extensionUriAnchor":1,"uri":"/functions_string.yaml"}]}"""));
}
