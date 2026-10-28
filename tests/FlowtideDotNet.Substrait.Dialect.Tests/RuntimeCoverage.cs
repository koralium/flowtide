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

using System.Collections;
using System.Reflection;
using FlowtideDotNet.Core.Compute;
using FlowtideDotNet.Core.Engine;
using FlowtideDotNet.Substrait.Relations;
using FlowtideDotNet.Substrait.Type;

namespace FlowtideDotNet.Substrait.Dialect.Tests;

internal static class RuntimeCoverage
{
    // gettimestamp is a SQL/planner intrinsic, removed by TimestampToJoin before execution.
    // Its binding is separately checked against an actual SQL plan in RuntimeCoverageTests.
    internal const string TimestampIntrinsic = "/functions_datetime.yaml:gettimestamp";

    private static readonly Dictionary<string, string> RegistryCategories = new()
    {
        ["_scalarFunctions"] = "scalar", ["_columnScalarFunctions"] = "scalar",
        ["_aggregateFunctions"] = "aggregate", ["_columnAggregateFunctions"] = "aggregate",
        ["_bulkAggregationFunctions"] = "aggregate", ["_windowFunctions"] = "window",
        ["_bulkWindowFunctions"] = "window", ["_tableFunctions"] = "table", ["_columnTableFunctions"] = "table"
    };

    public static IEnumerable<ValidationIssue> Functions(DialectCatalog catalog, FunctionsRegister register)
    {
        var actual = RegistryCategories.Values.Distinct().ToDictionary(category => category,
            _ => new HashSet<string>(StringComparer.OrdinalIgnoreCase));
        // Keep reflection in one place and fail if a new registry is added or an existing one is renamed.
        var fields = typeof(FunctionsRegister).GetFields(BindingFlags.NonPublic | BindingFlags.Instance)
            .Where(field => field.FieldType.IsGenericType && field.FieldType.GetGenericTypeDefinition() == typeof(Dictionary<,>)
                && field.FieldType.GenericTypeArguments[0] == typeof(string)).ToArray();
        foreach (var name in RegistryCategories.Keys.Except(fields.Select(field => field.Name)))
            yield return new("FunctionsRegister", "", $"Registry '{name}' was removed or renamed; update the coverage check.");
        foreach (var field in fields)
        {
            if (!RegistryCategories.TryGetValue(field.Name, out var category))
            {
                yield return new("FunctionsRegister", "", $"Classify new registry '{field.Name}' in the coverage check.");
                continue;
            }
            actual[category].UnionWith(((IDictionary)field.GetValue(register)!).Keys.Cast<string>());
        }
        actual["scalar"].Add(TimestampIntrinsic);
        foreach (var (category, registered) in actual)
        {
            var documented = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
            foreach (var entry in catalog.Functions().Where(entry => entry.Category == category))
            {
                var uri = entry.Data["metadata"]?["legacy_extension_uri"].Text();
                if (string.IsNullOrWhiteSpace(uri))
                    yield return new(catalog.Dialect.Path, entry.Pointer + "/metadata/legacy_extension_uri", "Missing runtime binding URI.");
                else documented.Add(uri + ":" + entry.Data["name"].Text());
            }
            foreach (var name in registered.Except(documented))
                yield return new(catalog.Dialect.Path, "", $"Registered {category} function '{name}' is missing from the dialect.");
            foreach (var name in documented.Except(registered))
                yield return new(catalog.Dialect.Path, "", $"Documented {category} function '{name}' has no built-in registration.");
        }
    }

    public static IEnumerable<ValidationIssue> Relations(DialectCatalog catalog)
    {
        var standard = new Dictionary<string, string[]>
        {
            ["READ"] = ["ReadRelation", "VirtualTableReadRelation"], ["FILTER"] = ["FilterRelation"],
            ["PROJECT"] = ["ProjectRelation"], ["JOIN"] = ["JoinRelation"], ["MERGE_JOIN"] = ["MergeJoinRelation"],
            ["AGGREGATE"] = ["AggregateRelation"], ["SET"] = ["SetRelation"], ["REFERENCE"] = ["ReferenceRelation"],
            ["WRITE"] = ["WriteRelation"], ["CONSISTENT_PARTITION_WINDOW"] = ["ConsistentPartitionWindowRelation"],
            ["EXCHANGE"] = ["ExchangeRelation"], ["EXTENSION_MULTI"] = ["IterationRelation"]
        };
        var documented = new HashSet<string>(StringComparer.Ordinal);
        foreach (var relation in catalog.Dialect.Data["supported_relations"].Objects())
        {
            var kind = relation["relation"].Text() ?? "";
            if (standard.TryGetValue(kind, out var models)) documented.UnionWith(models);
            else if (kind is not "EXTENSION_SINGLE" and not "EXTENSION_LEAF")
                yield return new(catalog.Dialect.Path, "/supported_relations", $"Map relation kind '{kind}' to its native model in the coverage check.");
            if (relation["metadata"]?["messages"] is System.Text.Json.Nodes.JsonObject messages)
                foreach (var message in messages)
                    if (message.Value?["native_relation"].Text() is { } name) documented.Add(name);
            foreach (var extension in (relation["metadata"]?["native_only_extensions"]).Objects())
                if (extension["native_relation"].Text() is { } name) documented.Add(name);
        }
        var executed = typeof(SubstraitVisitor).GetMethods(BindingFlags.Public | BindingFlags.Instance | BindingFlags.DeclaredOnly)
            .Where(method => method.GetBaseDefinition() != method && method.GetParameters().Length > 0
                && typeof(Relation).IsAssignableFrom(method.GetParameters()[0].ParameterType))
            .Select(method => method.GetParameters()[0].ParameterType.Name).ToHashSet(StringComparer.Ordinal);
        // Root is a plan wrapper. Fetch is explicitly rejected by the runtime and documented under TopN.
        executed.ExceptWith(["RootRelation", "FetchRelation"]);
        foreach (var name in executed.Except(documented))
            yield return new(catalog.Dialect.Path, "/supported_relations", $"Runtime relation '{name}' is missing from the dialect.");
        foreach (var name in documented.Except(executed))
            yield return new(catalog.Dialect.Path, "/supported_relations", $"Documented relation '{name}' has no runtime visitor implementation.");
    }

    public static IEnumerable<ValidationIssue> Types(DialectCatalog catalog)
    {
        var declared = catalog.Dialect.Data["supported_types"].Objects()
            .Concat((catalog.Dialect.Data["metadata"]?["flowtide_type_compatibility"]?["additional_model_types"]).Objects());
        var documented = new HashSet<string>();
        foreach (var entry in declared)
        {
            var model = entry["metadata"]?["native_model"].Text() ?? entry["native_model"].Text();
            var className = model?.Split(' ')[0];
            var type = typeof(SubstraitBaseType).Assembly.GetType($"FlowtideDotNet.Substrait.Type.{className}");
            if (type == null || !typeof(SubstraitBaseType).IsAssignableFrom(type))
            {
                yield return new(catalog.Dialect.Path, "/supported_types", $"Unknown native type model '{model}'.");
                continue;
            }
            // The Type property is independent of child types. Supply valid children for container constructors.
            var instance = type == typeof(ListType) ? new ListType(new AnyType())
                : type == typeof(MapType) ? new MapType(new AnyType(), new AnyType())
                : (SubstraitBaseType)Activator.CreateInstance(type)!;
            documented.Add(instance.Type.ToString());
        }
        foreach (var name in Enum.GetNames<SubstraitType>().Except(documented))
            yield return new(catalog.Dialect.Path, "/supported_types", $"Native type '{name}' needs a supported-type entry or an explicit compatibility note.");
    }
}
