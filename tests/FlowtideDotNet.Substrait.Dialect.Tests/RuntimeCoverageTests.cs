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

using System.Text.Json.Nodes;
using FlowtideDotNet.Core.Compute;
using FlowtideDotNet.Core.Compute.Internal;
using FlowtideDotNet.Substrait.Sql;

namespace FlowtideDotNet.Substrait.Dialect.Tests;

public class RuntimeCoverageTests
{
    private static FunctionsRegister Builtins()
    {
        var register = new FunctionsRegister();
        BuiltinFunctions.RegisterFunctions(register);
        return register;
    }

    [Fact]
    public void All_builtin_function_registrations_are_documented() =>
        CatalogValidationTests.Valid(RuntimeCoverage.Functions(CatalogValidationTests.Catalog, Builtins()));

    [Fact]
    public void Builtins_use_declared_URN_namespaces_and_reject_legacy_file_identifiers()
    {
        var register = Builtins();
        Assert.True(register.TryGetColumnScalarFunction("extension:io.substrait:functions_string", "lower", out _));
        Assert.True(register.TryGetColumnScalarFunction("extension:io.github.koralium.flowtide:functions_string", "from_json", out _));
        Assert.False(register.TryGetColumnScalarFunction("extension:io.substrait:functions_string", "from_json", out _));
        Assert.Throws<ArgumentException>(() => register.RegisterColumnScalarFunction("/custom.yaml", "invalid", (_, _, _, _) => throw new NotSupportedException()));
    }

    [Fact]
    public void A_new_undocumented_registration_fails_validation()
    {
        var register = Builtins();
        register.RegisterColumnScalarFunction("extension:org.example:validation", "new_function", (_, _, _, _) => throw new NotSupportedException());
        Assert.Contains(RuntimeCoverage.Functions(CatalogValidationTests.Catalog, register),
            error => error.Message.Contains("extension:org.example:validation:new_function") && error.Message.Contains("missing"));
    }

    [Fact]
    public void Stale_documented_runtime_bindings_fail_validation()
    {
        var catalog = CatalogValidationTests.Catalog;
        var data = (JsonObject)catalog.Dialect.Data.DeepClone();
        data["supported_scalar_functions"]![0]!["metadata"]!["runtime_extension_urn"] = "extension:org.example:missing";
        Assert.Contains(RuntimeCoverage.Functions(catalog.WithDialect(data), Builtins()),
            error => error.Message.Contains("extension:org.example:missing:") && error.Message.Contains("no built-in registration"));
    }

    [Fact]
    public void A_registered_namespace_cannot_replace_the_declared_extension_contract()
    {
        var catalog = CatalogValidationTests.Catalog;
        var data = (JsonObject)catalog.Dialect.Data.DeepClone();
        var sum = data["supported_aggregate_functions"].Objects().First(entry =>
            entry["name"].Text() == "sum" && entry["source"].Text() == "arithmetic");
        sum["metadata"]!["runtime_extension_urn"] = "extension:io.github.koralium.flowtide:functions_arithmetic";
        Assert.Contains(RuntimeCoverage.Functions(catalog.WithDialect(data), Builtins()),
            error => error.Message.Contains("does not match the source extension URN"));
    }

    [Fact]
    public void Timestamp_intrinsic_is_bound_by_the_SQL_planner()
    {
        var builder = new SqlPlanBuilder();
        builder.Sql("CREATE TABLE input_data (id INT); SELECT gettimestamp() FROM input_data;");
        var proto = SubstraitSerializer.Serialize(builder.GetPlan());
        var binding = Assert.Single(proto.Extensions.Where(extension => extension.ExtensionFunction?.Name == "gettimestamp")).ExtensionFunction;
        var urn = Assert.Single(proto.ExtensionUrns.Where(urn => urn.ExtensionUrnAnchor == binding.ExtensionUrnReference)).Urn;
        Assert.Equal(RuntimeCoverage.TimestampIntrinsic, urn + ":" + binding.Name);
        var entry = Assert.Single(CatalogValidationTests.Catalog.Functions().Where(entry => entry.Category == "scalar"
            && entry.Data["name"].Text() == binding.Name));
        Assert.Equal("TimestampToJoin", entry.Data["metadata"]?["requires_optimizer_pass"].Text());
    }

    [Fact]
    public void Runtime_relation_implementations_are_documented() =>
        CatalogValidationTests.Valid(RuntimeCoverage.Relations(CatalogValidationTests.Catalog));

    [Fact]
    public void Native_type_models_are_documented_or_explicitly_qualified() =>
        CatalogValidationTests.Valid(RuntimeCoverage.Types(CatalogValidationTests.Catalog));

    [Fact]
    public void Removing_relation_and_type_entries_fails_coverage()
    {
        var catalog = CatalogValidationTests.Catalog;
        var data = (JsonObject)catalog.Dialect.Data.DeepClone();
        ((JsonArray)data["supported_relations"]!).RemoveAt(0);
        ((JsonArray)data["supported_types"]!).RemoveAt(0);
        var changed = catalog.WithDialect(data);
        Assert.Contains(RuntimeCoverage.Relations(changed), error => error.Message.Contains("ReadRelation"));
        Assert.Contains(RuntimeCoverage.Types(changed), error => error.Message.Contains("Bool"));
    }
}
