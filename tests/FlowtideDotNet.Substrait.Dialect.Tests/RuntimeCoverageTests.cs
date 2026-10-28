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
    public void A_new_undocumented_registration_fails_validation()
    {
        var register = Builtins();
        register.RegisterColumnScalarFunction("/validation.yaml", "new_function", (_, _, _, _) => throw new NotSupportedException());
        Assert.Contains(RuntimeCoverage.Functions(CatalogValidationTests.Catalog, register),
            error => error.Message.Contains("/validation.yaml:new_function") && error.Message.Contains("missing"));
    }

    [Fact]
    public void Stale_documented_runtime_bindings_fail_validation()
    {
        var catalog = CatalogValidationTests.Catalog;
        var data = (JsonObject)catalog.Dialect.Data.DeepClone();
        data["supported_scalar_functions"]![0]!["metadata"]!["legacy_extension_uri"] = "/missing.yaml";
        Assert.Contains(RuntimeCoverage.Functions(catalog.WithDialect(data), Builtins()),
            error => error.Message.Contains("/missing.yaml:") && error.Message.Contains("no built-in registration"));
    }

    [Fact]
    public void Timestamp_intrinsic_is_bound_by_the_SQL_planner()
    {
        var builder = new SqlPlanBuilder();
        builder.Sql("CREATE TABLE input_data (id INT); SELECT gettimestamp() FROM input_data;");
        var proto = SubstraitSerializer.Serialize(builder.GetPlan());
        var binding = Assert.Single(proto.Extensions.Where(extension => extension.ExtensionFunction?.Name == "gettimestamp")).ExtensionFunction;
        var uri = Assert.Single(proto.ExtensionUris.Where(uri => uri.ExtensionUriAnchor == binding.ExtensionUriReference)).Uri;
        Assert.Equal(RuntimeCoverage.TimestampIntrinsic, uri + ":" + binding.Name);
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
