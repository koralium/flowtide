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

namespace FlowtideDotNet.Substrait.Documentation.Catalog;

internal sealed partial class DialectCatalog
{
    // Table functions live in arbitrary metadata, so upstream JSON Schema cannot check them.
    public IEnumerable<ValidationIssue> ValidateTableMetadata()
    {
        var issues = new List<ValidationIssue>();
        void Require(bool valid, CatalogDocument doc, string pointer, string message)
        {
            if (!valid) issues.Add(new(doc.Path, pointer, message));
        }
        JsonObject[] Objects(JsonNode? node, CatalogDocument doc, string pointer)
        {
            Require(node is JsonArray { Count: > 0 } array && array.All(item => item is JsonObject),
                doc, pointer, "Expected a non-empty array of objects.");
            return node.Objects().ToArray();
        }
        void Text(JsonNode? node, CatalogDocument doc, string pointer) =>
            Require(!string.IsNullOrWhiteSpace(node.Text()), doc, pointer, "Expected a non-empty string.");

        var bindingPath = "/metadata/flowtide_table_functions/supported_functions";
        var bindings = Objects(Dialect.Data["metadata"]?["flowtide_table_functions"]?["supported_functions"], Dialect, bindingPath);
        for (var i = 0; i < bindings.Length; i++)
        {
            var binding = bindings[i];
            var pointer = $"{bindingPath}/{i}";
            foreach (var key in new[] { "source", "name" }) Text(binding[key], Dialect, $"{pointer}/{key}");
            Require(binding["supported_impls"] is JsonArray { Count: > 0 } sigs && sigs.All(sig => sig.Text() != null),
                Dialect, pointer + "/supported_impls", "Expected a non-empty array of signature strings.");
            Text(binding["metadata"]?["runtime_extension_urn"], Dialect, pointer + "/metadata/runtime_extension_urn");
        }
        foreach (var doc in LocalExtensions)
        {
            if (doc.Data["metadata"] is not JsonObject metadata || !metadata.ContainsKey("table_functions")) continue;
            var functions = Objects(metadata["table_functions"], doc, "/metadata/table_functions");
            for (var f = 0; f < functions.Length; f++)
            {
                var function = functions[f];
                var pointer = $"/metadata/table_functions/{f}";
                Text(function["name"], doc, pointer + "/name");
                Text(function["metadata"]?["runtime_extension_urn"], doc, pointer + "/metadata/runtime_extension_urn");
                var countNode = function["metadata"]?["output_column_count"];
                var hasCount = countNode is JsonValue value && value.TryGetValue<int>(out var count) && count > 0;
                Require(hasCount, doc, pointer + "/metadata/output_column_count", "Expected a positive output column count.");
                var impls = Objects(function["impls"], doc, pointer + "/impls");
                for (var i = 0; i < impls.Length; i++)
                {
                    var implPath = $"{pointer}/impls/{i}";
                    var impl = impls[i];
                    Require(impl["args"] is JsonArray argsArray && argsArray.All(arg => arg is JsonObject),
                        doc, implPath + "/args", "Expected an array of value arguments.");
                    var args = impl["args"].Objects().ToArray();
                    for (var a = 0; a < args.Length; a++)
                    {
                        Text(args[a]["name"], doc, $"{implPath}/args/{a}/name");
                        Require(args[a]["value"] is JsonObject || args[a]["value"].Text() != null,
                            doc, $"{implPath}/args/{a}/value", "Expected a value argument type.");
                        Require(!args[a].ContainsKey("constant") || args[a]["constant"] is JsonValue constant && constant.TryGetValue<bool>(out _),
                            doc, $"{implPath}/args/{a}/constant", "Expected a boolean.");
                    }
                    var columns = Objects(impl["output_columns"], doc, implPath + "/output_columns");
                    Require(!hasCount || columns.Length == countNode!.GetValue<int>(), doc, implPath + "/output_columns", "Output columns disagree with output_column_count.");
                    for (var c = 0; c < columns.Length; c++)
                    {
                        Text(columns[c]["name"], doc, $"{implPath}/output_columns/{c}/name");
                        Require(columns[c]["value"] is JsonObject || columns[c]["value"].Text() != null,
                            doc, $"{implPath}/output_columns/{c}/value", "Expected an output type.");
                    }
                    if (function["metadata"]?["literal_arguments"] is JsonArray literals)
                        foreach (var literal in literals)
                            Require(literal is JsonValue index && index.TryGetValue<int>(out var n) && n >= 0 && n < args.Length
                                && args[n]["constant"]?.GetValue<bool>() == true, doc, pointer + "/metadata/literal_arguments",
                                "Literal argument indices must identify constant arguments in every implementation.");
                }
            }
        }
        return issues;
    }
}
