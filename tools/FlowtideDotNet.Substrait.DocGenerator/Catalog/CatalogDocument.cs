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

internal sealed record CatalogDocument(string Path, JsonObject Data)
{
    public string Urn => Data["urn"]?.GetValue<string>() ?? throw new InvalidDataException($"{Path}: missing urn.");
    public static CatalogDocument Load(string path, string root)
    {
        var displayPath = System.IO.Path.GetRelativePath(root, path).Replace('\\', '/');
        try { return new(displayPath, YamlJson.Load(File.ReadAllText(path))); }
        catch (Exception error) { throw new InvalidDataException($"{displayPath}: {error.Message}", error); }
    }
}

internal sealed record FunctionEntry(string Category, string Pointer, JsonObject Data);
internal sealed record ResolvedImplementation(CatalogDocument Source, JsonObject Definition, JsonObject Data, string Signature);
internal sealed record ValidationIssue(string File, string Pointer, string Message)
{
    public override string ToString() => $"{File}#{Pointer}: {Message}";
}

internal static class JsonCatalog
{
    public static IEnumerable<JsonObject> Objects(this JsonNode? node) => node is JsonArray array
        ? array.OfType<JsonObject>() : [];
    public static string? Text(this JsonNode? node) => node is JsonValue value && value.TryGetValue<string>(out var text) ? text : null;
    public static IEnumerable<string> Strings(this JsonNode? node) => node is JsonArray array
        ? array.Select(item => item?.GetValue<string>() ?? "") : [];
    public static string Pointer(string name) => name.Replace("~", "~0").Replace("/", "~1");
}
