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
using System.Xml.Linq;
using FlowtideDotNet.Substrait.Sql;

namespace FlowtideDotNet.Substrait.Dialect.Tests;

public class DocumentationGenerationTests
{
    private static IReadOnlyDictionary<string, string> Generate(DialectCatalog? catalog = null) => ReferenceGenerator.Generate(
        catalog ?? CatalogValidationTests.Catalog, File.ReadAllText(Path.Combine(AppContext.BaseDirectory, "docs", "reference", "expressions.md")));

    [Fact]
    public void Checked_in_reference_outputs_are_current()
    {
        using var output = new StringWriter();
        var status = Documentation.Program.WriteFiles(AppContext.BaseDirectory, Generate(), check: true, output);
        Assert.True(status == 0, output.ToString());
    }

    [Fact]
    public void Every_supported_profile_and_its_notes_appear_in_Markdown_and_XML()
    {
        var generated = Generate();
        var markdown = generated["docs/llms/expressions.txt"];
        var xml = XDocument.Parse(generated["docs/reference/sql-functions.xml"]);
        foreach (var entry in CatalogValidationTests.Catalog.Functions()
            .Where(entry => entry.Data["metadata"]?["sql_exposed"]?.GetValue<bool>() != false))
        {
            var topic = Assert.Single(xml.Root!.Elements("topic").Where(topic => topic.Attribute("id")!.Value == "functions-" + entry.Category));
            Assert.Contains(entry.Data["metadata"]!["sql_signature"].Text()!, topic.Value);
            Assert.Contains(entry.Data["metadata"]!["sql_signature"].Text()!, markdown);
            if (entry.Data["metadata"]!["notes"].Text() is { } notes)
            {
                Assert.Contains(notes, markdown);
                Assert.Contains(notes, topic.Value);
            }
        }
        Assert.Equal(markdown, generated["docs/docs/expressions/functions-reference.md"]);
    }

    [Fact]
    public void SQL_spellings_and_only_selected_extension_overloads_are_emitted()
    {
        var generated = Generate();
        var xml = XDocument.Parse(generated["docs/reference/sql-functions.xml"]);
        var scalar = Assert.Single(xml.Root!.Elements("topic").Where(topic => topic.Attribute("id")!.Value == "functions-scalar"));
        Assert.Contains(scalar.Elements("para"), para => para.Element("b")?.Value == "len(string)");
        Assert.DoesNotContain(scalar.Elements("para"), para => para.Element("b")?.Value.StartsWith("char_length(") == true);
        Assert.DoesNotContain("tumbling_window_start", scalar.Value);
        var rounding = CatalogValidationTests.Catalog.Functions().Where(entry => entry.Category == "scalar" && entry.Data["name"].Text() == "round").ToArray();
        Assert.NotEmpty(rounding);
        Assert.All(rounding.SelectMany(entry => CatalogValidationTests.Catalog.Implementations(entry)), implementation =>
            Assert.Single(implementation.Data["args"].Objects()));
        Assert.Contains("Output column count: 2", xml.Root.Value);
        Assert.Contains("Literal arguments: [1, 2]", xml.Root.Value);
    }

    [Fact]
    public void XML_escaping_preserves_SQL_operators_and_example_text()
    {
        const string notes = "Compare x < y & y > z; string value <not-an-element>.";
        const string example = "SELECT '``` | <tag> & value' FROM input_data";
        var catalog = CatalogValidationTests.Catalog;
        var data = (JsonObject)catalog.Dialect.Data.DeepClone();
        var metadata = data["supported_scalar_functions"]![0]!["metadata"]!;
        metadata["notes"] = notes;
        metadata["sql_example"] = example;
        var generated = Generate(catalog.WithDialect(data));
        var xml = XDocument.Parse(generated["docs/reference/sql-functions.xml"]);
        Assert.Contains(notes, xml.Root!.Value);
        Assert.Contains(example, xml.Root.Value);
        Assert.Empty(xml.Descendants("not-an-element"));
        Assert.Contains("````sql\n" + example + "\n````", generated["docs/llms/expressions.txt"]);
    }

    [Fact]
    public void Check_mode_reports_missing_or_stale_files_without_writing()
    {
        var directory = Path.Combine(Path.GetTempPath(), "flowtide-sql-reference-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(directory);
        try
        {
            var file = Path.Combine(directory, "stale.txt");
            File.WriteAllText(file, "old");
            var files = new Dictionary<string, string> { ["stale.txt"] = "new", ["missing.txt"] = "missing" };
            using var output = new StringWriter();
            Assert.Equal(1, Documentation.Program.WriteFiles(directory, files, check: true, output));
            Assert.Contains("stale.txt", output.ToString());
            Assert.Contains("missing.txt", output.ToString());
            Assert.Equal("old", File.ReadAllText(file));
            Assert.False(File.Exists(Path.Combine(directory, "missing.txt")));
            Assert.Equal(0, Documentation.Program.WriteFiles(directory, files, check: false, output));
            var timestamp = File.GetLastWriteTimeUtc(file);
            Assert.Equal(0, Documentation.Program.WriteFiles(directory, files, check: false, output));
            Assert.Equal(timestamp, File.GetLastWriteTimeUtc(file));
        }
        finally
        {
            // Delete only these exact test-owned files; no recursive directory operation.
            foreach (var file in new[] { "stale.txt", "missing.txt" }) File.Delete(Path.Combine(directory, file));
            Directory.Delete(directory);
        }
    }

    [Fact]
    public void Nuget_assembly_XML_contains_expanded_reference_content_and_API_links()
    {
        var substrait = XDocument.Load(Path.ChangeExtension(typeof(SqlPlanBuilder).Assembly.Location, ".xml"));
        var members = substrait.Descendants("member").ToDictionary(member => member.Attribute("name")!.Value);
        Assert.Contains("len(string)", members["T:FlowtideDotNet.Substrait.Sql.SqlFunctionReference.Scalar"].Value);
        Assert.Contains("COUNT(DISTINCT x)", members["T:FlowtideDotNet.Substrait.Sql.SqlFunctionReference.Aggregate"].Value);
        Assert.Contains("ROWS", members["T:FlowtideDotNet.Substrait.Sql.SqlFunctionReference.Window"].Value);
        Assert.Contains("tumbling_window", members["T:FlowtideDotNet.Substrait.Sql.SqlFunctionReference.Table"].Value);
        Assert.Contains("len(string)", members["M:FlowtideDotNet.Substrait.Sql.SqlPlanBuilder.Sql(System.String)"].Value);
        Assert.Empty(substrait.Descendants("include"));
        var diAssembly = typeof(Microsoft.Extensions.DependencyInjection.FlowtideServiceCollectionExtensions).Assembly;
        var dependencyInjection = XDocument.Load(Path.ChangeExtension(diAssembly.Location, ".xml"));
        var registration = Assert.Single(dependencyInjection.Descendants("member")
            .Where(member => member.Attribute("name")!.Value.Contains(".AddFlowtideStream(")));
        Assert.Contains("A stream needs a plan, source and sink connectors, and state storage.", registration.Element("remarks")!.Value);
        Assert.Contains("await host.RunAsync();", registration.Element("example")!.Element("code")!.Value);
        Assert.Contains(registration.Elements("seealso"), see => see.Attribute("cref")?.Value == "T:FlowtideDotNet.Substrait.Sql.SqlFunctionReference");
        var sqlSetup = Assert.Single(dependencyInjection.Descendants("member")
            .Where(member => member.Attribute("name")!.Value.Contains(".AddSqlTextAsPlan(")));
        Assert.Contains("len(string)", sqlSetup.Value);
        Assert.Contains(sqlSetup.Descendants("see"), see => see.Attribute("cref")?.Value == "T:FlowtideDotNet.Substrait.Sql.SqlFunctionReference.Scalar");
        Assert.Empty(dependencyInjection.Descendants("include"));
    }
}
