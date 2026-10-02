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

using FlowtideDotNet.Core.Lineage.Dbt.Internal;

namespace FlowtideDotNet.Core.Tests.LineageTests.Dbt
{
    public class DbtNamingTests
    {
        private static IReadOnlyDictionary<DbtTableIdentity, DbtNodeName> Name(DbtTableIdentity[] models, DbtTableIdentity[] sources, string project = "flowtide")
        {
            return DbtNodeNamer.Name(project, models, sources);
        }

        [Theory]
        [InlineData("Kafka", "kafka")]
        [InlineData("my-db.Schema", "my_db_schema")]
        [InlineData("a_1", "a_1")]
        [InlineData("åäö", "___")]
        [InlineData("", "_")]
        public void Sanitize(string value, string expected)
        {
            Assert.Equal(expected, DbtNodeNamer.Sanitize(value));
        }

        [Fact]
        public void ModelNameSkipsMissingParts()
        {
            var model = new DbtTableIdentity("elasticsearch", null, "", "MyIndex");

            var name = Name([model], [])[model];

            Assert.Equal("elasticsearch__myindex", name.Name);
            Assert.Equal("model.flowtide.elasticsearch__myindex", name.UniqueId);
            Assert.Equal(["flowtide", "elasticsearch", "elasticsearch__myindex"], name.Fqn);
            Assert.Equal("elasticsearch/elasticsearch__myindex.sql", name.Path);
            Assert.Equal("models/elasticsearch/elasticsearch__myindex.sql", name.OriginalFilePath);
            Assert.Null(name.SourceName);
        }

        [Fact]
        public void SourceIdIsSanitizedNameIsPhysical()
        {
            var source = new DbtTableIdentity("mssql", "Shop", "dbo", "Orders");

            var name = Name([], [source])[source];

            Assert.Equal("source.flowtide.mssql_shop_dbo.orders", name.UniqueId);
            Assert.Equal("Orders", name.Name);
            Assert.Equal("mssql_shop_dbo", name.SourceName);
            Assert.Equal(["flowtide", "mssql_shop_dbo", "Orders"], name.Fqn);
            Assert.Equal("models/flowtide_sources.yml", name.Path);
            Assert.Equal("models/flowtide_sources.yml", name.OriginalFilePath);
        }

        [Fact]
        public void NamespaceCollisionHashesBoth()
        {
            var first = new DbtTableIdentity("kafka://a:9092", null, "", "t");
            var second = new DbtTableIdentity("kafka://b:9092", null, "", "t");

            var names = Name([first, second], []);

            Assert.Equal($"model.flowtide.kafka_{DbtHashing.Hash8("kafka://a:9092")}__t", names[first].UniqueId);
            Assert.Equal($"model.flowtide.kafka_{DbtHashing.Hash8("kafka://b:9092")}__t", names[second].UniqueId);
            Assert.NotEqual(names[first].UniqueId, names[second].UniqueId);
        }

        [Fact]
        public void SourceNameCollisionHashesBoth()
        {
            var first = new DbtTableIdentity("mssql", "Shop", "dbo", "t");
            var second = new DbtTableIdentity("mssql", "shop", "dbo", "t");

            var names = Name([], [first, second]);

            Assert.Equal($"mssql_shop_dbo_{DbtHashing.Hash8("mssql", "Shop", "dbo")}", names[first].SourceName);
            Assert.Equal($"mssql_shop_dbo_{DbtHashing.Hash8("mssql", "shop", "dbo")}", names[second].SourceName);
            Assert.Equal("t", names[first].Name);
        }

        [Fact]
        public void SourceTableCollisionHashesBoth()
        {
            var first = new DbtTableIdentity("mssql", "shop", "dbo", "Orders");
            var second = new DbtTableIdentity("mssql", "shop", "dbo", "orders");

            var names = Name([], [first, second]);

            Assert.Equal($"source.flowtide.mssql_shop_dbo.orders_{DbtHashing.Hash8("Orders")}", names[first].UniqueId);
            Assert.Equal($"source.flowtide.mssql_shop_dbo.orders_{DbtHashing.Hash8("orders")}", names[second].UniqueId);
        }

        [Fact]
        public void ModelNameCollisionHashesBoth()
        {
            var first = new DbtTableIdentity("mssql", "shop", "dbo", "Orders");
            var second = new DbtTableIdentity("mssql", "shop", "dbo", "orders");

            var names = Name([first, second], []);

            Assert.Equal($"mssql__shop__dbo__orders_{DbtHashing.Hash8("mssql", "shop", "dbo", "Orders")}", names[first].Name);
            Assert.Equal($"mssql__shop__dbo__orders_{DbtHashing.Hash8("mssql", "shop", "dbo", "orders")}", names[second].Name);
        }

        [Fact]
        public void ProjectNameIsSanitizedIntoIds()
        {
            var store = DbtTestData.Store(o => o.ProjectName = "My Project");
            store.Register(DbtTestData.Snapshot(
                [DbtTestData.Input("pg", "src", [DbtTestData.Col("a")])],
                [DbtTestData.Output("pg", "out", [DbtTestData.Col("a")], new() { ["a"] = [DbtTestData.Identity("pg", "src", "a")] })]), "stream");

            var root = DbtTestData.Parse(store.GetManifest());

            Assert.Equal("my_project", root.GetProperty("metadata").GetProperty("project_name").GetString());
            var model = root.GetProperty("nodes").GetProperty("model.my_project.pg__out");
            Assert.Equal("my_project", model.GetProperty("package_name").GetString());
            Assert.Equal("my_project", model.GetProperty("fqn")[0].GetString());
            var source = root.GetProperty("sources").GetProperty("source.my_project.pg.src");
            Assert.Equal("models/my_project_sources.yml", source.GetProperty("path").GetString());
        }

        [Fact]
        public void Hash8IsStable()
        {
            Assert.Equal(8, DbtHashing.Hash8("kafka://a:9092").Length);
            Assert.Equal(DbtHashing.Hash8("a", "b"), DbtHashing.Hash8("a", "b"));
            Assert.NotEqual(DbtHashing.Hash8("a", "b"), DbtHashing.Hash8("ab"));
        }
    }
}
