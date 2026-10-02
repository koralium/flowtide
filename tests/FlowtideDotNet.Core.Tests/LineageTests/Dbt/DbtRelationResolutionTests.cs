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

using FlowtideDotNet.Core.Lineage.Dbt;
using FlowtideDotNet.Core.Lineage.Dbt.Internal;

namespace FlowtideDotNet.Core.Tests.LineageTests.Dbt
{
    public class DbtRelationResolutionTests
    {
        private static DbtTableIdentity? Resolve(string ns, string tableName, Action<DbtManifestOptions>? configure = null)
        {
            var options = new DbtManifestOptions();
            configure?.Invoke(options);
            return new DbtRelationResolution(DbtGeneratorSettings.Create(options)).Resolve(ns, tableName, tableName.Split('.'));
        }

        [Theory]
        [InlineData("t", "db", "sch", "t")]
        [InlineData("s.t", "db", "s", "t")]
        [InlineData("d.s.t", "d", "s", "t")]
        [InlineData("a.d.s.t", "a.d", "s", "t")]
        [InlineData("x.a.d.s.t", "x.a.d", "s", "t")]
        public void NamePartsMapByCount(string tableName, string database, string schema, string identifier)
        {
            var identity = Resolve("pg", tableName, o => o.MapNamespace("pg", "db", "sch"));

            Assert.Equal(new DbtTableIdentity("pg", database, schema, identifier), identity);
        }

        [Fact]
        public void UnmappedOnePartHasNoDatabaseOrSchema()
        {
            Assert.Equal(new DbtTableIdentity("pg", null, "", "t"), Resolve("pg", "t"));
        }

        [Fact]
        public void MssqlDefaultsToDbo()
        {
            Assert.Equal(new DbtTableIdentity("mssql", null, "dbo", "orders"), Resolve("mssql", "orders"));
            Assert.Equal(new DbtTableIdentity("mssql", "shop", "dbo", "orders"), Resolve("mssql", "orders", o => o.MapNamespace("mssql", "shop")));
            // Empty schema overrides the built in default.
            Assert.Equal(new DbtTableIdentity("mssql", null, "", "orders"), Resolve("mssql", "orders", o => o.MapNamespace("mssql", defaultSchema: "")));
        }

        [Fact]
        public void ElasticsearchAndKafkaAreFlat()
        {
            Assert.Equal(new DbtTableIdentity("elasticsearch", null, "", "logs.2026"), Resolve("elasticsearch", "logs.2026"));
            Assert.Equal(new DbtTableIdentity("kafka://b1:9092", null, "", "orders.v1"), Resolve("kafka://b1:9092", "orders.v1"));
            Assert.Equal(new DbtTableIdentity("kafka", "cluster", "", "a.b.c"), Resolve("kafka", "a.b.c", o => o.MapNamespace("kafka", "cluster")));
        }

        [Fact]
        public void ShortNamespaceMappingApplies()
        {
            Assert.Equal(new DbtTableIdentity("pg://host:5432", "db", "s", "t"), Resolve("pg://host:5432", "s.t", o => o.MapNamespace("pg", "db")));
            // Full namespace wins over the short one.
            Assert.Equal(new DbtTableIdentity("pg://host:5432", "full", "s", "t"), Resolve("pg://host:5432", "s.t", o => o.MapNamespace("pg", "db").MapNamespace("pg://host:5432", "full")));
        }

        [Fact]
        public void ResolverOverridesDefault()
        {
            DbtRelationContext? seen = null;
            var identity = Resolve("mssql", "s.t", o =>
            {
                o.MapNamespace("mssql", "shop");
                o.RelationResolver = context =>
                {
                    seen = context;
                    return new DbtRelation("dwh", context.DefaultRelation.Schema, context.DefaultRelation.Identifier.ToUpperInvariant());
                };
            });

            Assert.Equal(new DbtTableIdentity("mssql", "dwh", "s", "T"), identity);
            Assert.NotNull(seen);
            Assert.Equal("mssql", seen.Namespace);
            Assert.Equal("s.t", seen.TableName);
            Assert.Equal(["s", "t"], seen.NameParts);
            Assert.Equal(("shop", "s", "t"), (seen.DefaultRelation.Database, seen.DefaultRelation.Schema, seen.DefaultRelation.Identifier));
        }

        [Fact]
        public void ResolverNullKeepsDefault()
        {
            Assert.Equal(new DbtTableIdentity("mssql", null, "dbo", "t"), Resolve("mssql", "t", o => o.RelationResolver = _ => null));
        }

        [Fact]
        public void ResolverThrowIsWrapped()
        {
            var e = Assert.Throws<InvalidOperationException>(() => Resolve("mssql", "orders", o => o.RelationResolver = _ => throw new FormatException("boom")));

            Assert.Contains("'mssql'", e.Message);
            Assert.Contains("'orders'", e.Message);
            Assert.IsType<FormatException>(e.InnerException);
        }

        [Fact]
        public void ExcludedNamespaceNeverCallsResolver()
        {
            var calls = 0;
            void Configure(DbtManifestOptions o)
            {
                o.ExcludedNamespaces.Add("kafka");
                o.RelationResolver = _ =>
                {
                    calls++;
                    return null;
                };
            }

            Assert.Null(Resolve("console", "out", Configure));
            Assert.Null(Resolve("TEST", "out", Configure));
            Assert.Null(Resolve("kafka://b1:9092", "topic", Configure));
            Assert.Equal(0, calls);
            Assert.NotNull(Resolve("pg", "t", Configure));
            Assert.Equal(1, calls);
        }

        [Fact]
        public void RelationNormalizesParts()
        {
            var relation = new DbtRelation("", null, "t");

            Assert.Null(relation.Database);
            Assert.Equal("", relation.Schema);
            Assert.Throws<ArgumentException>(() => new DbtRelation("d", "s", ""));
        }
    }
}
