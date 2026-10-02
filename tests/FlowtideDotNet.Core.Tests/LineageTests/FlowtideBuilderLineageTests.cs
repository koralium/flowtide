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

using FlowtideDotNet.Core.Engine;
using FlowtideDotNet.Core.Engine.Distributed;
using FlowtideDotNet.Core.Lineage;
using FlowtideDotNet.Core.Lineage.Dbt;
using FlowtideDotNet.Core.Lineage.Internal;
using FlowtideDotNet.Core.Tests.LineageTests.Dbt;
using FlowtideDotNet.Core.Tests.Failure;
using FlowtideDotNet.Storage.Persistence.CacheStorage;
using FlowtideDotNet.Storage.StateManager;
using FlowtideDotNet.Substrait;
using FlowtideDotNet.Substrait.Relations;
using FlowtideDotNet.Substrait.Sql;
using Microsoft.Extensions.Logging;

namespace FlowtideDotNet.Core.Tests.LineageTests
{
    public class FlowtideBuilderLineageTests
    {
        private const string LineageCategory = "FlowtideDotNet.Core.Lineage";

        private const string Url = "http://127.0.0.1:9/api/v1/lineage";

        private const string GetTimestampSql = @"
            CREATE TABLE input1 (a any, d timestamp);
            INSERT INTO output SELECT a FROM input1 WHERE d < gettimestamp();
            ";

        private class ThrowingLineageSinkFactory : FailureEgressFactory
        {
            public ThrowingLineageSinkFactory() : base("*", new FailureEgressOptions())
            {
            }

            public override TableLineageMetadata GetLineageMetadata(WriteRelation writeRelation, bool includeSchema)
            {
                throw new InvalidOperationException("lineage unavailable");
            }
        }

        private class SchemaRecordingIngressFactory : TestIngressFactory
        {
            public SchemaRecordingIngressFactory() : base("^input1$")
            {
            }

            public List<bool> IncludeSchemaCalls { get; } = new List<bool>();

            public override TableLineageMetadata GetLineageMetadata(ReadRelation readRelation, bool includeSchema)
            {
                IncludeSchemaCalls.Add(includeSchema);
                return base.GetLineageMetadata(readRelation, includeSchema);
            }
        }

        [Fact]
        public void GetTimestampPlanBuildsWithOpenLineage()
        {
            var logs = new ListLoggerProvider();
            var connectorManager = new ConnectorManager();
            // Anchored, so nothing claims the timestamp read.
            connectorManager.AddSource(new TestIngressFactory("^input1$"));
            connectorManager.AddSink(new FailureEgressFactory("*", new FailureEgressOptions()));

            var stream = CreateBuilder(GetPlan(GetTimestampSql), "olGetTimestamp", logs)
                .AddConnectorManager(connectorManager)
                .WithOpenLineageHttp(new OpenLineageHttpOptions() { Url = Url })
                .Build();

            Assert.NotNull(stream);
            Assert.DoesNotContain(logs.Entries, x => x.Category == LineageCategory);
            Assert.Contains(typeof(OpenLineageHttpReporter).FullName, logs.Categories);
        }

        [Fact]
        public void LineageFailureLogsAndBuildSucceeds()
        {
            var logs = new ListLoggerProvider();
            var connectorManager = new ConnectorManager();
            connectorManager.AddSource(new TestIngressFactory("^input1$"));
            connectorManager.AddSink(new ThrowingLineageSinkFactory());

            var stream = CreateBuilder(GetPlan(GetTimestampSql), "olLineageFailure", logs)
                .AddConnectorManager(connectorManager)
                .WithOpenLineageHttp(new OpenLineageHttpOptions() { Url = Url })
                .Build();

            Assert.NotNull(stream);
            var error = Assert.Single(logs.Entries, x => x.Category == LineageCategory);
            Assert.Equal(LogLevel.Error, error.Level);
            Assert.Equal("lineage unavailable", Assert.IsType<InvalidOperationException>(error.Exception).Message);
            Assert.DoesNotContain(typeof(OpenLineageHttpReporter).FullName, logs.Categories);
        }

        [Fact]
        public void MissingUrlStillThrows()
        {
            var connectorManager = new ConnectorManager();
            connectorManager.AddSource(new TestIngressFactory("^input1$"));
            connectorManager.AddSink(new FailureEgressFactory("*", new FailureEgressOptions()));

            var e = Assert.Throws<ArgumentException>(() =>
            {
                CreateBuilder(GetPlan(GetTimestampSql), "olMissingUrl", new ListLoggerProvider())
                    .AddConnectorManager(connectorManager)
                    .WithOpenLineageHttp(new OpenLineageHttpOptions())
                    .Build();
            });
            Assert.Equal("OpenLineageOptions.Url must be set", e.Message);
        }

        [Fact]
        public void NoConnectorManagerLogsWarning()
        {
            var logs = new ListLoggerProvider();
            var readWriteFactory = new ReadWriteFactory()
                .AddReadResolver((readRelation, functionsRegister, options) => new ReadOperatorInfo(new TestIngress(options)))
                .AddWriteResolver((writeRelation, options) => new FailureEgress(options, new FailureEgressOptions()));

#pragma warning disable CS0618 // Only the legacy factory builds without connectors.
            var stream = CreateBuilder(GetPlan(GetTimestampSql), "olNoConnectorManager", logs)
                .AddReadWriteFactory(readWriteFactory)
                .WithOpenLineageHttp(new OpenLineageHttpOptions() { Url = Url })
                .Build();
#pragma warning restore CS0618

            Assert.NotNull(stream);
            var warning = Assert.Single(logs.Entries, x => x.Category == LineageCategory);
            Assert.Equal(LogLevel.Warning, warning.Level);
            Assert.DoesNotContain(typeof(OpenLineageHttpReporter).FullName, logs.Categories);
        }

        [Fact]
        public void DbtStoreRegisteredOnceAfterBuild()
        {
            var logs = new ListLoggerProvider();
            var store = CreateStore();
            var builder = CreateBuilder(GetPlan(GetTimestampSql), "dbtRegistered", logs)
                .AddConnectorManager(CreateConnectorManager())
                .WithDbtManifestStore(store);
            Assert.Equal(0, store.Version);

            var stream = builder.Build();

            Assert.NotNull(stream);
            Assert.Equal(1, store.Version);
            Assert.DoesNotContain(logs.Entries, x => x.Category == LineageCategory);
            Assert.True(store.TryGetManifest("stream", out var manifest));
            var root = DbtTestData.Parse(manifest);
            Assert.Equal(["model.flowtide.failure__output"], DbtTestData.Keys(root.GetProperty("nodes")));
            // The internal timestamp read never becomes a source.
            Assert.Equal(["source.flowtide.test.input1"], DbtTestData.Keys(root.GetProperty("sources")));
        }

        [Fact]
        public void DbtStoreNotRegisteredWhenLineageFails()
        {
            var logs = new ListLoggerProvider();
            var store = CreateStore();
            var connectorManager = new ConnectorManager();
            connectorManager.AddSource(new TestIngressFactory("^input1$"));
            connectorManager.AddSink(new ThrowingLineageSinkFactory());

            var stream = CreateBuilder(GetPlan(GetTimestampSql), "dbtLineageFailure", logs)
                .AddConnectorManager(connectorManager)
                .WithDbtManifestStore(store)
                .Build();

            Assert.NotNull(stream);
            Assert.Equal(LogLevel.Error, Assert.Single(logs.Entries, x => x.Category == LineageCategory).Level);
            Assert.Equal(0, store.Version);
        }

        [Fact]
        public void DbtStoreNotRegisteredWhenBuildFails()
        {
            var store = CreateStore();
            var connectorManager = new ConnectorManager();
            // Lineage succeeds, creating the source throws.
            connectorManager.AddSource(new LineageTestSourceFactory());
            connectorManager.AddSink(new FailureEgressFactory("*", new FailureEgressOptions()));

            Assert.Throws<NotSupportedException>(() => CreateBuilder(GetPlan(GetTimestampSql), "dbtBuildFailure", new ListLoggerProvider())
                .AddConnectorManager(connectorManager)
                .WithDbtManifestStore(store)
                .Build());

            Assert.Equal(0, store.Version);
        }

        [Fact]
        public void DbtStoreWithoutConnectorManagerLogsWarning()
        {
            var logs = new ListLoggerProvider();
            var store = CreateStore();
            var readWriteFactory = new ReadWriteFactory()
                .AddReadResolver((readRelation, functionsRegister, options) => new ReadOperatorInfo(new TestIngress(options)))
                .AddWriteResolver((writeRelation, options) => new FailureEgress(options, new FailureEgressOptions()));

#pragma warning disable CS0618 // Only the legacy factory builds without connectors.
            var stream = CreateBuilder(GetPlan(GetTimestampSql), "dbtNoConnectorManager", logs)
                .AddReadWriteFactory(readWriteFactory)
                .WithDbtManifestStore(store)
                .Build();
#pragma warning restore CS0618

            Assert.NotNull(stream);
            Assert.Equal(LogLevel.Warning, Assert.Single(logs.Entries, x => x.Category == LineageCategory).Level);
            Assert.Equal(0, store.Version);
        }

        [Theory]
        [InlineData(true, false, true)]
        [InlineData(false, false, false)]
        [InlineData(false, true, true)]
        public void ConnectorSchemaRequestedByStoreOrOpenLineage(bool storeIncludesSchema, bool openLineageIncludesSchema, bool expected)
        {
            var source = new SchemaRecordingIngressFactory();
            var connectorManager = new ConnectorManager();
            connectorManager.AddSource(source);
            connectorManager.AddSink(new FailureEgressFactory("*", new FailureEgressOptions()));
            var options = new DbtManifestOptions() { IncludeConnectorSchema = storeIncludesSchema };

            CreateBuilder(GetPlan(GetTimestampSql), $"dbtSchema{storeIncludesSchema}{openLineageIncludesSchema}", new ListLoggerProvider())
                .AddConnectorManager(connectorManager)
                .WithDbtManifestStore(new DbtManifestStore(options))
                .WithOpenLineageHttp(new OpenLineageHttpOptions() { Url = Url, IncludeSchema = openLineageIncludesSchema })
                .Build();

            Assert.Equal([expected], source.IncludeSchemaCalls);
        }

        [Fact]
        public void DbtStoreUsesLogicalStreamName()
        {
            var store = CreateStore();
            var stream = new FlowtideBuilder("6_orders_sub1")
                .AddPlan(GetPlan(GetTimestampSql))
                .AddConnectorManager(CreateConnectorManager())
                .SetDistributedOptions(new DistributedOptions("sub1", null, new LocalSubstreamCommunicationHub().CreateFactory("sub1")))
                .WithDbtManifestStore(store)
                .WithStateOptions(new StateManagerOptions()
                {
                    PersistentStorage = new FileCachePersistentStorage(new FlowtideDotNet.Storage.FileCacheOptions()
                    {
                        DirectoryPath = "./data/tempFiles/dbtLogicalName"
                    })
                })
                .Build();

            Assert.NotNull(stream);
            Assert.True(store.TryGetManifest("orders", out _));
            Assert.False(store.TryGetManifest("6_orders_sub1", out _));
        }

        private static DbtManifestStore CreateStore()
        {
            var options = new DbtManifestOptions();
            options.ExcludedNamespaces.Clear();
            return new DbtManifestStore(options);
        }

        private static ConnectorManager CreateConnectorManager()
        {
            var connectorManager = new ConnectorManager();
            // Anchored, so nothing claims the timestamp read.
            connectorManager.AddSource(new TestIngressFactory("^input1$"));
            connectorManager.AddSink(new FailureEgressFactory("*", new FailureEgressOptions()));
            return connectorManager;
        }

        private static FlowtideBuilder CreateBuilder(Plan plan, string directoryName, ListLoggerProvider logs)
        {
            return new FlowtideBuilder("stream")
                .AddPlan(plan)
                .WithLoggerFactory(LoggerFactory.Create(b => b.AddProvider(logs)))
                .WithStateOptions(new StateManagerOptions()
                {
                    PersistentStorage = new FileCachePersistentStorage(new FlowtideDotNet.Storage.FileCacheOptions()
                    {
                        DirectoryPath = $"./data/tempFiles/{directoryName}"
                    })
                });
        }

        private static Plan GetPlan(string sql)
        {
            var builder = new SqlPlanBuilder();
            builder.Sql(sql);
            return builder.GetPlan();
        }
    }
}
