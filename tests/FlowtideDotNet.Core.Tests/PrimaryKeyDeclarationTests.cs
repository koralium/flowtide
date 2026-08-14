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

using FlowtideDotNet.Base.Vertices;
using FlowtideDotNet.Core.Compute;
using FlowtideDotNet.Core.Connectors;
using FlowtideDotNet.Core.Engine;
using FlowtideDotNet.Core.Exceptions;
using FlowtideDotNet.Core.Lineage;
using FlowtideDotNet.Core.Sinks;
using FlowtideDotNet.Core.Tests.Failure;
using FlowtideDotNet.Storage.Persistence.CacheStorage;
using FlowtideDotNet.Substrait;
using FlowtideDotNet.Substrait.Relations;
using FlowtideDotNet.Substrait.Sql;
using FlowtideDotNet.Substrait.Type;
using System.Threading.Tasks.Dataflow;

namespace FlowtideDotNet.Core.Tests
{
    /// <summary>
    /// Covers plan declared primary keys and the sink check.
    /// </summary>
    public class PrimaryKeyDeclarationTests
    {
        private class PrimaryKeyAwareSinkFactory : RegexConnectorSinkFactory
        {
            public PrimaryKeyAwareSinkFactory(string regexPattern) : base(regexPattern)
            {
            }

            public override bool SupportsPrimaryKeyDeclaration => true;

            public WriteRelation? LastWriteRelation { get; private set; }

            public override IStreamEgressVertex CreateSink(WriteRelation writeRelation, IFunctionsRegister functionsRegister, ExecutionDataflowBlockOptions dataflowBlockOptions)
            {
                LastWriteRelation = writeRelation;
                return new FailureEgress(dataflowBlockOptions, new FailureEgressOptions());
            }

            public override TableLineageMetadata GetLineageMetadata(WriteRelation writeRelation, bool includeSchema)
            {
                return new TableLineageMetadata("test", writeRelation.NamedObject.DotSeperated, default);
            }
        }

        private static FlowtideBuilder CreateBuilder(Plan plan, ConnectorManager connectorManager, string directoryName)
        {
            return new FlowtideBuilder("test")
                .AddPlan(plan)
                .AddConnectorManager(connectorManager)
                .WithStateOptions(new FlowtideDotNet.Storage.StateManager.StateManagerOptions()
                {
                    PersistentStorage = new FileCachePersistentStorage(new FlowtideDotNet.Storage.FileCacheOptions()
                    {
                        DirectoryPath = $"./data/tempFiles/{directoryName}"
                    })
                });
        }

        private static Plan GetPlan(string sql)
        {
            SqlPlanBuilder builder = new SqlPlanBuilder();
            builder.AddTableDefinition("a", new NamedStruct()
            {
                Names = new List<string>() { "c1", "c2" },
                Struct = new Struct()
                {
                    Types = new List<SubstraitBaseType>() { new AnyType(), new AnyType() }
                }
            });
            builder.Sql(sql);
            return builder.GetPlan();
        }

        [Fact]
        public void SinkThatDoesNotSupportPrimaryKeyDeclarationThrows()
        {
            var plan = GetPlan("INSERT INTO test PRIMARY KEY (c1) SELECT c1, c2 FROM a");

            var connectorManager = new ConnectorManager();
            connectorManager.AddSource(new FailureIngressFactory("*"));
            connectorManager.AddConsoleSink(".*");

            var e = Assert.Throws<FlowtidePrimaryKeyDeclarationNotSupportedException>(() =>
            {
                CreateBuilder(plan, connectorManager, "pkNotSupported").Build();
            });

            Assert.Equal("The connector writing to 'test' cannot use primary keys declared in the plan. Remove the 'PRIMARY KEY' declaration from the insert statement, the connector would otherwise write the data with other primary keys than the declared ones.", e.Message);
        }

        [Fact]
        public void SinkThatSupportsPrimaryKeyDeclarationReceivesTheNames()
        {
            var plan = GetPlan("INSERT INTO test PRIMARY KEY (c2, c1) SELECT c1, c2 FROM a");

            var sinkFactory = new PrimaryKeyAwareSinkFactory(".*");
            var connectorManager = new ConnectorManager();
            connectorManager.AddSource(new FailureIngressFactory("*"));
            connectorManager.AddSink(sinkFactory);

            CreateBuilder(plan, connectorManager, "pkSupported").Build();

            Assert.NotNull(sinkFactory.LastWriteRelation);
            Assert.Equal(new List<string>() { "c2", "c1" }, sinkFactory.LastWriteRelation.PrimaryKeyNames);
        }

        /// <summary>
        /// The catalog wrapper must forward what the inner factory supports.
        /// </summary>
        [Fact]
        public void SinkInCatalogReceivesThePrimaryKeyNames()
        {
            var plan = GetPlan("INSERT INTO mycatalog.test PRIMARY KEY (c2) SELECT c1, c2 FROM a");

            var sinkFactory = new PrimaryKeyAwareSinkFactory(".*");
            var connectorManager = new ConnectorManager();
            connectorManager.AddSource(new FailureIngressFactory("*"));
            connectorManager.AddCatalog("mycatalog", catalog =>
            {
                catalog.AddSink(sinkFactory);
            });

            CreateBuilder(plan, connectorManager, "pkCatalog").Build();

            Assert.NotNull(sinkFactory.LastWriteRelation);
            Assert.Equal(new List<string>() { "c2" }, sinkFactory.LastWriteRelation.PrimaryKeyNames);
        }

        [Fact]
        public void SinkWithoutPrimaryKeyDeclarationIsNotAffected()
        {
            var plan = GetPlan("INSERT INTO test SELECT c1, c2 FROM a");

            var connectorManager = new ConnectorManager();
            connectorManager.AddSource(new FailureIngressFactory("*"));
            connectorManager.AddConsoleSink(".*");

            CreateBuilder(plan, connectorManager, "pkNone").Build();
        }
    }
}
