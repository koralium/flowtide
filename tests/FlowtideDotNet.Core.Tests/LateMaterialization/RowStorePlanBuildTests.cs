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

using FlowtideDotNet.Base;
using FlowtideDotNet.Base.Vertices;
using FlowtideDotNet.Core.Compute;
using FlowtideDotNet.Core.Connectors;
using FlowtideDotNet.Core.Engine;
using FlowtideDotNet.Core.Lineage;
using FlowtideDotNet.Core.Operators.Read;
using FlowtideDotNet.Core.Sinks;
using FlowtideDotNet.Core.Tests.Failure;
using FlowtideDotNet.Storage.Persistence.CacheStorage;
using FlowtideDotNet.Substrait;
using FlowtideDotNet.Substrait.Relations;
using FlowtideDotNet.Substrait.Type;
using System.Runtime.CompilerServices;
using System.Threading.Tasks.Dataflow;

namespace FlowtideDotNet.Core.Tests.LateMaterialization
{
    public class RowStorePlanBuildTests
    {
        private sealed class HostingSource : ColumnBatchReadBaseOperator
        {
            public HostingSource(ReadRelation readRelation, IFunctionsRegister functionsRegister, DataflowBlockOptions options)
                : base(readRelation, functionsRegister, options)
            {
            }

            public override string DisplayName => "hosting source";

            public override Task Compact() => Task.CompletedTask;

            protected override Task Checkpoint(long checkpointTime) => Task.CompletedTask;

            protected override Task<IReadOnlySet<string>> GetWatermarkNames() => Task.FromResult<IReadOnlySet<string>>(new HashSet<string>() { "t" });

            protected override ValueTask<List<int>> GetPrimaryKeyColumns() => ValueTask.FromResult(new List<int>() { 0 });

            protected override async IAsyncEnumerable<ColumnReadEvent> FullLoad(CancellationToken cancellationToken, [EnumeratorCancellation] CancellationToken enumeratorCancellationToken = default)
            {
                await Task.CompletedTask;
                yield break;
            }

            protected override async IAsyncEnumerable<DeltaReadEvent> DeltaLoad(Func<Task> EnterCheckpointLock, Action ExitCheckpointLock, CancellationToken cancellationToken, [EnumeratorCancellation] CancellationToken enumeratorCancellationToken = default)
            {
                await Task.CompletedTask;
                yield break;
            }
        }

        private sealed class HostingSourceFactory : RegexConnectorSourceFactory
        {
            public HostingSourceFactory(string regexPattern) : base(regexPattern)
            {
            }

            public override IStreamIngressVertex CreateSource(ReadRelation readRelation, IFunctionsRegister functionsRegister, DataflowBlockOptions dataflowBlockOptions)
            {
                return new HostingSource(readRelation, functionsRegister, dataflowBlockOptions);
            }

            public override TableLineageMetadata GetLineageMetadata(ReadRelation readRelation, bool includeSchema)
            {
                return new TableLineageMetadata("test", readRelation.NamedTable.DotSeperated, default);
            }
        }

        private static ReadRelation Read(string? store)
        {
            return new ReadRelation()
            {
                BaseSchema = new NamedStruct()
                {
                    Names = ["k", "kept", "text"],
                    Struct = new Struct() { Types = [new AnyType(), new AnyType(), new AnyType()] }
                },
                NamedTable = new NamedTable() { Names = ["t"] },
                RowStore = store == null ? null : new RowStoreDefinition() { Name = store, StoredColumns = [2] }
            };
        }

        private static Plan Plan(Relation input)
        {
            var names = new List<string>();
            for (int i = 0; i < input.OutputLength; i++)
            {
                names.Add("c" + i);
            }
            return new Plan()
            {
                Relations =
                [
                    new WriteRelation()
                    {
                        Input = input,
                        NamedObject = new NamedTable() { Names = ["output"] },
                        TableSchema = new NamedStruct() { Names = names }
                    }
                ]
            };
        }

        private static FetchRequiredColumnsRelation Fetch(Relation input, string store)
        {
            return new FetchRequiredColumnsRelation()
            {
                Input = input,
                Fetches = [new RowStoreFetch() { ReferenceColumn = 2, StoreName = store, Columns = [0] }],
                Emit = [0, 1, 3]
            };
        }

        private static FlowtideBuilder Builder(Plan plan, bool hostingSource, string directoryName)
        {
            var connectorManager = new ConnectorManager();
            if (hostingSource)
            {
                connectorManager.AddSource(new HostingSourceFactory("*"));
            }
            else
            {
                connectorManager.AddSource(new FailureIngressFactory("*"));
            }
            connectorManager.AddConsoleSink(".*");
            return new FlowtideBuilder("test")
                .AddPlan(plan, false)
                .AddConnectorManager(connectorManager)
                .WithStateOptions(new FlowtideDotNet.Storage.StateManager.StateManagerOptions()
                {
                    PersistentStorage = new FileCachePersistentStorage(new FlowtideDotNet.Storage.FileCacheOptions()
                    {
                        DirectoryPath = $"./data/tempFiles/{directoryName}"
                    })
                });
        }

        [Fact]
        public void APlanWithAStoreAndAFetchBuilds()
        {
            var stream = Builder(Plan(Fetch(Read("rows"), "rows")), true, "rowStoreBuilds").Build();
            Assert.NotNull(stream);
        }

        [Fact]
        public void AStoreOnASourceThatCannotHostOneIsRefused()
        {
            var exception = Assert.Throws<NotSupportedException>(() => Builder(Plan(Fetch(Read("rows"), "rows")), false, "rowStoreNoHost").Build());
            Assert.Equal("The read of 't' asks for row store 'rows', its operator 'FailureIngress' cannot keep columns in a row store.", exception.Message);
        }

        [Fact]
        public void AReferenceThatReachesTheSinkIsRefusedAtBuild()
        {
            var exception = Assert.Throws<InvalidOperationException>(() => Builder(Plan(Read("rows")), true, "rowStoreToSink").Build());
            Assert.Equal("A Write relation gets row references of row store 'rows' in column 2 of an input, it cannot take them. Read the stored columns with a fetch before it.", exception.Message);
        }

        [Fact]
        public void AFetchFromAStoreNobodyHostsIsRefusedAtBuild()
        {
            var exception = Assert.Throws<InvalidOperationException>(() => Builder(Plan(Fetch(Read("rows"), "other")), true, "rowStoreUnknown").Build());
            Assert.Equal("A fetch reads from row store 'other', no relation of the plan hosts it.", exception.Message);
        }
    }
}
