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
using FlowtideDotNet.Core.Compute.Internal;
using FlowtideDotNet.Core.Connectors;
using FlowtideDotNet.Core.Engine;
using FlowtideDotNet.Core.Engine.LateMaterialization;
using FlowtideDotNet.Core.Lineage;
using FlowtideDotNet.Core.Optimizer.MergeJoinParallelize;
using FlowtideDotNet.Storage.Persistence.Reservoir;
using FlowtideDotNet.Storage.Persistence.Reservoir.Internal;
using FlowtideDotNet.Storage.Persistence.Reservoir.MemoryDisk;
using FlowtideDotNet.Storage.StateManager;
using FlowtideDotNet.Substrait;
using FlowtideDotNet.Substrait.FunctionExtensions;
using FlowtideDotNet.Substrait.Relations;
using FlowtideDotNet.Substrait.Type;
using System.Threading.Tasks.Dataflow;
using static FlowtideDotNet.Core.Tests.LateMaterialization.Differential.DifferentialCarrierTests;

namespace FlowtideDotNet.Core.Tests.LateMaterialization;

public class ReviewerARound2ReaderCountTests
{
    private sealed class Sources : RegexConnectorSourceFactory
    {
        public Sources() : base("*")
        {
        }

        public override IStreamIngressVertex CreateSource(ReadRelation readRelation, IFunctionsRegister functionsRegister, DataflowBlockOptions dataflowBlockOptions)
            => throw new NotSupportedException("no source in this test");

        public override TableLineageMetadata GetLineageMetadata(ReadRelation readRelation, bool includeSchema)
            => new("test", readRelation.NamedTable.DotSeperated, default);

        public override IReadOnlyList<string>? GetRowStoreKeyColumns(ReadRelation readRelation)
            => ["k"];
    }

    private sealed class Sinks : RegexConnectorSinkFactory
    {
        private readonly IReadOnlyList<int>? _keys;

        public Sinks(IReadOnlyList<int>? keys = null) : base("*")
        {
            _keys = keys;
        }

        public override IReadOnlyList<int>? GetRowStoreFetchKeyColumns(WriteRelation writeRelation) => _keys;

        public override IStreamEgressVertex CreateSink(WriteRelation writeRelation, IFunctionsRegister functionsRegister, ExecutionDataflowBlockOptions dataflowBlockOptions)
            => throw new NotSupportedException("no sink in this test");

        public override TableLineageMetadata GetLineageMetadata(WriteRelation writeRelation, bool includeSchema)
            => new("test", writeRelation.NamedObject.DotSeperated, default);
    }

    private static ReadRelation Wide(string name, int payload)
    {
        var names = new List<string>() { "k" };
        var types = new List<SubstraitBaseType>() { new Int64Type() };
        for (int i = 0; i < payload; i++)
        {
            names.Add("c" + i);
            types.Add(new StringType());
        }
        return new ReadRelation()
        {
            NamedTable = new NamedTable() { Names = [name] },
            BaseSchema = new NamedStruct() { Names = names, Struct = new Struct() { Types = types } }
        };
    }

    private static Relation JoinedTo(Relation left, string table, int payload = 0)
    {
        return new MergeJoinRelation()
        {
            Left = left,
            Right = Wide(table, payload),
            Type = JoinType.Inner,
            LeftKeys = [Field(0)],
            RightKeys = [Field(left.OutputLength)]
        };
    }

    private static Plan PartitionedPlan(int partitions, int dimensionPayload = 0)
    {
        var chain = JoinedTo(JoinedTo(JoinedTo(Wide("fact", 8), "d1", dimensionPayload), "d2", dimensionPayload), "d3", dimensionPayload);
        return ParallelizeOptimizer.Optimize(Write(chain, chain.OutputLength), partitions);
    }

    private static ConnectorManager Connectors(IReadOnlyList<int>? sinkKeys = null)
    {
        var connectors = new ConnectorManager();
        connectors.AddSource(new Sources());
        connectors.AddSink(new Sinks(sinkKeys));
        return connectors;
    }

    private static FunctionsRegister Functions()
    {
        var functions = new FunctionsRegister();
        BuiltinFunctions.RegisterFunctions(functions);
        return functions;
    }

    [Fact]
    public void ASinkKeyFetchUnderAFilterThatReadsIsAReaderOfItsOwn()
    {
        var chain = JoinedTo(JoinedTo(JoinedTo(Wide("fact", 8), "d1"), "d2"), "d3");
        var filter = new FilterRelation() { Input = chain, Condition = Compare(FunctionsComparison.Equal, Field(1), Field(1)) };
        var plan = Write(filter, filter.OutputLength);
        var connectors = Connectors([2]);
        var functions = Functions();

        var report = LateMaterializationPass.Analyze(plan, connectors, functions, 1);
        Assert.True(report.Admitted > 0, report.ToString());
        int counted = report.Hosts.Where(x => x.Admitted).Sum(x => x.Fetches);
        LateMaterializationPass.Run(plan, connectors, functions, 1, report.Selection());
        RowStoreHostCollector.Count(plan.Relations, out _, out int readers);

        Assert.Equal(readers, counted);
    }

    [Theory]
    [InlineData(LateMaterializationMode.Off, 8, 0, 200)]
    [InlineData(LateMaterializationMode.Auto, 8, 0, 200)]
    [InlineData(LateMaterializationMode.Off, 16, 8, 1000)]
    [InlineData(LateMaterializationMode.Auto, 16, 8, 1000)]
    public void AutoBuildsAPartitionedPlanWhereverOffBuildsIt(LateMaterializationMode mode, int partitions, int dimensionPayload, int cachePages)
    {
        using var storage = new ReservoirPersistentStorage(new ReservoirStorageOptions() { FileProvider = new MemoryFileProvider() });
        var builder = new FlowtideBuilder("reader-count-" + mode)
            .AddPlan(PartitionedPlan(partitions, dimensionPayload), false)
            .AddConnectorManager(Connectors())
            .WithStateOptions(new StateManagerOptions() { CachePageCount = cachePages, PersistentStorage = storage })
            .SetLateMaterialization(mode);

        var error = Record.Exception(() => builder.Build());

        Assert.True(error is NotSupportedException, $"{mode}: {error}; {builder.LateMaterializationReport}");
    }
}
