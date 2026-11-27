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
using FlowtideDotNet.Core.Lineage;
using FlowtideDotNet.Core.Tests.LateMaterialization.Differential;
using FlowtideDotNet.Core.Engine.LateMaterialization;
using FlowtideDotNet.Storage.Persistence.Reservoir;
using FlowtideDotNet.Storage.Persistence.Reservoir.Internal;
using FlowtideDotNet.Storage.Persistence.Reservoir.MemoryDisk;
using FlowtideDotNet.Storage.StateManager;
using FlowtideDotNet.Substrait;
using FlowtideDotNet.Substrait.Expressions;
using FlowtideDotNet.Substrait.FunctionExtensions;
using FlowtideDotNet.Substrait.Relations;
using FlowtideDotNet.Substrait.Type;
using System.Threading.Tasks.Dataflow;
using static FlowtideDotNet.Core.Tests.LateMaterialization.Differential.DifferentialCarrierTests;

namespace FlowtideDotNet.Core.Tests.LateMaterialization;

public partial class LateMaterializationCostTests
{
    [Fact]
    public void SecondNamesReadWholeAtAJoinSideAreReportedUnderARelationOfThePlan()
    {
        var project = new ProjectRelation()
        {
            Input = Wide("plain", 8, new StringType()),
            Expressions = [Field(1), Field(2), Field(3), Field(4)]
        };
        var first = new MergeJoinRelation()
        {
            Left = project,
            Right = Wide("d1", 0),
            Type = JoinType.Inner,
            LeftKeys = [Field(0)],
            RightKeys = [Field(project.OutputLength)],
            PostJoinFilter = new ScalarFunction() { ExtensionUrn = "extension:test:opaque", ExtensionName = "opaque", Arguments = [Field(1)] }
        };
        var plan = Write(JoinedTo(JoinedTo(first, "d2"), "d3"), 16);

        var report = Analyze(plan).report;

        Assert.NotEmpty(report.MidPlan);
        Assert.DoesNotContain(report.MidPlan, x => x.Producer == nameof(ReferenceRelation));
    }

    private sealed class LogSources : RegexConnectorSourceFactory
    {
        public LogSources() : base("*") { }

        public override IStreamIngressVertex CreateSource(ReadRelation readRelation, IFunctionsRegister functionsRegister, DataflowBlockOptions dataflowBlockOptions)
            => new DifferentialSource(readRelation, new DifferentialTable(readRelation.NamedTable.DotSeperated, readRelation.BaseSchema.Names.Count), functionsRegister);

        public override TableLineageMetadata GetLineageMetadata(ReadRelation readRelation, bool includeSchema)
            => new("test", readRelation.NamedTable.DotSeperated, default);

        public override IReadOnlyList<string>? GetRowStoreKeyColumns(ReadRelation readRelation)
            => readRelation.NamedTable.DotSeperated.StartsWith("log") ? [] : ["k"];
    }

    private static FlowtideBuilder LogBuilder(Plan plan, LateMaterializationMode mode)
    {
        var connectors = new ConnectorManager();
        connectors.AddSource(new LogSources());
        connectors.AddSink(new RunnableSinks());
        return new FlowtideBuilder("review-a-" + Guid.NewGuid().ToString("N"))
            .AddPlan(plan, false).AddConnectorManager(connectors).SetLateMaterialization(mode);
    }

    private static Plan ScatteredLog()
    {
        var exchange = new ExchangeRelation()
        {
            Input = Wide("log", 13, new StringType()),
            ExchangeKind = new ScatterExchangeKind() { Fields = [Field(0)] },
            PartitionCount = 0,
            Targets =
            [
                new StandardOutputExchangeTarget() { PartitionIds = [0] },
                new StandardOutputExchangeTarget() { PartitionIds = [1] }
            ]
        };
        var reference = new StandardOutputExchangeReferenceRelation() { RelationId = 0, TargetId = 0, ReferenceOutputLength = 14 };
        var filter = new FilterRelation()
        {
            Input = JoinedTo(JoinedTo(reference, "d1"), "d2"),
            Condition = Compare(FunctionsComparison.Equal, Field(13), Field(13))
        };
        var plan = Write(JoinedTo(filter, "d3"), 17);
        plan.Relations.Insert(0, exchange);
        return plan;
    }

    [Fact]
    public async Task ACopyThatAdmitsOtherColumnsAtTheSameHostPlacesNothing()
    {
        var plan = ScatteredLog();
        var real = Of(Analyze(plan).report, "log");
        Assert.True(real.Admitted, real.Reason);
        Assert.Contains(13, real.Columns);
        var copy = Of(Analyze(new SubstraitDeserializer().Deserialize(SubstraitSerializer.Serialize(plan))).report, "log");
        Assert.True(copy.Admitted, copy.Reason);
        Assert.DoesNotContain(13, copy.Columns);

        using var storage = new ReservoirPersistentStorage(new ReservoirStorageOptions() { FileProvider = new MemoryFileProvider() });
        var builder = LogBuilder(plan, LateMaterializationMode.Auto)
            .WithStateOptions(new StateManagerOptions() { PersistentStorage = storage });
        try
        {
            await using var stream = builder.Build();
        }
        catch (Exception)
        {
        }

        Assert.Contains("reads back differently", builder.LateMaterializationReport!.Skipped);
        Assert.False(RowStoreHostCollector.UsesRowStores(plan.Relations));
    }
}
