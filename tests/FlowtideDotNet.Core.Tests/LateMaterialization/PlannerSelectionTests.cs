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
using FlowtideDotNet.Core.Engine.LateMaterialization;
using FlowtideDotNet.Core.Lineage;
using FlowtideDotNet.Core.Optimizer.MergeJoinParallelize;
using FlowtideDotNet.Substrait;
using FlowtideDotNet.Substrait.Relations;
using FlowtideDotNet.Substrait.Type;
using System.Threading.Tasks.Dataflow;
using static FlowtideDotNet.Core.Tests.LateMaterialization.Differential.DifferentialCarrierTests;

namespace FlowtideDotNet.Core.Tests.LateMaterialization;

public class PlannerSelectionTests
{
    private sealed class Sources : RegexConnectorSourceFactory
    {
        public Sources() : base("*")
        {
        }

        public override IStreamIngressVertex CreateSource(ReadRelation readRelation, IFunctionsRegister functionsRegister, DataflowBlockOptions dataflowBlockOptions)
            => throw new NotSupportedException();

        public override TableLineageMetadata GetLineageMetadata(ReadRelation readRelation, bool includeSchema)
            => new("test", readRelation.NamedTable.DotSeperated, default);

        public override IReadOnlyList<string>? GetRowStoreKeyColumns(ReadRelation readRelation)
            => ["k"];
    }

    private sealed class Sinks : RegexConnectorSinkFactory
    {
        public Sinks() : base("*")
        {
        }

        public override IStreamEgressVertex CreateSink(WriteRelation writeRelation, IFunctionsRegister functionsRegister, ExecutionDataflowBlockOptions dataflowBlockOptions)
            => throw new NotSupportedException();

        public override TableLineageMetadata GetLineageMetadata(WriteRelation writeRelation, bool includeSchema)
            => new("test", writeRelation.NamedObject.DotSeperated, default);
    }

    private static ReadRelation Key(string name)
    {
        return new ReadRelation()
        {
            NamedTable = new NamedTable() { Names = [name] },
            BaseSchema = new NamedStruct() { Names = ["k"], Struct = new Struct() { Types = [new Int64Type()] } }
        };
    }

    private static MergeJoinRelation Join(Relation left, string table, int leftKey)
    {
        return new MergeJoinRelation()
        {
            Left = left,
            Right = Key(table),
            Type = JoinType.Inner,
            LeftKeys = [Field(leftKey)],
            RightKeys = [Field(left.OutputLength)]
        };
    }

    [Fact]
    public void ANarrowColumnReadAfterOneCopyIsNotDeferredForAFetchOfItsOwn()
    {
        var names = new List<string>() { "k", "j" };
        var types = new List<SubstraitBaseType>() { new Int64Type(), new Int64Type() };
        for (int i = 0; i < 8; i++)
        {
            names.Add("s" + i);
            types.Add(new StringType());
        }
        var fact = new ReadRelation()
        {
            NamedTable = new NamedTable() { Names = ["fact"] },
            BaseSchema = new NamedStruct() { Names = names, Struct = new Struct() { Types = types } }
        };
        var second = Join(Join(fact, "d1", 0), "d2", 1);
        var plan = Write(Join(Join(second, "d3", 0), "d4", 0), 14);

        var connectors = new ConnectorManager();
        connectors.AddSource(new Sources());
        connectors.AddSink(new Sinks());
        var functions = new FunctionsRegister();
        BuiltinFunctions.RegisterFunctions(functions);
        var report = LateMaterializationPass.Analyze(plan, connectors, functions, 1);
        var host = Assert.Single(report.Hosts, x => x.Table == "fact");
        Assert.True(host.Admitted, host.Reason);
        Assert.DoesNotContain(1, host.Columns);
        Assert.Equal(1, host.Fetches);

        LateMaterializationPass.Run(plan, connectors, functions, 1, report.Selection());
        RowReferencePlanValidator.Validate(plan.Relations, functions, 1);
        Assert.IsNotType<FetchRequiredColumnsRelation>(second.Left);
    }

    private static (LateMaterializationReport report, ConnectorManager connectors, FunctionsRegister functions) Analyze(Plan plan)
    {
        var connectors = new ConnectorManager();
        connectors.AddSource(new Sources());
        connectors.AddSink(new Sinks());
        var functions = new FunctionsRegister();
        BuiltinFunctions.RegisterFunctions(functions);
        return (LateMaterializationPass.Analyze(plan, connectors, functions, 1), connectors, functions);
    }

    private static Plan OneStringThroughOneJoin()
    {
        var fact = new ReadRelation()
        {
            NamedTable = new NamedTable() { Names = ["fact"] },
            BaseSchema = new NamedStruct() { Names = ["k", "s"], Struct = new Struct() { Types = [new Int64Type(), new StringType()] } }
        };
        return Write(Join(fact, "d1", 0), 3);
    }

    [Theory]
    [InlineData(1)]
    [InlineData(8)]
    public void APartitionedJoinStoresEachRowOnceWhateverThePartitionCount(int partitions)
    {
        var plan = OneStringThroughOneJoin();
        if (partitions > 1)
        {
            ParallelizeOptimizer.Optimize(plan, partitions);
            Assert.Contains(plan.Relations, x => x is ExchangeRelation);
        }
        var (report, _, _) = Analyze(plan);

        var host = Assert.Single(report.Hosts, x => x.Table == "fact");
        Assert.Equal(1, host.Copies);
        Assert.Equal(32, host.Saving);
        Assert.False(host.Admitted);
    }
}
