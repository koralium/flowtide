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
using FlowtideDotNet.Substrait;
using FlowtideDotNet.Substrait.Expressions;
using FlowtideDotNet.Substrait.FunctionExtensions;
using FlowtideDotNet.Substrait.Relations;
using FlowtideDotNet.Substrait.Type;
using System.Threading.Tasks.Dataflow;
using static FlowtideDotNet.Core.Tests.LateMaterialization.Differential.DifferentialCarrierTests;

namespace FlowtideDotNet.Core.Tests.LateMaterialization;

public class PartitionedJoinPassTests
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

    private static FunctionsRegister Functions()
    {
        var functions = new FunctionsRegister();
        BuiltinFunctions.RegisterFunctions(functions);
        functions.RegisterScalarFunctionWithExpression(
            "extension:test:rows", "is_odd", x => FlexBuffers.FlxValue.FromBytes(FlexBuffers.FlexBuffer.SingleValue(x.AsLong % 2 == 1)));
        return functions;
    }

    private static int Run(Plan plan, int parallelism = 1)
    {
        var connectors = new ConnectorManager();
        connectors.AddSource(new Sources());
        connectors.AddSink(new Sinks());
        var functions = Functions();
        int stores = LateMaterializationPass.Run(plan, connectors, functions, parallelism);
        RowReferencePlanValidator.Validate(plan.Relations, functions, parallelism);
        return stores;
    }

    // l joined with r on l.n = r.k, in two partitions
    private static Plan PartitionedJoin(bool rowOnlyPostFilter)
    {
        var plan = Join(false, JoinType.Inner);
        var join = (MergeJoinRelation)((WriteRelation)plan.Relations[0]).Input;
        if (rowOnlyPostFilter)
        {
            join.PostJoinFilter = new ScalarFunction() { ExtensionUrn = "extension:test:rows", ExtensionName = "is_odd", Arguments = [Field(0)] };
        }
        return Core.Optimizer.MergeJoinParallelize.ParallelizeOptimizer.Optimize(plan, 2);
    }

    [Fact]
    public void APartitionedJoinWithARowOnlyPostFilterBuilds()
    {
        var plan = PartitionedJoin(rowOnlyPostFilter: true);
        var exception = Record.Exception(() => Run(plan));
        Assert.Null(exception);
    }

    [Fact]
    public void APartitionedJoinThatReadsEveryStoredLeftColumnBuilds()
    {
        var plan = Join(false, JoinType.Inner);
        var join = (MergeJoinRelation)((WriteRelation)plan.Relations[0]).Input;
        join.PostJoinFilter = new ScalarFunction()
        {
            ExtensionUrn = FunctionsBoolean.Urn,
            ExtensionName = FunctionsBoolean.And,
            Arguments = [Compare(FunctionsComparison.GreaterThan, Field(2), Field(6)), Compare(FunctionsComparison.GreaterThan, Field(3), Field(7))]
        };
        plan = Core.Optimizer.MergeJoinParallelize.ParallelizeOptimizer.Optimize(plan, 2);
        var exception = Record.Exception(() => Run(plan));
        Assert.Null(exception);
    }

    [Fact]
    public void AnExchangeUnderASubstreamRootStaysTheRootsInput()
    {
        var exchange = new ExchangeRelation()
        {
            Input = Read("t", false),
            ExchangeKind = new ScatterExchangeKind() { Fields = [Field(0)] },
            Targets = [new StandardOutputExchangeTarget() { PartitionIds = [0] }],
            PartitionCount = 1
        };
        var plan = new Plan()
        {
            Relations =
            [
                new SubStreamRootRelation() { Name = "a", Input = exchange },
                new SubStreamRootRelation()
                {
                    Name = "a",
                    Input = new WriteRelation()
                    {
                        Input = new StandardOutputExchangeReferenceRelation() { RelationId = 0, TargetId = 0, ReferenceOutputLength = 4 },
                        NamedObject = new NamedTable() { Names = ["output"] },
                        TableSchema = new NamedStruct() { Names = ["c0", "c1", "c2", "c3"] }
                    }
                }
            ]
        };
        Run(plan);
        Assert.IsType<ExchangeRelation>(((SubStreamRootRelation)plan.Relations[0]).Input);
    }

    [Fact]
    public void APartitionedJoinUnderBuilderParallelismBuilds()
    {
        var plan = PartitionedJoin(rowOnlyPostFilter: false);
        var exception = Record.Exception(() => Run(plan, parallelism: 2));
        Assert.Null(exception);
    }
}
