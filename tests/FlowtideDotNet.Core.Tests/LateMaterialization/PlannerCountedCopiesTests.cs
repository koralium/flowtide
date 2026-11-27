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
using FlowtideDotNet.Substrait.Expressions.Literals;
using FlowtideDotNet.Substrait.FunctionExtensions;
using FlowtideDotNet.Substrait.Relations;
using FlowtideDotNet.Substrait.Type;
using System.Threading.Tasks.Dataflow;
using static FlowtideDotNet.Core.Tests.LateMaterialization.Differential.DifferentialCarrierTests;

namespace FlowtideDotNet.Core.Tests.LateMaterialization;

public class PlannerCountedCopiesTests
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
        private readonly IReadOnlyList<int>? _keys;

        public Sinks(IReadOnlyList<int>? keys) : base("*")
        {
            _keys = keys;
        }

        public override IStreamEgressVertex CreateSink(WriteRelation writeRelation, IFunctionsRegister functionsRegister, ExecutionDataflowBlockOptions dataflowBlockOptions)
            => throw new NotSupportedException();

        public override TableLineageMetadata GetLineageMetadata(WriteRelation writeRelation, bool includeSchema)
            => new("test", writeRelation.NamedObject.DotSeperated, default);

        public override IReadOnlyList<int>? GetRowStoreFetchKeyColumns(WriteRelation writeRelation)
            => _keys;
    }

    private static ReadRelation Wide(string name, int payload, SubstraitBaseType? type = null)
    {
        var names = new List<string>() { "k" };
        var types = new List<SubstraitBaseType>() { new Int64Type() };
        for (int i = 0; i < payload; i++)
        {
            names.Add("c" + i);
            types.Add(type ?? new AnyType());
        }
        return new ReadRelation()
        {
            NamedTable = new NamedTable() { Names = [name] },
            BaseSchema = new NamedStruct() { Names = names, Struct = new Struct() { Types = types } }
        };
    }

    private static ReadRelation KeyLast(string name, int payload)
    {
        var names = new List<string>();
        var types = new List<SubstraitBaseType>();
        for (int i = 0; i < payload; i++)
        {
            names.Add("c" + i);
            types.Add(new StringType());
        }
        names.Add("k");
        types.Add(new Int64Type());
        return new ReadRelation()
        {
            NamedTable = new NamedTable() { Names = [name] },
            BaseSchema = new NamedStruct() { Names = names, Struct = new Struct() { Types = types } }
        };
    }

    private static Relation JoinedTo(Relation left, string table)
    {
        return new MergeJoinRelation()
        {
            Left = left,
            Right = Wide(table, 0),
            Type = JoinType.Inner,
            LeftKeys = [Field(0)],
            RightKeys = [Field(left.OutputLength)]
        };
    }

    private static SetRelation UnionOfOneRelation(int length, int key)
    {
        return new SetRelation()
        {
            Operation = SetOperation.UnionAll,
            Inputs =
            [
                new ReferenceRelation() { RelationId = 1, ReferenceOutputLength = length },
                new FilterRelation()
                {
                    Input = new ReferenceRelation() { RelationId = 1, ReferenceOutputLength = length },
                    Condition = Compare(FunctionsComparison.GreaterThan, Field(key), new NumericLiteral() { Value = 1 }),
                    Emit = Enumerable.Range(0, length).ToList()
                }
            ]
        };
    }

    private static (LateMaterializationReport report, ConnectorManager connectors, FunctionsRegister functions) Analyze(Plan plan, IReadOnlyList<int>? sinkKeys)
    {
        var connectors = new ConnectorManager();
        connectors.AddSource(new Sources());
        connectors.AddSink(new Sinks(sinkKeys));
        var functions = new FunctionsRegister();
        BuiltinFunctions.RegisterFunctions(functions);
        var report = LateMaterializationPass.Analyze(plan, connectors, functions, 1);
        return (report, connectors, functions);
    }

    private static RowStoreCandidate Of(LateMaterializationReport report, string table)
    {
        return Assert.Single(report.Hosts, x => x.Table == table);
    }

    [Fact]
    public void AUnionOfOneRelationWhoseInputsDifferOnlyByAnEmitCarriesTheCountedCopy()
    {
        var join = new MergeJoinRelation()
        {
            Left = UnionOfOneRelation(9, 8),
            Right = Wide("d1", 0),
            Type = JoinType.Inner,
            LeftKeys = [Field(8)],
            RightKeys = [Field(9)]
        };
        var plan = Write(join, 10);
        plan.Relations.Add(KeyLast("fact", 8));
        var (report, connectors, functions) = Analyze(plan, null);
        var fact = Of(report, "fact");

        LateMaterializationPass.Run(plan, connectors, functions, 1, report.Selection());

        var write = Assert.IsType<WriteRelation>(plan.Relations[0]);
        Assert.True(!fact.Admitted || write.Input is FetchRequiredColumnsRelation, report.ToString());
        RowReferencePlanValidator.Validate(plan.Relations, functions, 1);
    }

    [Fact]
    public void APartialSelectionAboveAUnionOfOneRelationCarriesTheCountedCopies()
    {
        var filter = new FilterRelation()
        {
            Input = UnionOfOneRelation(9, 0),
            Condition = Compare(FunctionsComparison.GreaterThan, Field(4), new NumericLiteral() { Value = 1 })
        };
        var plan = Write(JoinedTo(JoinedTo(filter, "d1"), "d2"), 11);
        plan.Relations.Add(Wide("fact", 8, new StringType()));
        var (report, connectors, functions) = Analyze(plan, null);
        var fact = Of(report, "fact");

        LateMaterializationPass.Run(plan, connectors, functions, 1, report.Selection());

        var write = Assert.IsType<WriteRelation>(plan.Relations[0]);
        Assert.True(!fact.Admitted || write.Input is FetchRequiredColumnsRelation, report.ToString());
        RowReferencePlanValidator.Validate(plan.Relations, functions, 1);
    }

    [Fact]
    public void ASinkKeyThatIsStoredIsReadByAFetchOfItsOwn()
    {
        var plan = Write(JoinedTo(JoinedTo(Wide("fact", 8, new StringType()), "d1"), "d2"), 11);
        var (report, connectors, functions) = Analyze(plan, [1]);
        var fact = Of(report, "fact");
        Assert.True(fact.Admitted, report.ToString());

        LateMaterializationPass.Run(plan, connectors, functions, 1, report.Selection());
        RowReferencePlanValidator.Validate(plan.Relations, functions, 1);

        int fetches = 0;
        var at = Assert.IsType<WriteRelation>(plan.Relations[0]).Input;
        while (at is FetchRequiredColumnsRelation fetch)
        {
            fetches++;
            at = fetch.Input;
        }
        Assert.Equal(fetches, fact.Fetches);
    }
}
