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

using FlowtideDotNet.Core.Engine.LateMaterialization;
using FlowtideDotNet.Substrait;
using FlowtideDotNet.Substrait.Relations;
using FlowtideDotNet.Substrait.Type;
using static FlowtideDotNet.Core.Tests.LateMaterialization.Differential.DifferentialCarrierTests;

namespace FlowtideDotNet.Core.Tests.LateMaterialization;

public partial class LateMaterializationCostTests
{
    [Fact]
    public void DisjointJoinInputsChargeBothFetches()
    {
        var read = Wide("fact", 6, new StringType());
        Relation Branch(string dimension, params int[] columns) => JoinedTo(new ProjectRelation()
        {
            Input = new ReferenceRelation() { RelationId = 0, ReferenceOutputLength = 7 },
            Expressions = [],
            Emit = new[] { 0 }.Concat(columns).ToList()
        }, dimension);
        var join = new MergeJoinRelation()
        {
            Left = Branch("d1", 1, 2, 3),
            Right = Branch("d2", 4, 5, 6),
            Type = JoinType.Inner,
            LeftKeys = [Field(1), Field(2), Field(3)],
            RightKeys = [Field(6), Field(7), Field(8)]
        };
        var plan = Write(join, 10);
        plan.Relations.Insert(0, read);
        var (report, connectors, functions) = Analyze(plan);
        var candidate = Of(report, "fact");
        Assert.Equal(192, candidate.Saving);
        Assert.Equal(1, LateMaterializationPass.Run(plan, connectors, functions, 1,
            new Dictionary<Relation, List<int>>() { [read] = candidate.Columns }));
        RowReferencePlanValidator.Validate(plan.Relations, functions, 1);
        Assert.IsType<FetchRequiredColumnsRelation>(join.Left);
        Assert.IsType<FetchRequiredColumnsRelation>(join.Right);
        Assert.Equal(2, candidate.Fetches);
        Assert.False(candidate.Admitted, $"Two input fetches leave only 120 bytes net, got {candidate.Net}.");
    }

    [Fact]
    public void ScatterRowsKeepTheirShareAfterABroadcastJoin()
    {
        const int count = 8;
        ExchangeRelation Exchange(Relation input, bool scatter) => new()
        {
            Input = input,
            ExchangeKind = scatter ? new ScatterExchangeKind() { Fields = [Field(0)] } : new BroadcastExchangeKind(),
            PartitionCount = count,
            Targets = Enumerable.Range(0, count).Select(i => (ExchangeTarget)new StandardOutputExchangeTarget() { PartitionIds = [i] }).ToList()
        };
        StandardOutputExchangeReferenceRelation Reference(int relation, int partition, int width) => new()
        {
            RelationId = relation,
            TargetId = partition,
            ReferenceOutputLength = width
        };
        var branches = new List<Relation>();
        for (int i = 0; i < count; i++)
        {
            var first = new MergeJoinRelation()
            {
                Left = Reference(0, i, 2), Right = Reference(1, i, 1),
                Type = JoinType.Inner, LeftKeys = [Field(0)], RightKeys = [Field(2)]
            };
            branches.Add(new MergeJoinRelation()
            {
                Left = first, Right = Reference(2, i, 1),
                Type = JoinType.Inner, LeftKeys = [Field(0)], RightKeys = [Field(3)]
            });
        }
        var plan = Write(new SetRelation() { Inputs = branches, Operation = SetOperation.UnionAll }, 4);
        plan.Relations.InsertRange(0,
        [
            Exchange(Wide("fact", 1, new StringType()), true),
            Exchange(Wide("d1", 0), false),
            Exchange(Wide("d2", 0), false)
        ]);
        var (report, connectors, functions) = Analyze(plan);
        var candidate = Of(report, "fact");
        Assert.Equal(1, LateMaterializationPass.Run(plan, connectors, functions, 1,
            new Dictionary<Relation, List<int>>() { [candidate.Host] = candidate.Columns }));
        RowReferencePlanValidator.Validate(plan.Relations, functions, 1);
        Assert.Equal(2, candidate.Copies);
        Assert.Equal(64, candidate.Saving);
        Assert.False(candidate.Admitted);
    }

    [Fact]
    public void MarginalSelectionChargesTheExtraReferenceSites()
    {
        var read = Wide("fact", 9, new StringType());
        read.BaseSchema.Struct!.Types[9] = new Int64Type();
        ProjectRelation Branch(IEnumerable<int> columns) => new()
        {
            Input = new ReferenceRelation() { RelationId = 0, ReferenceOutputLength = 10 },
            Expressions = [],
            Emit = new[] { 0 }.Concat(columns).ToList()
        };
        var wide = Write(FourJoins(Branch(Enumerable.Range(1, 8))), 13).Relations[0];
        var narrow = Write(JoinedTo(JoinedTo(Branch([9]), "e1"), "e2"), 4).Relations[0];
        var plan = new Plan() { Relations = [read, wide, narrow] };
        var (report, connectors, functions) = Analyze(plan);
        var candidate = Of(report, "fact");
        Assert.True(candidate.Admitted, candidate.Reason);
        Assert.DoesNotContain(9, candidate.Columns);
        Assert.Equal(1024, candidate.Saving);
        Assert.Equal(952, candidate.Net);
        Assert.Equal(1, LateMaterializationPass.Run(plan, connectors, functions, 1, report.Selection()));
        RowReferencePlanValidator.Validate(plan.Relations, functions, 1);
        Assert.IsNotType<FetchRequiredColumnsRelation>(((WriteRelation)narrow).Input);
    }
}
