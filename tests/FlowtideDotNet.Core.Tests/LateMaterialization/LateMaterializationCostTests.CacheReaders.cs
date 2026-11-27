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
using FlowtideDotNet.Core.Lineage;
using FlowtideDotNet.Core.Tests.LateMaterialization.Differential;
using System.Threading.Tasks.Dataflow;
using FlowtideDotNet.Substrait.Expressions;
using FlowtideDotNet.Core.Engine;
using FlowtideDotNet.Core.Engine.LateMaterialization;
using FlowtideDotNet.Storage.Persistence.Reservoir;
using FlowtideDotNet.Storage.Persistence.Reservoir.Internal;
using FlowtideDotNet.Storage.Persistence.Reservoir.MemoryDisk;
using FlowtideDotNet.Storage.StateManager;
using FlowtideDotNet.Substrait;
using FlowtideDotNet.Substrait.Expressions.Literals;
using FlowtideDotNet.Substrait.FunctionExtensions;
using FlowtideDotNet.Substrait.Relations;
using static FlowtideDotNet.Core.Tests.LateMaterialization.Differential.DifferentialCarrierTests;

namespace FlowtideDotNet.Core.Tests.LateMaterialization;

public partial class LateMaterializationCostTests
{
    [Fact]
    public async Task AutoDeclinesWhenUnionDemandCreatesMoreReadersThanAnalysisCounts()
    {
        Plan MakePlan()
        {
            const int partitions = 16;
            var exchange = new ExchangeRelation()
            {
                Input = FourJoins(Wide("fact", 16)),
                ExchangeKind = new ScatterExchangeKind() { Fields = [Field(1)] },
                Targets = Enumerable.Range(0, partitions).Select(i => (ExchangeTarget)new StandardOutputExchangeTarget() { PartitionIds = [i] }).ToList(),
                PartitionCount = partitions
            };
            var filters = new List<Relation>();
            for (int i = 0; i < partitions; i++)
            {
                filters.Add(new FilterRelation()
                {
                    Input = new StandardOutputExchangeReferenceRelation() { RelationId = 1, TargetId = i, ReferenceOutputLength = exchange.OutputLength },
                    Condition = Compare(FunctionsComparison.GreaterThan, Field(2), new NumericLiteral() { Value = 1 })
                });
            }
            var plan = Write(new SetRelation() { Inputs = filters, Operation = SetOperation.UnionAll }, exchange.OutputLength);
            plan.Relations.Add(exchange);
            return plan;
        }

        using var storage = new ReservoirPersistentStorage(new ReservoirStorageOptions() { FileProvider = new MemoryFileProvider() });
        var options = new StateManagerOptions() { PersistentStorage = storage, CachePageCount = 700, MinCachePageCount = 700 };
        await using var off = RunnableBuilder(MakePlan(), LateMaterializationMode.Off).WithStateOptions(options).Build();
        var builder = RunnableBuilder(MakePlan(), LateMaterializationMode.Auto).WithStateOptions(options);
        await using var auto = builder.Build();
        Assert.Equal(0, builder.LateMaterializationReport!.Admitted);
    }
    private sealed class PlanModifyingSink : RegexConnectorSinkFactory
    {
        public PlanModifyingSink() : base("*") { }

        public override IStreamEgressVertex CreateSink(WriteRelation writeRelation, IFunctionsRegister functionsRegister, ExecutionDataflowBlockOptions dataflowBlockOptions)
            => new DifferentialSink();

        public override TableLineageMetadata GetLineageMetadata(WriteRelation writeRelation, bool includeSchema)
            => new("test", writeRelation.NamedObject.DotSeperated, default);

        public override Relation ModifyPlan(WriteRelation relation)
        {
            relation.Input = new FilterRelation
            {
                Input = relation.Input,
                Condition = new ScalarFunction
                {
                    ExtensionUrn = "extension:test:fn",
                    ExtensionName = "accept_any",
                    Arguments = [new ArrayLiteral { Expressions = [Field(0)] }]
                }
            };
            return relation;
        }
    }

    [Fact]
    public async Task AutoChecksSerializationAfterConnectorPlanModification()
    {
        FlowtideBuilder Builder(LateMaterializationMode mode, ReservoirPersistentStorage storage)
        {
            var connectors = new ConnectorManager();
            connectors.AddSource(new RunnableSources());
            connectors.AddSink(new PlanModifyingSink());
            var builder = new FlowtideBuilder("review-c-modified-" + mode)
                .AddPlan(Write(FourJoins(Wide("fact", 8)), 13), false)
                .AddConnectorManager(connectors)
                .SetLateMaterialization(mode)
                .WithStateOptions(new StateManagerOptions() { PersistentStorage = storage });
            builder.FunctionsRegister.RegisterColumnScalarFunction(
                "extension:test:fn", "accept_any",
                (_, _, _, _) => System.Linq.Expressions.Expression.Constant(true));
            return builder;
        }

        using var offStorage = new ReservoirPersistentStorage(new ReservoirStorageOptions() { FileProvider = new MemoryFileProvider() });
        await using var off = Builder(LateMaterializationMode.Off, offStorage).Build();
        using var autoStorage = new ReservoirPersistentStorage(new ReservoirStorageOptions() { FileProvider = new MemoryFileProvider() });
        var builder = Builder(LateMaterializationMode.Auto, autoStorage);
        await using var auto = builder.Build();
        Assert.Equal(0, builder.LateMaterializationReport!.Admitted);
    }

}
