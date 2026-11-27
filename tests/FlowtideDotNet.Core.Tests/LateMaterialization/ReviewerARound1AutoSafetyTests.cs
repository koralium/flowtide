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
using FlowtideDotNet.Core.Operators.LateMaterialization;
using FlowtideDotNet.Storage;
using FlowtideDotNet.Storage.Persistence;
using FlowtideDotNet.Storage.Persistence.CacheStorage;
using FlowtideDotNet.Storage.Persistence.Reservoir;
using FlowtideDotNet.Storage.Persistence.Reservoir.Internal;
using FlowtideDotNet.Storage.Persistence.Reservoir.MemoryDisk;
using FlowtideDotNet.Storage.StateManager;
using FlowtideDotNet.Substrait.Expressions.Literals;
using FlowtideDotNet.Substrait.FunctionExtensions;
using FlowtideDotNet.Substrait.Relations;
using FlowtideDotNet.Substrait.Type;
using System.Threading.Tasks.Dataflow;
using static FlowtideDotNet.Core.Tests.LateMaterialization.Differential.DifferentialCarrierTests;

namespace FlowtideDotNet.Core.Tests.LateMaterialization;

public class ReviewerARound1AutoSafetyTests
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
            => readRelation.NamedTable.DotSeperated.StartsWith("plain") ? null : ["k"];
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

    private static ReadRelation Wide(string name, int payload)
    {
        var names = new List<string>() { "k" };
        var types = new List<SubstraitBaseType>() { new Int64Type() };
        for (int i = 0; i < payload; i++)
        {
            names.Add("c" + i);
            types.Add(new AnyType());
        }
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

    private static Relation FourJoins(Relation fact)
    {
        return JoinedTo(JoinedTo(JoinedTo(JoinedTo(fact, "d1"), "d2"), "d3"), "d4");
    }

    private static ConnectorManager Connectors()
    {
        var connectors = new ConnectorManager();
        connectors.AddSource(new Sources());
        connectors.AddSink(new Sinks());
        return connectors;
    }

    private static IPersistentStorage? StorageOf(string kind)
    {
        switch (kind)
        {
            case "reservoir":
                return new ReservoirPersistentStorage(new ReservoirStorageOptions() { FileProvider = new MemoryFileProvider() });
            case "file":
                return new FileCachePersistentStorage(new FileCacheOptions() { DirectoryPath = Path.Combine(Path.GetTempPath(), "flowtide-auto-safety-" + Guid.NewGuid().ToString("N")) });
            default:
                return null;
        }
    }

    private static int Fetchers(Relation relation)
    {
        return relation switch
        {
            FetchRequiredColumnsRelation fetch => fetch.Fetches.Count + Fetchers(fetch.Input),
            WriteRelation write => Fetchers(write.Input),
            SetRelation set => set.Inputs.Sum(x => Fetchers(x)),
            FilterRelation filter => Fetchers(filter.Input),
            ExchangeRelation exchange => Fetchers(exchange.Input),
            MergeJoinRelation join => Fetchers(join.Left) + Fetchers(join.Right),
            _ => 0
        };
    }

    [Theory]
    [InlineData("reservoir")]
    [InlineData("file")]
    [InlineData("unset")]
    public void AutoPlacesAStoreOnlyWhereTheStorageCanHoldOne(string kind)
    {
        using var persistent = StorageOf(kind);
        var plan = Write(FourJoins(Wide("fact", 8)), 13);
        var builder = new FlowtideBuilder("auto-safety-" + kind)
            .AddPlan(plan, false)
            .AddConnectorManager(Connectors())
            .WithStateOptions(new StateManagerOptions() { PersistentStorage = persistent })
            .SetLateMaterialization(LateMaterializationMode.Auto);

        Assert.Throws<NotSupportedException>(() => builder.Build());

        bool stores = RowStoreHostCollector.UsesRowStores(plan.Relations);
        Assert.True(stores == (kind == "reservoir"), $"{kind}: stores placed {stores}; {builder.LateMaterializationReport}");
    }

    [Fact]
    public void TheStoresAndTheirReadersFitTheShareOfTheCacheTheyWereAdmittedTo()
    {
        const int cachePages = 1000;
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
        var connectors = Connectors();
        var functions = new FunctionsRegister();
        BuiltinFunctions.RegisterFunctions(functions);

        var report = LateMaterializationPass.Analyze(plan, connectors, functions, 1);
        int stores = LateMaterializationPass.Run(plan, connectors, functions, 1, report.Selection());
        RowReferencePlanValidator.Validate(plan.Relations, functions, 1);

        int readers = plan.Relations.Sum(x => Fetchers(x));
        int queue = FlowtideDotNet.Core.Engine.FlowtideBuilder.RowStoreWriteQueuePages(cachePages, stores, readers);
        int held = stores * (queue + 2) + readers;
        Assert.True(queue < 1 || held <= cachePages / 20, $"{stores} stores and {readers} fetch readers hold {held} pages, a cache of {cachePages} lets {cachePages / 20} be held; {report}");
    }
}
