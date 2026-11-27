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

using FlowtideDotNet.Core.Engine;
using FlowtideDotNet.Storage.Persistence.Reservoir;
using FlowtideDotNet.Storage.Persistence.Reservoir.Internal;
using FlowtideDotNet.Storage.Persistence.Reservoir.MemoryDisk;
using FlowtideDotNet.Storage.StateManager;
using FlowtideDotNet.Substrait;
using FlowtideDotNet.Substrait.Expressions;
using FlowtideDotNet.Substrait.FunctionExtensions;
using FlowtideDotNet.Substrait.Relations;
using FlowtideDotNet.Substrait.Type;
using static FlowtideDotNet.Core.Tests.LateMaterialization.Differential.DifferentialCarrierTests;

namespace FlowtideDotNet.Core.Tests.LateMaterialization;

public partial class LateMaterializationCostTests
{
    [Fact]
    public async Task AutoDeclinesWhenTheCopyAdmitsDifferentColumnsAtTheSameHost()
    {
        Plan MakePlan()
        {
            var fact = Wide("fact", 7, new StringType());
            fact.BaseSchema.Struct!.Types[7] = new Int64Type();
            var source = JoinedTo(fact, "d1");
            var reference = new ReferenceRelation() { RelationId = 1, ReferenceOutputLength = source.OutputLength - 1 };
            var filter = new FilterRelation()
            {
                Input = reference,
                Condition = new ScalarFunction()
                {
                    ExtensionUrn = FunctionsBoolean.Urn,
                    ExtensionName = FunctionsBoolean.And,
                    Arguments = Enumerable.Range(1, 6).Select(i => (Expression)Compare(FunctionsComparison.Equal, Field(i), Field(i))).ToList()
                }
            };
            var plan = Write(filter, reference.OutputLength);
            plan.Relations.Add(source);
            return plan;
        }

        var plan = MakePlan();
        var originalReport = Analyze(plan).report;
        Assert.Equal(0, originalReport.Admitted);
        Assert.Contains("says it has 8 columns, the relation has 9", originalReport.Skipped);
        var before = SubstraitSerializer.SerializeToJson(plan);
        using var storage = new ReservoirPersistentStorage(new ReservoirStorageOptions() { FileProvider = new MemoryFileProvider() });
        var options = new StateManagerOptions() { PersistentStorage = storage, CachePageCount = 80, MinCachePageCount = 80 };
        await using var off = RunnableBuilder(MakePlan(), LateMaterializationMode.Off).WithStateOptions(options).Build();
        var builder = RunnableBuilder(plan, LateMaterializationMode.Auto).WithStateOptions(options);

        await using var auto = builder.Build();

        Assert.Equal(0, builder.LateMaterializationReport!.Admitted);
        Assert.Contains("says it has 8 columns", builder.LateMaterializationReport.Skipped);
        Assert.Equal(before, SubstraitSerializer.SerializeToJson(plan));
    }
}
