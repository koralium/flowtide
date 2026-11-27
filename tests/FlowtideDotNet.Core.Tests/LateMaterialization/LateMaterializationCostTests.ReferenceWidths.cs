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
using FlowtideDotNet.Substrait.Relations;
using static FlowtideDotNet.Core.Tests.LateMaterialization.Differential.DifferentialCarrierTests;

namespace FlowtideDotNet.Core.Tests.LateMaterialization;

public partial class LateMaterializationCostTests
{
    [Theory]
    [InlineData(false, 0)]
    [InlineData(false, 7)]
    [InlineData(false, 14)]
    [InlineData(false, 15)]
    [InlineData(true, 0)]
    [InlineData(true, 7)]
    [InlineData(true, 14)]
    [InlineData(true, 15)]
    public void ReferenceWidthsSkipBothReferenceKindsOnlyOnMismatch(bool exchange, int width)
    {
        var plan = StaleReference(width);
        if (exchange)
        {
            plan.Relations[0] = new ExchangeRelation()
            {
                Input = plan.Relations[0],
                ExchangeKind = new ScatterExchangeKind() { Fields = [Field(0)] },
                Targets = [new StandardOutputExchangeTarget() { PartitionIds = [0] }],
                PartitionCount = 1
            };
            ((FilterRelation)((WriteRelation)plan.Relations[1]).Input).Input = new StandardOutputExchangeReferenceRelation()
            {
                RelationId = 0,
                TargetId = 0,
                ReferenceOutputLength = width
            };
        }

        var report = Analyze(plan).report;

        Assert.NotEmpty(report.Hosts);
        if (width == 14)
        {
            Assert.Empty(report.Skipped);
            Assert.True(report.Admitted > 0);
        }
        else
        {
            Assert.Contains($"says it has {width} columns, the relation has 14", report.Skipped);
            Assert.Equal(0, report.Admitted);
            Assert.Empty(report.Selection());
        }
    }

    [Theory]
    [InlineData(LateMaterializationMode.Auto)]
    [InlineData(LateMaterializationMode.Placebo)]
    public async Task StaleReferenceLeavesAutoAndPlaceboPlansUnchanged(LateMaterializationMode mode)
    {
        var plan = StaleReference(7);
        var before = SubstraitSerializer.SerializeToJson(plan);
        using var storage = new ReservoirPersistentStorage(new ReservoirStorageOptions() { FileProvider = new MemoryFileProvider() });
        var builder = RunnableBuilder(plan, mode)
            .WithStateOptions(new StateManagerOptions() { PersistentStorage = storage });

        await using var stream = builder.Build();

        Assert.Contains("says it has 7 columns, the relation has 14", builder.LateMaterializationReport!.Skipped);
        Assert.Equal(0, builder.LateMaterializationReport.Admitted);
        Assert.Empty(builder.LateMaterializationReport.Selection());
        Assert.Equal(before, SubstraitSerializer.SerializeToJson(plan));
    }
}
