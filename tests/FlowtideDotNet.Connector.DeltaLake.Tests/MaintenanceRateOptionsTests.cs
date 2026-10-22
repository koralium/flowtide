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

using FlowtideDotNet.Connector.DeltaLake.Internal;
using FlowtideDotNet.Connector.DeltaLake.Internal.Maintenance;
using Stowage;
using System.Threading.Tasks.Dataflow;
using static FlowtideDotNet.Connector.DeltaLake.Tests.DeltaTestKit;

namespace FlowtideDotNet.Connector.DeltaLake.Tests
{
    public class MaintenanceRateOptionsTests
    {
        private static DeltaLakeSink Construct(DeltaLakeOptions options)
        {
            return new DeltaLakeSink(options, CleanupConfigurationTests.Relation("t"), new ExecutionDataflowBlockOptions());
        }

        [Fact]
        public void RatesBelowTheDefaultsThrowAtConstruction()
        {
            var directory = StoreFixtures.TempDirectory(nameof(RatesBelowTheDefaultsThrowAtConstruction));
            using var storage = Files.Of.LocalDisk(directory);
            foreach (var requests in new[] { 199, 0, -1 })
            {
                var options = new DeltaLakeOptions() { StorageLocation = storage, MaintenanceRequestsPerSecond = requests };
                var e = Assert.Throws<ArgumentException>(() => Construct(options));
                Assert.Equal(nameof(DeltaLakeOptions.MaintenanceRequestsPerSecond), e.ParamName);
                Assert.Contains("use at least", e.Message);
                // The factory every stream uses funnels through the same constructor
                Assert.Throws<ArgumentException>(() => new DeltaLakeSinkFactory(options).CreateSink(CleanupConfigurationTests.Relation("t"), null!, new ExecutionDataflowBlockOptions()));
            }
            foreach (var bytes in new[] { (64L << 20) - 1, 0, -1 })
            {
                var options = new DeltaLakeOptions() { StorageLocation = storage, MaintenanceBytesPerSecond = bytes };
                var e = Assert.Throws<ArgumentException>(() => Construct(options));
                Assert.Equal(nameof(DeltaLakeOptions.MaintenanceBytesPerSecond), e.ParamName);
                Assert.Contains("use at least", e.Message);
                Assert.Throws<ArgumentException>(() => new DeltaLakeSinkFactory(options).CreateSink(CleanupConfigurationTests.Relation("t"), null!, new ExecutionDataflowBlockOptions()));
            }
            Construct(new DeltaLakeOptions() { StorageLocation = storage, MaintenanceRequestsPerSecond = 200, MaintenanceBytesPerSecond = 64L << 20 });
            Construct(new DeltaLakeOptions() { StorageLocation = storage, MaintenanceRequestsPerSecond = 5000, MaintenanceBytesPerSecond = 1L << 30 });
        }

        [Fact]
        public async Task ConfiguredRatesReachTheProcessBudget()
        {
            var testName = nameof(ConfiguredRatesReachTheProcessBudget);
            var storage = new HookFileStorage(Files.Of.InternalMemory($"./{testName}"));
            var source = new DeltaMaintenanceBudget.SharedBudget();
            await using var stream = new DeltaLakeSinkStream(testName, storage, options =>
            {
                options.MaintenanceBudgetOverride = null;
                options.SharedBudgetForTests = source;
                options.MaintenanceTempDirectory = Path.Combine(Path.GetTempPath(), "flowtide-tests", testName);
                options.MaintenanceRequestsPerSecond = 5000;
                options.MaintenanceBytesPerSecond = 256L << 20;
            });
            stream.Generate(10);
            await stream.StartStream(UserInsert);
            await WaitForVersion(storage, "test", stream, 0);

            var budget = source.Existing;
            Assert.NotNull(budget);
            Assert.Equal(5000, budget!.Buckets.Requests.Rate);
            Assert.Equal(5000, budget.Buckets.Requests.Burst);
            Assert.Equal(256L << 20, budget.Buckets.Bytes.Rate);
            Assert.Equal(256L << 20, budget.Buckets.Bytes.Burst);
            Assert.Equal(MaintenanceBuckets.MaxByteChunk, budget.Buckets.ByteChunk);
            Assert.Equal(5000, budget.Settings.RequestsPerSecond);
            Assert.Equal(256L << 20, budget.Settings.BytesPerSecond);
            Assert.Equal(5000, budget.Capacity.RequestsPerSecond);
            Assert.Equal(256L << 20, budget.Capacity.BytesPerSecond);
        }
    }
}
