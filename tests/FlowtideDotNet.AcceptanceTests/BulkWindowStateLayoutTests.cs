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

using FlowtideDotNet.AcceptanceTests.Entities;
using FlowtideDotNet.AcceptanceTests.Internal;
using FlowtideDotNet.Core.ColumnStore;
using FlowtideDotNet.Core.ColumnStore.DataValues;
using FlowtideDotNet.Core.ColumnStore.TreeStorage;
using FlowtideDotNet.Core.Compute;
using FlowtideDotNet.Core.Compute.Columnar.Functions.WindowFunctions.Bulk;
using FlowtideDotNet.Core.Operators.Window.Bulk;
using FlowtideDotNet.Substrait.Expressions;
using FlowtideDotNet.Substrait.FunctionExtensions;
using System.Diagnostics.CodeAnalysis;

namespace FlowtideDotNet.AcceptanceTests
{
    /// <summary>
    /// Tests that the bulk window operator detects state written with another function state layout,
    /// which happens when a build changes a function's auxiliary columns and restores an old checkpoint.
    /// </summary>
    public class BulkWindowStateLayoutTests
    {
        /// <summary>
        /// Wraps a bulk window function with one extra auxiliary column, simulating a build where the
        /// same function has another state layout.
        /// </summary>
        private sealed class ExtraAuxWindowFunction : IBulkWindowFunction
        {
            private readonly IBulkWindowFunction _inner;
            private int _extraAuxIndex;

            public ExtraAuxWindowFunction(IBulkWindowFunction inner)
            {
                _inner = inner;
            }

            public long AffectedRowsBefore => _inner.AffectedRowsBefore;

            public long AffectedRowsAfter => _inner.AffectedRowsAfter;

            public bool StableByValueEquality => _inner.StableByValueEquality;

            public long EqualityStableAfterRows => _inner.EqualityStableAfterRows;

            public int AuxiliaryStateColumnCount => _inner.AuxiliaryStateColumnCount + 1;

            public Task Initialize(BulkWindowFunctionContext context)
            {
                _extraAuxIndex = context.AuxiliaryColumnStartIndex + _inner.AuxiliaryStateColumnCount;
                return _inner.Initialize(context);
            }

            public ValueTask Commit() => _inner.Commit();

            public ValueTask StartScan(ColumnRowReference partitionValues, BulkWindowSeedReader seedReader, bool fromPartitionStart)
                => _inner.StartScan(partitionValues, seedReader, fromPartitionStart);

            public bool TryComputeRow(BulkWindowRowContext context, DataValueContainer result)
            {
                if (_inner.TryComputeRow(context, result))
                {
                    context.SetAuxValue(_extraAuxIndex, NullValue.Instance);
                    return true;
                }
                return false;
            }

            public async ValueTask ComputeRow(BulkWindowRowContext context, DataValueContainer result)
            {
                await _inner.ComputeRow(context, result);
                context.SetAuxValue(_extraAuxIndex, NullValue.Instance);
            }

            public ValueTask EndScan() => _inner.EndScan();
        }

        private sealed class ExtraAuxDefinition : BulkWindowFunctionDefinition
        {
            private readonly BulkWindowFunctionDefinition _inner;

            public ExtraAuxDefinition(BulkWindowFunctionDefinition inner)
            {
                _inner = inner;
            }

            public override bool TryCreate(
                WindowFunction windowFunction,
                IFunctionsRegister functionsRegister,
                [NotNullWhen(true)] out IBulkWindowFunction? bulkWindowFunction)
            {
                if (_inner.TryCreate(windowFunction, functionsRegister, out var inner))
                {
                    bulkWindowFunction = new ExtraAuxWindowFunction(inner);
                    return true;
                }
                bulkWindowFunction = null;
                return false;
            }
        }

        public record SumResult(string? companyId, int userkey, long? value);

        private const string RunningSumQuery = @"
            INSERT INTO output
            SELECT
                CompanyId,
                UserKey,
                CAST(SUM(DoubleValue) OVER (PARTITION BY CompanyId ORDER BY UserKey ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS INT) as value
            FROM users";

        private static void SeedUsers(FlowtideTestStream stream)
        {
            for (int i = 0; i < 10; i++)
            {
                stream.AddOrUpdateUser(new User()
                {
                    UserKey = i,
                    CompanyId = "1",
                    DoubleValue = i + 1
                });
            }
        }

        private static void UseExtraAuxSum(FlowtideTestStream stream)
        {
            ((FunctionsRegister)stream.FunctionsRegister).SetBulkWindowFunctionForTests(
                FunctionsArithmetic.Uri,
                FunctionsArithmetic.Sum,
                new ExtraAuxDefinition(new BulkSumWindowFunctionDefinition()));
        }

        [Fact]
        public async Task RestartWithChangedAuxiliaryLayoutFailsLoudly()
        {
            // The stored state layout depends on each function's auxiliary column count, which can
            // change between builds. Restoring must fail loudly instead of reading shifted columns.
            var fileProvider = new KeepAliveMemoryFileProvider();

            // Run a sum with one extra auxiliary column, simulating an older build.
            await using (var oldBuild = new SharedStorageTestStream("BulkWindowLayout/AuxDrift", fileProvider))
            {
                UseExtraAuxSum(oldBuild);
                SeedUsers(oldBuild);
                await oldBuild.StartStream(RunningSumQuery);
                await oldBuild.WaitForUpdate();
                await oldBuild.StopStream();
            }

            // Restart with the normal sum, the stored layout has one auxiliary column too many.
            await using var newBuild = new SharedStorageTestStream("BulkWindowLayout/AuxDrift", fileProvider);
            SeedUsers(newBuild);

            var exception = await Assert.ThrowsAnyAsync<Exception>(async () =>
            {
                await newBuild.StartStream(RunningSumQuery);

                // Push a change through the restored stream, surfacing any failure from the restore.
                newBuild.AddOrUpdateUser(new User()
                {
                    UserKey = 100,
                    CompanyId = "1",
                    DoubleValue = 100
                });
                await newBuild.WaitForUpdate();
            });
            Assert.Contains("state layout", exception.Message);
        }

        [Fact]
        public async Task RestartWithSameLayoutRestores()
        {
            // A restart on the same build must restore and keep producing correct values.
            var fileProvider = new KeepAliveMemoryFileProvider();

            await using (var firstRun = new SharedStorageTestStream("BulkWindowLayout/SameLayout", fileProvider))
            {
                SeedUsers(firstRun);
                await firstRun.StartStream(RunningSumQuery);
                await firstRun.WaitForUpdate();
                await firstRun.StopStream();
            }

            await using var secondRun = new SharedStorageTestStream("BulkWindowLayout/SameLayout", fileProvider);
            SeedUsers(secondRun);
            await secondRun.StartStream(RunningSumQuery);

            secondRun.AddOrUpdateUser(new User()
            {
                UserKey = 100,
                CompanyId = "1",
                DoubleValue = 100
            });
            await secondRun.WaitForUpdate();

            var expected = new List<SumResult>();
            long sum = 0;
            for (int i = 0; i < 10; i++)
            {
                sum += i + 1;
                expected.Add(new SumResult("1", i, sum));
            }
            expected.Add(new SumResult("1", 100, sum + 100));
            secondRun.AssertCurrentDataEqual(expected);
        }
    }
}
