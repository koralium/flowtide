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

using FlowtideDotNet.Core.ColumnStore;
using FlowtideDotNet.Core.ColumnStore.TreeStorage;
using FlowtideDotNet.Core.Operators.Window.Bulk;
using FlowtideDotNet.Substrait.Expressions;
using System.Diagnostics.CodeAnalysis;

namespace FlowtideDotNet.Core.Compute.Columnar.Functions.WindowFunctions.Bulk
{
    internal class BulkRowNumberWindowFunctionDefinition : BulkWindowFunctionDefinition
    {
        public override bool TryCreate(WindowFunction windowFunction, IFunctionsRegister functionsRegister, [NotNullWhen(true)] out IBulkWindowFunction? bulkWindowFunction)
        {
            var maxRowNumber = long.MaxValue;
            if (windowFunction.Options != null &&
                windowFunction.Options.TryGetValue(BulkWindowFunctionOptions.MaxRowNumber, out var maxRowNumberText) &&
                long.TryParse(maxRowNumberText, out var parsedMaxRowNumber) &&
                parsedMaxRowNumber >= 1)
            {
                maxRowNumber = parsedMaxRowNumber;
            }
            bulkWindowFunction = new BulkRowNumberWindowFunction(maxRowNumber);
            return true;
        }
    }

    /// <summary>
    /// Row number for the bulk window operator. When a scan starts in the middle of a partition the next
    /// row number is seeded from the stored value of the previous row, so appending rows at the end of a
    /// partition only touches the appended rows. The function is stable as soon as a recomputed row keeps
    /// its stored number, since all later numbers are then unchanged as well.
    /// When the optimizer provides a maximum row number (a filter such as row_number() = 1 sits directly
    /// above), rows past the bound output null instead of an exact number. Since null stays null when rows
    /// shift, a change near the top of a partition only recomputes rows up to the bound instead of the
    /// whole partition.
    /// </summary>
    internal class BulkRowNumberWindowFunction : IBulkWindowFunction
    {
        private readonly long _maxRowNumber;
        private int _functionIndex;
        private long _nextRowNumber;

        public BulkRowNumberWindowFunction(long maxRowNumber)
        {
            _maxRowNumber = maxRowNumber;
        }

        public long AffectedRowsBefore => 0;

        public long AffectedRowsAfter => long.MaxValue;

        public bool StableByValueEquality => true;

        public long EqualityStableAfterRows => 0;

        public int AuxiliaryStateColumnCount => 0;

        public Task Initialize(BulkWindowFunctionContext context)
        {
            _functionIndex = context.FunctionIndex;
            return Task.CompletedTask;
        }

        public ValueTask Commit()
        {
            return ValueTask.CompletedTask;
        }

        public async ValueTask StartScan(ColumnRowReference partitionValues, BulkWindowSeedReader seedReader, bool fromPartitionStart)
        {
            if (fromPartitionStart || !await seedReader.EnsureRows(1))
            {
                _nextRowNumber = 1;
                return;
            }
            var previous = seedReader.GetState(1, _functionIndex);
            if (previous.IsNull)
            {
                if (_maxRowNumber == long.MaxValue)
                {
                    throw new InvalidOperationException("The stored row number of the previous row is missing");
                }
                // The previous row is past the maximum row number, so every following row is as well. The
                // exact number is unknown but not needed, any value above the bound behaves the same.
                _nextRowNumber = _maxRowNumber + 1;
                return;
            }
            _nextRowNumber = previous.AsLong + 1;
        }

        public ValueTask ComputeRow(BulkWindowRowContext context, DataValueContainer result)
        {
            if (_nextRowNumber > _maxRowNumber)
            {
                result._type = ArrowTypeId.Null;
            }
            else
            {
                result._type = ArrowTypeId.Int64;
                result._int64Value = new Int64Value(_nextRowNumber);
                _nextRowNumber++;
            }
            return ValueTask.CompletedTask;
        }

        public ValueTask EndScan()
        {
            return ValueTask.CompletedTask;
        }
    }
}
