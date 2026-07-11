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
using FlowtideDotNet.Core.ColumnStore.DataValues;
using FlowtideDotNet.Core.ColumnStore.TreeStorage;
using FlowtideDotNet.Core.Operators.Window.Bulk;
using FlowtideDotNet.Storage.Memory;
using FlowtideDotNet.Substrait.Expressions;
using FlowtideDotNet.Substrait.Expressions.Literals;
using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;

namespace FlowtideDotNet.Core.Compute.Columnar.Functions.WindowFunctions.Bulk
{
    internal static class BulkLeadLagUtils
    {
        /// <summary>
        /// Reads a constant non negative offset argument, which the ring and lookahead fast paths handle.
        /// Dynamic per-row offsets and negative constants use the slower dynamic variant instead.
        /// </summary>
        public static bool TryGetConstantOffset(WindowFunction windowFunction, out long offset)
        {
            offset = 1;
            if (windowFunction.Arguments.Count < 2)
            {
                return true;
            }
            if (windowFunction.Arguments[1] is NumericLiteral numericLiteral &&
                numericLiteral.Value == decimal.Truncate(numericLiteral.Value) &&
                numericLiteral.Value >= 0)
            {
                offset = (long)numericLiteral.Value;
                return true;
            }
            return false;
        }

        public static bool TryCreate(WindowFunction windowFunction, IFunctionsRegister functionsRegister, bool isLead, [NotNullWhen(true)] out IBulkWindowFunction? bulkWindowFunction)
        {
            bulkWindowFunction = null;
            if (windowFunction.Arguments.Count < 1)
            {
                return false;
            }
            var valueFunction = ColumnProjectCompiler.CompileToValue(windowFunction.Arguments[0], functionsRegister);
            Func<EventBatchData, int, IDataValue>? defaultFunction = default;
            if (windowFunction.Arguments.Count > 2)
            {
                defaultFunction = ColumnProjectCompiler.CompileToValue(windowFunction.Arguments[2], functionsRegister);
            }
            if (TryGetConstantOffset(windowFunction, out var offset))
            {
                bulkWindowFunction = isLead
                    ? new BulkLeadWindowFunction(valueFunction, defaultFunction, offset)
                    : new BulkLagWindowFunction(valueFunction, defaultFunction, offset);
                return true;
            }
            var offsetFunction = ColumnProjectCompiler.CompileToValue(windowFunction.Arguments[1], functionsRegister);
            bulkWindowFunction = new BulkLeadLagDynamicWindowFunction(valueFunction, offsetFunction, defaultFunction, isLead);
            return true;
        }
    }

    internal class BulkLagWindowFunctionDefinition : BulkWindowFunctionDefinition
    {
        public override bool TryCreate(WindowFunction windowFunction, IFunctionsRegister functionsRegister, [NotNullWhen(true)] out IBulkWindowFunction? bulkWindowFunction)
        {
            return BulkLeadLagUtils.TryCreate(windowFunction, functionsRegister, isLead: false, out bulkWindowFunction);
        }
    }

    internal class BulkLeadWindowFunctionDefinition : BulkWindowFunctionDefinition
    {
        public override bool TryCreate(WindowFunction windowFunction, IFunctionsRegister functionsRegister, [NotNullWhen(true)] out IBulkWindowFunction? bulkWindowFunction)
        {
            return BulkLeadLagUtils.TryCreate(windowFunction, functionsRegister, isLead: true, out bulkWindowFunction);
        }
    }

    /// <summary>
    /// lag with a constant offset: the value of the row a fixed number of logical rows earlier, kept in a
    /// small ring that is seeded from the rows before the scan start.
    /// </summary>
    internal class BulkLagWindowFunction : IBulkWindowFunction
    {
        private readonly Func<EventBatchData, int, IDataValue> _valueFunction;
        private readonly Func<EventBatchData, int, IDataValue>? _defaultFunction;
        private readonly long _offset;

        private BulkWindowValueRing? _ring;

        public BulkLagWindowFunction(
            Func<EventBatchData, int, IDataValue> valueFunction,
            Func<EventBatchData, int, IDataValue>? defaultFunction,
            long offset)
        {
            _valueFunction = valueFunction;
            _defaultFunction = defaultFunction;
            _offset = offset;
        }

        public long AffectedRowsBefore => 0;

        public long AffectedRowsAfter => _offset;

        public bool StableByValueEquality => false;

        public long EqualityStableAfterRows => long.MaxValue;

        public int AuxiliaryStateColumnCount => 0;

        public Task Initialize(BulkWindowFunctionContext context)
        {
            if (_offset > 0)
            {
                _ring = new BulkWindowValueRing(_offset + 1, context.MemoryAllocator);
            }
            return Task.CompletedTask;
        }

        public ValueTask Commit()
        {
            return ValueTask.CompletedTask;
        }

        public async ValueTask StartScan(ColumnRowReference partitionValues, BulkWindowSeedReader seedReader, bool fromPartitionStart)
        {
            if (_ring == null)
            {
                return;
            }
            _ring.Clear();
            if (fromPartitionStart)
            {
                return;
            }
            await seedReader.EnsureRows((int)_offset);
            var available = Math.Min((int)_offset, seedReader.MaterializedRows);
            for (int back = available; back >= 1; back--)
            {
                var row = seedReader.GetRow(back);
                _ring.Push(_valueFunction(row.referenceBatch, row.RowIndex));
            }
        }

        public bool TryComputeRow(BulkWindowRowContext context, DataValueContainer result)
        {
            if (_offset == 0)
            {
                _valueFunction(context.Batch, context.RowIndex).CopyToContainer(result);
                return true;
            }
            Debug.Assert(_ring != null);

            if (_ring.Count >= _offset)
            {
                _ring.PopOldest().CopyToContainer(result);
            }
            else if (_defaultFunction != null)
            {
                _defaultFunction(context.Batch, context.RowIndex).CopyToContainer(result);
            }
            else
            {
                result._type = ArrowTypeId.Null;
            }
            _ring.Push(_valueFunction(context.Batch, context.RowIndex));
            return true;
        }

        public ValueTask ComputeRow(BulkWindowRowContext context, DataValueContainer result)
        {
            TryComputeRow(context, result);
            return ValueTask.CompletedTask;
        }

        public ValueTask EndScan()
        {
            return ValueTask.CompletedTask;
        }
    }

    /// <summary>
    /// lead with a constant offset: the value of the row a fixed number of logical rows later, read through
    /// a lookahead reader; the default value is used when the target is beyond the partition end.
    /// </summary>
    internal class BulkLeadWindowFunction : IBulkWindowFunction
    {
        private readonly Func<EventBatchData, int, IDataValue> _valueFunction;
        private readonly Func<EventBatchData, int, IDataValue>? _defaultFunction;
        private readonly long _offset;

        private BulkWindowForwardPartitionReader? _lookahead;
        private IMemoryAllocator? _memoryAllocator;

        private long _currentPosition;
        private long _lookaheadPosition;
        private bool _lookaheadStarted;
        private bool _lookaheadDone;

        public BulkLeadWindowFunction(
            Func<EventBatchData, int, IDataValue> valueFunction,
            Func<EventBatchData, int, IDataValue>? defaultFunction,
            long offset)
        {
            _valueFunction = valueFunction;
            _defaultFunction = defaultFunction;
            _offset = offset;
        }

        public long AffectedRowsBefore => _offset;

        public long AffectedRowsAfter => 0;

        public bool StableByValueEquality => false;

        public long EqualityStableAfterRows => long.MaxValue;

        public int AuxiliaryStateColumnCount => 0;

        public Task Initialize(BulkWindowFunctionContext context)
        {
            _memoryAllocator = context.MemoryAllocator;
            _lookahead = new BulkWindowForwardPartitionReader(context.PersistentTree, context.PartitionColumns, context.CreateInsertComparer());
            return Task.CompletedTask;
        }

        public ValueTask Commit()
        {
            return ValueTask.CompletedTask;
        }

        public ValueTask StartScan(ColumnRowReference partitionValues, BulkWindowSeedReader seedReader, bool fromPartitionStart)
        {
            _currentPosition = -1;
            _lookaheadStarted = false;
            _lookaheadDone = false;
            return ValueTask.CompletedTask;
        }

        public bool TryComputeRow(BulkWindowRowContext context, DataValueContainer result)
        {
            if (_offset == 0)
            {
                _valueFunction(context.Batch, context.RowIndex).CopyToContainer(result);
                _currentPosition++;
                return true;
            }
            return false;
        }

        public async ValueTask ComputeRow(BulkWindowRowContext context, DataValueContainer result)
        {
            Debug.Assert(_lookahead != null);
            Debug.Assert(_memoryAllocator != null);

            _currentPosition++;

            if (!_lookaheadStarted)
            {
                _lookaheadStarted = true;
                await _lookahead.ResetAtRow(new ColumnRowReference() { referenceBatch = context.Batch, RowIndex = context.RowIndex }, _memoryAllocator);
                _lookaheadPosition = _currentPosition - context.DupIndex - 1;
            }

            var targetPosition = _currentPosition + _offset;
            while (!_lookaheadDone && _lookaheadPosition < targetPosition)
            {
                if (!await _lookahead.MoveNextLogical())
                {
                    _lookaheadDone = true;
                    break;
                }
                _lookaheadPosition++;
            }

            if (!_lookaheadDone && _lookaheadPosition == targetPosition)
            {
                _valueFunction(_lookahead.Batch, _lookahead.RowIndex).CopyToContainer(result);
            }
            else if (_defaultFunction != null)
            {
                _defaultFunction(context.Batch, context.RowIndex).CopyToContainer(result);
            }
            else
            {
                result._type = ArrowTypeId.Null;
            }
        }

        public ValueTask EndScan()
        {
            return ValueTask.CompletedTask;
        }
    }

    /// <summary>
    /// lead/lag with a per-row offset expression. The target row can be anywhere in the partition, so any
    /// change recomputes the whole partition and each row's target is found with a forward reader from the
    /// partition start that rewinds when a target lies behind it, matching the non bulk implementation.
    /// Offsets that are not int64 values fall back to 1 and a negative offset reaches in the other
    /// direction; targets outside the partition produce the default value.
    /// </summary>
    internal class BulkLeadLagDynamicWindowFunction : IBulkWindowFunction
    {
        private readonly Func<EventBatchData, int, IDataValue> _valueFunction;
        private readonly Func<EventBatchData, int, IDataValue> _offsetFunction;
        private readonly Func<EventBatchData, int, IDataValue>? _defaultFunction;
        private readonly bool _isLead;

        private BulkWindowForwardPartitionReader? _reader;
        private IMemoryAllocator? _memoryAllocator;

        // Scans always start at the partition start, so the anchor for reader resets is copied from the
        // scan's first row and stays valid while the reader lazily advances across pages.
        private Column[]? _anchorColumns;
        private EventBatchData? _anchorBatch;

        private long _currentPosition;
        private long _readerPosition;
        private bool _readerDone;

        public BulkLeadLagDynamicWindowFunction(
            Func<EventBatchData, int, IDataValue> valueFunction,
            Func<EventBatchData, int, IDataValue> offsetFunction,
            Func<EventBatchData, int, IDataValue>? defaultFunction,
            bool isLead)
        {
            _valueFunction = valueFunction;
            _offsetFunction = offsetFunction;
            _defaultFunction = defaultFunction;
            _isLead = isLead;
        }

        public long AffectedRowsBefore => long.MaxValue;

        public long AffectedRowsAfter => long.MaxValue;

        public bool StableByValueEquality => false;

        public long EqualityStableAfterRows => long.MaxValue;

        public int AuxiliaryStateColumnCount => 0;

        public Task Initialize(BulkWindowFunctionContext context)
        {
            _memoryAllocator = context.MemoryAllocator;
            _reader = new BulkWindowForwardPartitionReader(context.PersistentTree, context.PartitionColumns);
            return Task.CompletedTask;
        }

        public ValueTask Commit()
        {
            return ValueTask.CompletedTask;
        }

        public async ValueTask StartScan(ColumnRowReference partitionValues, BulkWindowSeedReader seedReader, bool fromPartitionStart)
        {
            Debug.Assert(_reader != null);
            Debug.Assert(_memoryAllocator != null);

            if (_anchorColumns == null)
            {
                _anchorColumns = new Column[partitionValues.referenceBatch.Columns.Count];
                for (int c = 0; c < _anchorColumns.Length; c++)
                {
                    _anchorColumns[c] = Column.Create(_memoryAllocator);
                    _anchorColumns[c].Add(NullValue.Instance);
                }
                _anchorBatch = new EventBatchData(_anchorColumns);
            }
            for (int c = 0; c < _anchorColumns.Length; c++)
            {
                _anchorColumns[c].UpdateAt(0, partitionValues.referenceBatch.Columns[c].GetValueAt(partitionValues.RowIndex, default));
            }

            _currentPosition = -1;
            _readerPosition = -1;
            _readerDone = false;
            await _reader.Reset(new ColumnRowReference() { referenceBatch = _anchorBatch!, RowIndex = 0 });
        }

        public bool TryComputeRow(BulkWindowRowContext context, DataValueContainer result)
        {
            return false;
        }

        public async ValueTask ComputeRow(BulkWindowRowContext context, DataValueContainer result)
        {
            Debug.Assert(_reader != null);

            _currentPosition++;

            long offset = 1;
            var offsetValue = _offsetFunction(context.Batch, context.RowIndex);
            if (offsetValue.Type == ArrowTypeId.Int64)
            {
                offset = offsetValue.AsLong;
            }
            var targetPosition = _isLead ? _currentPosition + offset : _currentPosition - offset;

            if (targetPosition >= 0)
            {
                if (_readerPosition > targetPosition)
                {
                    await _reader.Reset(new ColumnRowReference() { referenceBatch = _anchorBatch!, RowIndex = 0 });
                    _readerPosition = -1;
                    _readerDone = false;
                }
                while (!_readerDone && _readerPosition < targetPosition)
                {
                    if (await _reader.MoveNextLogical())
                    {
                        _readerPosition++;
                    }
                    else
                    {
                        _readerDone = true;
                    }
                }
                if (!_readerDone && _readerPosition == targetPosition)
                {
                    _valueFunction(_reader.Batch, _reader.RowIndex).CopyToContainer(result);
                    return;
                }
            }

            if (_defaultFunction != null)
            {
                _defaultFunction(context.Batch, context.RowIndex).CopyToContainer(result);
            }
            else
            {
                result._type = ArrowTypeId.Null;
            }
        }

        public ValueTask EndScan()
        {
            return ValueTask.CompletedTask;
        }
    }
}
