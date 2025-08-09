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

using FlowtideDotNet.Base;
using FlowtideDotNet.Base.Vertices.Ingress;
using FlowtideDotNet.Core;
using FlowtideDotNet.Core.ColumnStore;
using FlowtideDotNet.Core.Operators.Read;
using FlowtideDotNet.Storage.DataStructures;
using FlowtideDotNet.Storage.StateManager;
using FlowtideDotNet.Substrait.Relations;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;
using static SqlParser.Ast.AssignmentTarget;

namespace FlowtideDotNet.Connector.Files.Internal.TextLineFiles
{
    internal class TextLineFileDataSource : ReadBaseOperator
    {
        private readonly TextLineInternalOptions _fileOptions;
        private readonly ReadRelation _readRelation;
        private readonly string _watermarkName;

        private IObjectState<long>? _batchNumber;
        private IObjectState<Dictionary<string, string>>? _customState;

        private readonly int _fileNameIndex;
        private readonly int _valueIndex;
        private readonly int _outputCount;
        private readonly int[] _extraColumnsIndices;

        private Task? _deltaLoadTask;
        private object _deltaLock = new object();

        public TextLineFileDataSource(TextLineInternalOptions fileOptions, ReadRelation readRelation, DataflowBlockOptions options) : base(options)
        {
            _fileOptions = fileOptions;
            this._readRelation = readRelation;
            _watermarkName = readRelation.NamedTable.DotSeperated;

            _fileNameIndex = -1;
            _valueIndex = -1;

            var extraColumns = _fileOptions.ExtraColumns;
            _extraColumnsIndices = new int[extraColumns.Count];
            for (int i = 0; i < readRelation.BaseSchema.Names.Count; i++)
            {
                var columnName = readRelation.BaseSchema.Names[i];
                if (columnName.Equals("fileName", StringComparison.OrdinalIgnoreCase))
                {
                    _fileNameIndex = i;
                    _outputCount++;
                }
                if (columnName.Equals("value", StringComparison.OrdinalIgnoreCase))
                {
                    _valueIndex = i;
                    _outputCount++;
                }
                for (int j = 0; j < extraColumns.Count; j++)
                {
                    if (columnName.Equals(extraColumns[j].ColumnName, StringComparison.OrdinalIgnoreCase))
                    {
                        _extraColumnsIndices[j] = i;
                        _outputCount++;
                    }
                }
            }
        }

        public override string DisplayName => $"TextLines({_watermarkName})";

        public override Task DeleteAsync()
        {
            return Task.CompletedTask;
        }

        public override Task OnTrigger(string triggerName, object? state)
        {
            if (triggerName == "delta_load")
            {
                lock (_deltaLock)
                {
                    if (_deltaLoadTask == null)
                    {
                        _deltaLoadTask = RunTask(HandleDelta)
                            .ContinueWith((t) =>
                            {
                                lock (_deltaLock)
                                {
                                    _deltaLoadTask = null;
                                }
                            });
                    }
                }
            }
            return Task.CompletedTask;
        }

        private async Task HandleDelta(IngressOutput<StreamEventBatch> output, object? state)
        {
            Debug.Assert(_batchNumber != null);
            Debug.Assert(_customState != null);
            Debug.Assert(_customState.Value != null);
            Debug.Assert(_fileOptions.DeltaGetNextFiles != null);

            await output.EnterCheckpointLock();

            var nextBatchId = _batchNumber.Value + 1;

            if (_fileOptions.BeforeBatch != null)
            {
                await _fileOptions.BeforeBatch(nextBatchId, _customState.Value, _fileOptions.FileStorage);
            }

            var files = (await _fileOptions.DeltaGetNextFiles(_fileOptions.FileStorage, _batchNumber.Value, _customState.Value)).ToList();

            if (files.Count == 0)
            {
                return;
            }

            _batchNumber.Value = nextBatchId;

            bool sentData = false;
            Column[] columns = new Column[_outputCount];
            for (int i = 0; i < _outputCount; i++)
            {
                columns[i] = Column.Create(MemoryAllocator);
            }
            var weights = new PrimitiveList<int>(MemoryAllocator);
            var iterations = new PrimitiveList<uint>(MemoryAllocator);

            foreach (var file in files)
            {
                var fileNameValue = new StringValue(file);
                using var stream = await _fileOptions.FileStorage.OpenRead(file);

                if (stream == null)
                {
                    throw new InvalidOperationException($"File {file} not found");
                }
                using var reader = new StreamReader(stream);

                string? row;
                while ((row = await reader.ReadLineAsync()) != null)
                {
                    if (_fileNameIndex >= 0)
                    {
                        columns[_fileNameIndex].Add(fileNameValue);
                    }
                    if (_valueIndex >= 0)
                    {
                        columns[_valueIndex].Add(new StringValue(row));
                    }
                    // Add any extra column values
                    for (int i = 0; i < _extraColumnsIndices.Length; i++)
                    {
                        if (_extraColumnsIndices[i] >= 0)
                        {
                            columns[_extraColumnsIndices[i]].Add(_fileOptions.ExtraColumns[i].GetValueFunction(file, nextBatchId, _customState.Value));
                        }
                    }

                    weights.Add(1);
                    iterations.Add(0);

                    if (weights.Count >= 100)
                    {
                        sentData = true;
                        await output.SendAsync(new StreamEventBatch(new EventBatchWeighted(weights, iterations, new EventBatchData(columns))));
                        weights = new PrimitiveList<int>(MemoryAllocator);
                        iterations = new PrimitiveList<uint>(MemoryAllocator);
                        columns = new Column[_outputCount];
                        for (int i = 0; i < _outputCount; i++)
                        {
                            columns[i] = Column.Create(MemoryAllocator);
                        }
                    }
                }
            }

            if (weights.Count > 0)
            {
                sentData = true;
                await output.SendAsync(new StreamEventBatch(new EventBatchWeighted(weights, iterations, new EventBatchData(columns))));
            }
            else
            {
                weights.Dispose();
                iterations.Dispose();
                for (int i = 0; i < columns.Length; i++)
                {
                    columns[i].Dispose();
                }
            }

            if (sentData)
            {
                await output.SendWatermark(new Base.Watermark(_watermarkName, LongWatermarkValue.Create(_batchNumber.Value)));
                ScheduleCheckpoint(TimeSpan.FromMilliseconds(1));
            }
            output.ExitCheckpointLock();
        }

        protected override Task<IReadOnlySet<string>> GetWatermarkNames()
        {
            return Task.FromResult<IReadOnlySet<string>>(new HashSet<string> { _watermarkName });
        }

        protected override async Task InitializeOrRestore(long restoreTime, IStateManagerClient stateManagerClient)
        {
            _batchNumber = await stateManagerClient.GetOrCreateObjectStateAsync<long>("batchNumber");
            _customState = await stateManagerClient.GetOrCreateObjectStateAsync<Dictionary<string, string>>("customState");

            if (_customState.Value == null)
            {
                _customState.Value = new Dictionary<string, string>();
            }
        }

        protected override async Task OnCheckpoint(long checkpointTime)
        {
            Debug.Assert(_batchNumber != null);
            Debug.Assert(_customState != null);

            await _batchNumber.Commit();
            await _customState.Commit();
        }

        protected override async Task SendInitial(IngressOutput<StreamEventBatch> output)
        {
            Debug.Assert(_batchNumber != null);
            Debug.Assert(_customState != null);
            Debug.Assert(_customState.Value != null);

            if (_batchNumber.Value > 0)
            {
                return;
            }

            await output.EnterCheckpointLock();

            _batchNumber.Value = 1;
            if (_fileOptions.BeforeBatch != null)
            {
                await _fileOptions.BeforeBatch(_batchNumber.Value, _customState.Value, _fileOptions.FileStorage);
            }

            var files = await _fileOptions.GetInitialFiles(_fileOptions.FileStorage, _customState.Value);

            Column[] columns = new Column[_outputCount];
            for (int i = 0; i < _outputCount; i++)
            {
                columns[i] = Column.Create(MemoryAllocator);
            }
            var weights = new PrimitiveList<int>(MemoryAllocator);
            var iterations = new PrimitiveList<uint>(MemoryAllocator);

            foreach(var initialFile in files)
            {
                var fileNameValue = new StringValue(initialFile);
                using var stream = await _fileOptions.FileStorage.OpenRead(initialFile);

                if (stream == null)
                {
                    throw new InvalidOperationException($"File {initialFile} not found");
                }

                using var reader = new StreamReader(stream);

                string? row;
                while ((row = await reader.ReadLineAsync()) != null)
                {
                    if (_fileNameIndex >= 0)
                    {
                        columns[_fileNameIndex].Add(fileNameValue);
                    }
                    if (_valueIndex >= 0)
                    {
                        columns[_valueIndex].Add(new StringValue(row));
                    }
                    // Add any extra column values
                    for (int i = 0; i < _extraColumnsIndices.Length; i++)
                    {
                        if (_extraColumnsIndices[i] >= 0)
                        {
                            columns[_extraColumnsIndices[i]].Add(_fileOptions.ExtraColumns[i].GetValueFunction(initialFile, _batchNumber.Value, _customState.Value));
                        }
                    }

                    weights.Add(1);
                    iterations.Add(0);

                    if (weights.Count >= 100)
                    {
                        await output.SendAsync(new StreamEventBatch(new EventBatchWeighted(weights, iterations, new EventBatchData(columns))));
                        weights = new PrimitiveList<int>(MemoryAllocator);
                        iterations = new PrimitiveList<uint>(MemoryAllocator);
                        columns = new Column[_outputCount];
                        for (int i = 0; i < _outputCount; i++)
                        {
                            columns[i] = Column.Create(MemoryAllocator);
                        }
                    }
                }
            }

            if (weights.Count >= 0)
            {
                await output.SendAsync(new StreamEventBatch(new EventBatchWeighted(weights, iterations, new EventBatchData(columns))));
            }
            else
            {
                weights.Dispose();
                iterations.Dispose();
                for (int i = 0; i < columns.Length; i++)
                {
                    columns[i].Dispose();
                }
            }

            await output.SendWatermark(new Base.Watermark(_watermarkName, LongWatermarkValue.Create(_batchNumber.Value)));
            ScheduleCheckpoint(TimeSpan.FromMilliseconds(1));

            if (_fileOptions.DeltaGetNextFiles != null)
            {
                await RegisterTrigger("delta_load", _fileOptions.DeltaInterval);
            }

            output.ExitCheckpointLock();
        }
    }
}
