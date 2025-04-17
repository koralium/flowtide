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

using FlowtideDotNet.Base.Vertices.Ingress;
using FlowtideDotNet.Connector.Files.Internal.XmlFiles.XmlParsers;
using FlowtideDotNet.Core;
using FlowtideDotNet.Core.ColumnStore;
using FlowtideDotNet.Core.ColumnStore.DataValues;
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
using System.Xml;

namespace FlowtideDotNet.Connector.Files.Internal.XmlFiles
{
    internal class XmlFileDataSource : ReadBaseOperator
    {
        private readonly XmlFileInternalOptions _fileOptions;
        private readonly ReadRelation _readRelation;
        private IFlowtideXmlParser _parser;
        private readonly string _watermarkName;
        private int[] _emitList;

        private IObjectState<long>? _batchNumber;
        private IObjectState<Dictionary<string, string>>? _customState;

        private Task? _deltaLoadTask;
        private object _deltaLock = new object();

        private readonly int[] _extraColumnsIndices;

        public XmlFileDataSource(XmlFileInternalOptions fileOptions, ReadRelation readRelation, DataflowBlockOptions options) : base(options)
        {
            _fileOptions = fileOptions;
            this._readRelation = readRelation;
            _watermarkName = readRelation.NamedTable.DotSeperated;

            _parser = new SchemaToParsers().ParseElement(fileOptions.ElementName, fileOptions.XmlSchema);

            var extraColumns = _fileOptions.ExtraColumns;

            // Build up the emit list
            var emitList = new List<int>();
            _extraColumnsIndices = new int[_fileOptions.ExtraColumns.Count];
            for (int i = 0; i < _extraColumnsIndices.Length; i++)
            {
                _extraColumnsIndices[i] = -1;
            }

            if (readRelation.EmitSet)
            {
                for (int i = 0; i < readRelation.Emit.Count; i++)
                {
                    var emitindex = readRelation.Emit[i];

                    var columnName = readRelation.BaseSchema.Names[emitindex];

                    bool found = false;
                    for (int j = 0; j < _fileOptions.FlowtideSchema.Names.Count; j++)
                    {
                        if (columnName.Equals(_fileOptions.FlowtideSchema.Names[j], StringComparison.OrdinalIgnoreCase))
                        {
                            found = true;
                            emitList.Add(j);
                        }
                    }

                    if (!found)
                    {
                        throw new InvalidOperationException($"Column {columnName} not found in schema");
                    }

                    for (int j = 0; j < extraColumns.Count; j++)
                    {
                        if (columnName.Equals(extraColumns[j].ColumnName, StringComparison.OrdinalIgnoreCase))
                        {
                            _extraColumnsIndices[j] = emitindex;
                            emitList.Remove(emitList.Count - 1);
                        }
                    }

                    
                }
            }
            else
            {
                for (int i = 0; i < readRelation.BaseSchema.Names.Count; i++)
                {
                    var columnName = readRelation.BaseSchema.Names[i];

                    bool found = false;
                    for (int j = 0; j < _fileOptions.FlowtideSchema.Names.Count; j++)
                    {
                        if (columnName.Equals(_fileOptions.FlowtideSchema.Names[j], StringComparison.OrdinalIgnoreCase))
                        {
                            found = true;
                            emitList.Add(j);
                        }
                    }

                    if (!found)
                    {
                        throw new InvalidOperationException($"Column {columnName} not found in schema");
                    }

                    for (int j = 0; j < extraColumns.Count; j++)
                    {
                        if (columnName.Equals(extraColumns[j].ColumnName, StringComparison.OrdinalIgnoreCase))
                        {
                            _extraColumnsIndices[j] = i;
                            emitList.Remove(emitList.Count - 1);
                        }
                    }
                }
            }

            _emitList = emitList.ToArray();
        }

        public override string DisplayName => $"XmlFile({_watermarkName})";

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
            
            if(files.Count == 0)
            {
                return;
            }
            bool sentData = false;
            Column[] columns = new Column[_readRelation.BaseSchema.Names.Count];
            for (int i = 0; i < _readRelation.BaseSchema.Names.Count; i++)
            {
                columns[i] = Column.Create(MemoryAllocator);
            }
            var weights = new PrimitiveList<int>(MemoryAllocator);
            var iterations = new PrimitiveList<uint>(MemoryAllocator);

            foreach (var file in files)
            {
                using var stream = await _fileOptions.FileStorage.OpenRead(file);

                if (stream == null)
                {
                    throw new InvalidOperationException($"File {file} not found");
                }

                var reader = XmlReader.Create(stream, new XmlReaderSettings()
                {
                    Async = true
                });

                while (await reader.ReadAsync())
                {
                    if (reader.LocalName.Equals(_fileOptions.ElementName, StringComparison.OrdinalIgnoreCase))
                    {
                        var val = await _parser.Parse(reader);

                        if (val is StructValue structValue)
                        {
                            for (int i = 0; i < _emitList.Length; i++)
                            {
                                columns[i].Add(structValue.GetAt(_emitList[i]));
                            }
                        }
                        else
                        {
                            for (int i = 0; i < _emitList.Length; i++)
                            {
                                columns[i].Add(NullValue.Instance);
                            }
                        }

                        // Add any extra column values
                        for (int i = 0; i < _extraColumnsIndices.Length; i++)
                        {
                            if (_extraColumnsIndices[i] >= 0)
                            {
                                columns[_extraColumnsIndices[i]].Add(_fileOptions.ExtraColumns[i].GetValueFunction(file, _customState.Value));
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
                            columns = new Column[_emitList.Length];
                            for (int i = 0; i < _emitList.Length; i++)
                            {
                                columns[i] = Column.Create(MemoryAllocator);
                            }
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

            _batchNumber.Value = nextBatchId;

            if (sentData)
            {
                await output.SendWatermark(new Base.Watermark(_watermarkName, _batchNumber.Value));
                ScheduleCheckpoint(TimeSpan.FromMilliseconds(1));
            }
            output.ExitCheckpointLock();

        }

        protected override Task<IReadOnlySet<string>> GetWatermarkNames()
        {
            return Task.FromResult<IReadOnlySet<string>>(new HashSet<string>() { _watermarkName });
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

            Column[] columns = new Column[_readRelation.BaseSchema.Names.Count];
            for (int i = 0; i < _readRelation.BaseSchema.Names.Count; i++)
            {
                columns[i] = Column.Create(MemoryAllocator);
            }
            var weights = new PrimitiveList<int>(MemoryAllocator);
            var iterations = new PrimitiveList<uint>(MemoryAllocator);

            foreach (var initialFile in files)
            {
                using var stream = await _fileOptions.FileStorage.OpenRead(initialFile);

                if (stream == null)
                {
                    throw new InvalidOperationException($"File {initialFile} not found");
                }

                var reader = XmlReader.Create(stream, new XmlReaderSettings()
                {
                    Async = true
                });

                while (await reader.ReadAsync())
                {
                    if (reader.LocalName.Equals(_fileOptions.ElementName, StringComparison.OrdinalIgnoreCase))
                    {
                        var val = await _parser.Parse(reader);

                        if (val is StructValue structValue)
                        {
                            for (int i = 0; i < _emitList.Length; i++)
                            {
                                columns[i].Add(structValue.GetAt(_emitList[i]));
                            }
                        }
                        else
                        {
                            for (int i = 0; i < _emitList.Length; i++)
                            {
                                columns[i].Add(NullValue.Instance);
                            }
                        }
                        // Add any extra column values
                        for (int i = 0; i < _extraColumnsIndices.Length; i++)
                        {
                            if (_extraColumnsIndices[i] >= 0)
                            {
                                columns[_extraColumnsIndices[i]].Add(_fileOptions.ExtraColumns[i].GetValueFunction(initialFile, _customState.Value));
                            }
                        }
                        weights.Add(1);
                        iterations.Add(0);

                        if (weights.Count >= 100)
                        {
                            await output.SendAsync(new StreamEventBatch(new EventBatchWeighted(weights, iterations, new EventBatchData(columns))));
                            weights = new PrimitiveList<int>(MemoryAllocator);
                            iterations = new PrimitiveList<uint>(MemoryAllocator);
                            columns = new Column[_emitList.Length];
                            for (int i = 0; i < _emitList.Length; i++)
                            {
                                columns[i] = Column.Create(MemoryAllocator);
                            }
                        }
                    }
                }
            }

            if (weights.Count > 0)
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

            await output.SendWatermark(new Base.Watermark(_watermarkName, 1));
            ScheduleCheckpoint(TimeSpan.FromMilliseconds(1));

            if (_fileOptions.DeltaGetNextFiles != null)
            {
                await RegisterTrigger("delta_load", _fileOptions.DeltaInterval);
            }

            output.ExitCheckpointLock();
        }
    }
}
