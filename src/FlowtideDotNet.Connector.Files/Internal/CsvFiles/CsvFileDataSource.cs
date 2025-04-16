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

using CsvHelper;
using CsvHelper.Configuration;
using FlowtideDotNet.Base.Vertices.Ingress;
using FlowtideDotNet.Core;
using FlowtideDotNet.Core.ColumnStore;
using FlowtideDotNet.Core.Operators.Read;
using FlowtideDotNet.Storage.DataStructures;
using FlowtideDotNet.Storage.StateManager;
using FlowtideDotNet.Substrait.Relations;
using FlowtideDotNet.Substrait.Type;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Globalization;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;

namespace FlowtideDotNet.Connector.Files.Internal.CsvFiles
{
    internal class CsvFileDataSource : ReadBaseOperator
    {
        private readonly CsvFileInternalOptions _fileOptions;
        private readonly ReadRelation _readRelation;
        private readonly string _watermarkName;
        private List<int> initialCsvRowsToOutput;
        private List<int> deltaCsvRowsToOutput;
        private IObjectState<long>? _batchNumber;
        private IObjectState<Dictionary<string, string>>? _customState;
        private IObjectState<string>? _lastLoadedFile;
        private Task? _deltaLoadTask;
        private object _deltaLock = new object();

        /// <summary>
        /// The convert functions that take the string values and convert them into the output data types.
        /// </summary>
        private Action<string?, IColumn>[] _convertFunctions;

        private List<int> emitList;

        public CsvFileDataSource(CsvFileInternalOptions fileOptions, ReadRelation readRelation, DataflowBlockOptions options) : base(options)
        {
            _fileOptions = fileOptions;
            _readRelation = readRelation;

            _watermarkName = readRelation.NamedTable.DotSeperated;

            initialCsvRowsToOutput = new List<int>();
            for (int i = 0; i < fileOptions.CsvColumns.Count; i++)
            {
                initialCsvRowsToOutput.Add(FindOutputSchemaNameIndex(fileOptions.CsvColumns[i]));
            }
            deltaCsvRowsToOutput = new List<int>();
            for (int i = 0; i < fileOptions.DeltaCsvColumns.Count; i++)
            {
                deltaCsvRowsToOutput.Add(FindOutputSchemaNameIndex(fileOptions.DeltaCsvColumns[i]));
            }


            // Map the emit data against the output schema
            emitList = new List<int>();
            if (readRelation.EmitSet)
            {
                for (int i = 0; i < readRelation.Emit.Count; i++)
                {
                    var outputName = readRelation.BaseSchema.Names[readRelation.Emit[i]];
                    var newEmitIndex = FindOutputSchemaNameIndex(outputName);

                    if (newEmitIndex < 0)
                    {
                        throw new InvalidOperationException($"Column name {outputName} not found in output schema {_fileOptions.OutputSchema}");
                    }
                    emitList.Add(newEmitIndex);
                }
            }
            else
            {
                for (int i = 0; i < readRelation.BaseSchema.Names.Count; i++)
                {
                    var outputName = readRelation.BaseSchema.Names[i];
                    var newEmitIndex = FindOutputSchemaNameIndex(outputName);

                    if (newEmitIndex < 0)
                    {
                        throw new InvalidOperationException($"Column name {outputName} not found in output schema {_fileOptions.OutputSchema}");
                    }
                    emitList.Add(newEmitIndex);
                }
            }

            // Go through the emit list and create the constructors
            _convertFunctions = new Action<string?, IColumn>[emitList.Count];
            for (int i = 0; i < emitList.Count; i++)
            {
                if (_fileOptions.OutputSchema.Struct != null)
                {
                    var type = _fileOptions.OutputSchema.Struct.Types[emitList[i]];
                    _convertFunctions[i] = StringToDataTypeConverter.GetConvertFunction(type);
                }
                else
                {
                    _convertFunctions[i] = StringToDataTypeConverter.GetConvertFunction(AnyType.Instance);
                }
            }
        }

        private int FindOutputSchemaNameIndex(string name)
        {
            for (int j = 0; j < _fileOptions.OutputSchema.Names.Count; j++)
            {
                if (name.Equals(_fileOptions.OutputSchema.Names[j], StringComparison.OrdinalIgnoreCase))
                {
                    return j;
                }
            }
            return -1;
        }

        public override string DisplayName => $"CsvFile({_watermarkName})";

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
                        _deltaLoadTask = RunTask(LoadDelta)
                            .ContinueWith((t) =>
                            {
                                lock (_deltaLock)
                                {
                                    _deltaLoadTask = default;
                                }
                            });
                    }
                }
            }
            return Task.CompletedTask;
        }

        private async Task LoadDelta(IngressOutput<StreamEventBatch> output, object? state)
        {
            Debug.Assert(_batchNumber != null);
            Debug.Assert(_lastLoadedFile != null);
            Debug.Assert(_lastLoadedFile.Value != null);
            Debug.Assert(_fileOptions.DeltaGetNextFile != null);
            Debug.Assert(_customState != null);
            Debug.Assert(_customState.Value != null);

            var nextBatchId = _batchNumber.Value + 1;

            var deltaFileName = _fileOptions.DeltaGetNextFile(_lastLoadedFile.Value, nextBatchId);

            using var stream = await _fileOptions.FileStorage.OpenRead(deltaFileName);

            if (stream == null)
            {
                // File was not found, do not load any delta
                return;
            }

            await output.EnterCheckpointLock();

            if (_fileOptions.BeforeReadFile != null)
            {
                await _fileOptions.BeforeReadFile(deltaFileName, nextBatchId, _customState.Value, _fileOptions.FileStorage);
            }

            using var reader = new StreamReader(stream);

            var csvConfig = new CsvConfiguration(CultureInfo.InvariantCulture)
            {
                Delimiter = _fileOptions.Delimiter,
            };

            using var csv = new CsvReader(reader, csvConfig);

            if (_fileOptions.FilesHaveHeader)
            {
                await csv.ReadAsync();
                csv.ReadHeader();
            }

            string?[] rowValues = new string?[_fileOptions.DeltaCsvColumns.Count];
            string?[] outputValues = new string?[_fileOptions.OutputSchema.Names.Count];

            PrimitiveList<int> weights = new PrimitiveList<int>(MemoryAllocator);
            PrimitiveList<uint> iterations = new PrimitiveList<uint>(MemoryAllocator);
            Column[] outputColumns = new Column[emitList.Count];
            for (int i = 0; i < emitList.Count; i++)
            {
                outputColumns[i] = Column.Create(MemoryAllocator);
            }

            while (await csv.ReadAsync())
            {
                for (int i = 0; i < rowValues.Length; i++)
                {
                    rowValues[i] = csv.GetField(i);
                }
                for (int i = 0; i < deltaCsvRowsToOutput.Count; i++)
                {
                    if (deltaCsvRowsToOutput[i] >= 0)
                    {
                        outputValues[deltaCsvRowsToOutput[i]] = rowValues[i];
                    }
                }

                if (_fileOptions.ModifyRow != null)
                {
                    _fileOptions.ModifyRow(rowValues, outputValues, nextBatchId, deltaFileName, _customState.Value);
                }

                for (int i = 0; i < emitList.Count; i++)
                {
                    _convertFunctions[i](outputValues[emitList[i]], outputColumns[i]);
                }
                weights.Add(_fileOptions.DeltaWeightFunction(rowValues));
                iterations.Add(0);

                if (weights.Count >= 100)
                {
                    await output.SendAsync(new StreamEventBatch(new EventBatchWeighted(weights, iterations, new EventBatchData(outputColumns))));
                    weights = new PrimitiveList<int>(MemoryAllocator);
                    iterations = new PrimitiveList<uint>(MemoryAllocator);
                    outputColumns = new Column[emitList.Count];
                    for (int i = 0; i < emitList.Count; i++)
                    {
                        outputColumns[i] = Column.Create(MemoryAllocator);
                    }
                }
            }

            if (weights.Count > 0)
            {
                await output.SendAsync(new StreamEventBatch(new EventBatchWeighted(weights, iterations, new EventBatchData(outputColumns))));
            }
            else
            {
                weights.Dispose();
                iterations.Dispose();
                for (int i = 0; i < emitList.Count; i++)
                {
                    outputColumns[i].Dispose();
                }
            }

            _lastLoadedFile.Value = deltaFileName;
            _batchNumber.Value = nextBatchId;

            await output.SendWatermark(new Base.Watermark(_watermarkName, _batchNumber.Value));
            ScheduleCheckpoint(TimeSpan.FromMilliseconds(1));

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
            _lastLoadedFile = await stateManagerClient.GetOrCreateObjectStateAsync<string>("lastLoadedFile");

            if (_customState.Value == null)
            {
                _customState.Value = new Dictionary<string, string>();
            }
        }

        protected override Task OnCheckpoint(long checkpointTime)
        {
            return Task.CompletedTask;
        }

        protected override async Task SendInitial(IngressOutput<StreamEventBatch> output)
        {
            Debug.Assert(_batchNumber != null);
            Debug.Assert(_customState != null);
            Debug.Assert(_customState.Value != null);
            Debug.Assert(_lastLoadedFile != null);

            if (_batchNumber.Value > 0)
            {
                return;
            }

            await output.EnterCheckpointLock();
            _batchNumber.Value = 1;

            if (_fileOptions.BeforeReadFile != null)
            {
                await _fileOptions.BeforeReadFile(_fileOptions.InitialFile, _batchNumber.Value, _customState.Value, _fileOptions.FileStorage);
            }

            using var stream = await _fileOptions.FileStorage.OpenRead(_fileOptions.InitialFile);

            if (stream == null)
            {
                throw new InvalidOperationException($"File {_fileOptions.InitialFile} not found");
            }

            using var reader = new StreamReader(stream);

            var csvConfig = new CsvConfiguration(CultureInfo.InvariantCulture)
            {
                Delimiter = _fileOptions.Delimiter,
            };
            using var csv = new CsvReader(reader, csvConfig);

            if (_fileOptions.FilesHaveHeader)
            {
                await csv.ReadAsync();
                csv.ReadHeader();
            }

            string?[] rowValues = new string?[_fileOptions.CsvColumns.Count];
            string?[] outputValues = new string?[_fileOptions.OutputSchema.Names.Count];

            PrimitiveList<int> weights = new PrimitiveList<int>(MemoryAllocator);
            PrimitiveList<uint> iterations = new PrimitiveList<uint>(MemoryAllocator);
            Column[] outputColumns = new Column[emitList.Count];
            for (int i = 0; i < emitList.Count; i++)
            {
                outputColumns[i] = Column.Create(MemoryAllocator);
            }

            while (await csv.ReadAsync())
            {
                for (int i = 0; i < csv.ColumnCount; i++)
                {
                    rowValues[i] = csv.GetField(i);
                }

                for (int i = 0; i < initialCsvRowsToOutput.Count; i++)
                {
                    if (initialCsvRowsToOutput[i] >= 0)
                    {
                        outputValues[initialCsvRowsToOutput[i]] = rowValues[i];
                    }
                }

                if (_fileOptions.ModifyRow != null)
                {
                    _fileOptions.ModifyRow(rowValues, outputValues, _batchNumber.Value, _fileOptions.InitialFile, _customState.Value);
                }

                for (int i = 0; i < emitList.Count; i++)
                {
                    _convertFunctions[i](outputValues[emitList[i]], outputColumns[i]);
                }
                weights.Add(_fileOptions.InitialWeightFunction(rowValues));
                iterations.Add(0);

                if (weights.Count >= 100)
                {
                    await output.SendAsync(new StreamEventBatch(new EventBatchWeighted(weights, iterations, new EventBatchData(outputColumns))));
                    weights = new PrimitiveList<int>(MemoryAllocator);
                    iterations = new PrimitiveList<uint>(MemoryAllocator);
                    outputColumns = new Column[emitList.Count];
                    for (int i = 0; i < emitList.Count; i++)
                    {
                        outputColumns[i] = Column.Create(MemoryAllocator);
                    }
                }
            }

            if (weights.Count > 0)
            {
                await output.SendAsync(new StreamEventBatch(new EventBatchWeighted(weights, iterations, new EventBatchData(outputColumns))));
            }
            else
            {
                weights.Dispose();
                iterations.Dispose();
                for (int i = 0; i < emitList.Count; i++)
                {
                    outputColumns[i].Dispose();
                }
            }

            _lastLoadedFile.Value = _fileOptions.InitialFile;

            await output.SendWatermark(new Base.Watermark(_watermarkName, _batchNumber.Value));
            ScheduleCheckpoint(TimeSpan.FromMilliseconds(1));

            if (_fileOptions.DeltaGetNextFile != null)
            {
                await RegisterTrigger("delta_load", _fileOptions.DeltaInterval);
            }
            output.ExitCheckpointLock();


        }
    }
}
