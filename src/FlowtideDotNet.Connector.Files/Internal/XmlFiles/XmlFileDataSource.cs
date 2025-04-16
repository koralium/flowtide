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

        public XmlFileDataSource(XmlFileInternalOptions fileOptions, ReadRelation readRelation, DataflowBlockOptions options) : base(options)
        {
            _fileOptions = fileOptions;
            this._readRelation = readRelation;
            _watermarkName = readRelation.NamedTable.DotSeperated;

            _parser = new SchemaToParsers().ParseElement(fileOptions.ElementName, fileOptions.XmlSchema);

            // Build up the emit list
            var emitList = new List<int>();
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
            return Task.CompletedTask;
        }

        protected override Task<IReadOnlySet<string>> GetWatermarkNames()
        {
            return Task.FromResult<IReadOnlySet<string>>(new HashSet<string>() { _watermarkName });
        }

        protected override Task InitializeOrRestore(long restoreTime, IStateManagerClient stateManagerClient)
        {
            return Task.CompletedTask;
        }

        protected override Task OnCheckpoint(long checkpointTime)
        {
            return Task.CompletedTask;
        }

        protected override async Task SendInitial(IngressOutput<StreamEventBatch> output)
        {
            await output.EnterCheckpointLock();
            using var stream = await _fileOptions.FileStorage.OpenRead(_fileOptions.InitialFile);

            if (stream == null)
            {
                throw new InvalidOperationException($"File {_fileOptions.InitialFile} not found");
            }

            var reader = XmlReader.Create(stream, new XmlReaderSettings()
            {
                Async = true
            });

            Column[] columns = new Column[_emitList.Length];
            for (int i = 0; i < _emitList.Length; i++)
            {
                columns[i] = Column.Create(MemoryAllocator);
            }
            var weights = new PrimitiveList<int>(MemoryAllocator);
            var iterations = new PrimitiveList<uint>(MemoryAllocator);

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

            output.ExitCheckpointLock();
        }
    }
}
