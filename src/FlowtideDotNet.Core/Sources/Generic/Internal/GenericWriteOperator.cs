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
using FlowtideDotNet.Core.Operators.Write;
using FlowtideDotNet.Storage.StateManager;
using FlowtideDotNet.Substrait.Relations;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Text.Json;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;

namespace FlowtideDotNet.Core.Sources.Generic.Internal
{
    internal class GenericWriteOperator<T> : SimpleGroupedWriteOperator
    {
        private readonly GenericDataSink<T> genericDataSink;
        private readonly WriteRelation writeRelation;
        private readonly StreamEventToJson streamEventToJson;

        public GenericWriteOperator(GenericDataSink<T> genericDataSink, WriteRelation writeRelation, ExecutionMode executionMode, ExecutionDataflowBlockOptions executionDataflowBlockOptions) 
            : base(executionMode, executionDataflowBlockOptions)
        {
            this.genericDataSink = genericDataSink;
            this.writeRelation = writeRelation;
            streamEventToJson = new StreamEventToJson(writeRelation.TableSchema.Names);
        }

        public override string DisplayName => $"GenericDataSink(Name={writeRelation.NamedObject.DotSeperated})";

        protected override async Task<MetadataResult> SetupAndLoadMetadataAsync()
        {
            var primaryKeyColumnNames = await genericDataSink.GetPrimaryKeyNames();
            
            List<int> primaryKeyIndices = new List<int>();
            foreach (var primaryKeyColumnName in primaryKeyColumnNames)
            {
                var index = writeRelation.TableSchema.Names.FindIndex(x => x.Equals(primaryKeyColumnName, StringComparison.OrdinalIgnoreCase));
                if (index == -1)
                {
                    throw new InvalidOperationException($"Primary key column {primaryKeyColumnName} not found in table schema");
                }
                primaryKeyIndices.Add(index);
            }
            return new MetadataResult(primaryKeyIndices);
        }

        protected override async Task InitializeOrRestore(long restoreTime, SimpleWriteState? state, IStateManagerClient stateManagerClient)
        {
            await genericDataSink.Initialize(writeRelation);
            await base.InitializeOrRestore(restoreTime, state, stateManagerClient);
        }

        private async IAsyncEnumerable<FlowtideGenericWriteObject<T>> ChangesToGeneric(IAsyncEnumerable<SimpleChangeEvent> rows)
        {
            await foreach(var row in rows)
            {
                using var memoryStream = new MemoryStream();
                streamEventToJson.Write(memoryStream, row.Row);
                memoryStream.Position = 0;
                var obj = JsonSerializer.Deserialize<T>(memoryStream, new JsonSerializerOptions()
                {
                    PropertyNameCaseInsensitive = true
                });
                if (obj == null)
                {
                    throw new InvalidOperationException("Could not convert row to generic object");
                }
                yield return new FlowtideGenericWriteObject<T>(obj, row.IsDeleted);
            }
        }

        protected override async Task UploadChanges(IAsyncEnumerable<SimpleChangeEvent> rows, Watermark watermark, CancellationToken cancellationToken)
        {
            await genericDataSink.OnChanges(ChangesToGeneric(rows));
        }
    }
}
