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
using FlowtideDotNet.Core.ColumnStore.ObjectConverter;
using FlowtideDotNet.Core.Operators.Write;
using FlowtideDotNet.Core.Operators.Write.Column;
using FlowtideDotNet.Storage.StateManager;
using FlowtideDotNet.Substrait.Relations;
using System.Threading.Tasks.Dataflow;

namespace FlowtideDotNet.Core.Sources.Generic.Internal
{
    class GenericWriteOperator<T> : ColumnGroupedWriteOperator
    {
        private readonly GenericDataSink<T> _genericDataSink;
        private readonly WriteRelation _writeRelation;
        private readonly BatchConverter _batchConverter;

        public GenericWriteOperator(
            GenericDataSink<T> genericDataSink,
            ExecutionMode executionMode,
            WriteRelation writeRelation,
            ExecutionDataflowBlockOptions executionDataflowBlockOptions)
            : base(executionMode, writeRelation, executionDataflowBlockOptions)
        {
            this._genericDataSink = genericDataSink;
            this._writeRelation = writeRelation;

            var resolver = new ObjectConverterResolver();

            // Get any custom convert resolvers and add them first in the list
            var converterResolvers = genericDataSink.GetCustomConverters().ToList();
            for (int i = converterResolvers.Count - 1; i >= 0; i--)
            {
                resolver.PrependResolver(converterResolvers[i]);
            }

            _batchConverter = BatchConverter.GetBatchConverter(typeof(T), writeRelation.TableSchema.Names, resolver);
        }

        public override string DisplayName => $"GenericDataSink(Name={_writeRelation.NamedObject.DotSeperated})";

        protected override void Checkpoint(long checkpointTime)
        {
        }

        protected override async ValueTask<IReadOnlyList<int>> GetPrimaryKeyColumns()
        {
            var primaryKeyColumnNames = await _genericDataSink.GetPrimaryKeyNames();

            List<int> primaryKeyIndices = new List<int>();
            foreach (var primaryKeyColumnName in primaryKeyColumnNames)
            {
                var index = _writeRelation.TableSchema.Names.FindIndex(x => x.Equals(primaryKeyColumnName, StringComparison.OrdinalIgnoreCase));
                if (index == -1)
                {
                    throw new InvalidOperationException($"Primary key column {primaryKeyColumnName} not found in table schema");
                }
                primaryKeyIndices.Add(index);
            }
            return primaryKeyIndices;
        }

        protected override async Task InitializeOrRestore(long restoreTime, IStateManagerClient stateManagerClient)
        {
            await _genericDataSink.Initialize(_writeRelation);
            await base.InitializeOrRestore(restoreTime, stateManagerClient);
        }

        private async IAsyncEnumerable<FlowtideGenericWriteObject<T>> ChangesToGeneric(IAsyncEnumerable<ColumnWriteOperation> rows)
        {
            await foreach (var row in rows)
            {
                var obj = (T)_batchConverter.ConvertToDotNetObject(row.EventBatchData.Columns, row.Index);
                if (obj == null)
                {
                    throw new InvalidOperationException("Could not convert row to generic object");
                }
                yield return new FlowtideGenericWriteObject<T>(obj, row.IsDeleted);
            }
        }

        protected override Task UploadChanges(IAsyncEnumerable<ColumnWriteOperation> rows, Watermark watermark, bool isInitialData, CancellationToken cancellationToken)
        {
            return _genericDataSink.OnChanges(ChangesToGeneric(rows), watermark, isInitialData, cancellationToken);
        }
    }
}
