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
using FlowtideDotNet.Core.ColumnStore.ObjectConverter;
using FlowtideDotNet.Core.Compute;
using FlowtideDotNet.Core.Operators.Read;
using FlowtideDotNet.Storage.DataStructures;
using FlowtideDotNet.Storage.StateManager;
using FlowtideDotNet.Substrait.Relations;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Threading.Tasks.Dataflow;

namespace FlowtideDotNet.Core.Sources.Generic.Internal
{
    internal class GenericReadOperator<T> : ColumnBatchReadBaseOperator
        where T: class
    {
        private readonly GenericDataSourceAsync<T> _genericDataSource;
        private readonly ReadRelation _readRelation;
        private BatchConverter _batchConverter;
        private string _watermarkName;
        private IObjectState<long>? _lastWatermark;
        private List<int> _primaryKeyIndices;
        private int _keyIndex;

        public GenericReadOperator(GenericDataSourceAsync<T> genericDataSource, ReadRelation readRelation, IFunctionsRegister functionsRegister, DataflowBlockOptions options) : base(readRelation, functionsRegister, options)
        {
            this._genericDataSource = genericDataSource;
            this._readRelation = readRelation;
            this.DeltaLoadInterval = _genericDataSource.DeltaLoadInterval;
            this.FullLoadInterval = _genericDataSource.FullLoadInterval;

            var resolver = new ObjectConverterResolver();

            // Get any custom convert resolvers and add them first in the list
            var converterResolvers = genericDataSource.GetCustomConverters().ToList();
            for (int i = converterResolvers.Count - 1; i >= 0; i--)
            {
                resolver.PrependResolver(converterResolvers[i]);
            }

            _batchConverter = BatchConverter.GetBatchConverter(typeof(T), readRelation.BaseSchema.Names.Where(x => x != "__key").ToList(), resolver);
            _watermarkName = readRelation.NamedTable.DotSeperated;

            _primaryKeyIndices = new List<int>();

            _keyIndex = readRelation.BaseSchema.Names.IndexOf("__key");
            if (_keyIndex < 0)
            {
                throw new InvalidOperationException("__key must be included in the selected names");
            }
            _primaryKeyIndices.Add(_keyIndex);

        }

        public override string DisplayName => "Generic";

        protected override async Task Checkpoint(long checkpointTime)
        {
            Debug.Assert(_lastWatermark != null);
            await _lastWatermark.Commit();
        }

        protected override async Task InitializeOrRestore(long restoreTime, IStateManagerClient stateManagerClient)
        {
            _lastWatermark = await stateManagerClient.GetOrCreateObjectStateAsync<long>("lastwatermark");
            await base.InitializeOrRestore(restoreTime, stateManagerClient);
        }

        protected override async IAsyncEnumerable<DeltaReadEvent> DeltaLoad(Func<Task> EnterCheckpointLock, Action ExitCheckpointLock, CancellationToken cancellationToken, [EnumeratorCancellation] CancellationToken enumeratorCancellationToken = default)
        {
            Debug.Assert(_lastWatermark != null);
            IColumn[] columns = new Column[_readRelation.BaseSchema.Names.Count];

            for (int i = 0; i < columns.Length; i++)
            {
                columns[i] = new Column(MemoryAllocator);
            }

            PrimitiveList<int> weights = new PrimitiveList<int>(MemoryAllocator);
            PrimitiveList<uint> iterations = new PrimitiveList<uint>(MemoryAllocator);

            await EnterCheckpointLock();

            await foreach (var ev in _genericDataSource.DeltaLoadAsync(_lastWatermark.Value))
            {
                AppendToColumns(columns, weights, iterations, ev);
                _lastWatermark.Value = ev.Watermark;

                if (weights.Count >= 100)
                {
                    yield return new DeltaReadEvent(new EventBatchWeighted(weights, iterations, new EventBatchData(columns)), new Base.Watermark(_watermarkName, _lastWatermark.Value));
                    columns = new Column[_readRelation.BaseSchema.Names.Count + 1];
                    for (int i = 0; i < columns.Length; i++)
                    {
                        columns[i] = new Column(MemoryAllocator);
                    }
                    weights = new PrimitiveList<int>(MemoryAllocator);
                    iterations = new PrimitiveList<uint>(MemoryAllocator);
                }
            }

            if (weights.Count > 0)
            {
                yield return new DeltaReadEvent(new EventBatchWeighted(weights, iterations, new EventBatchData(columns)), new Base.Watermark(_watermarkName, _lastWatermark.Value));
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

            ExitCheckpointLock();
        }

        private void AppendToColumns(IColumn[] columnsWithKeyLast, PrimitiveList<int> weights, PrimitiveList<uint> iterations, FlowtideGenericObject<T> obj)
        {
            columnsWithKeyLast[_keyIndex].Add(new StringValue(obj.Key));
            if (!obj.isDelete)
            {
                if (obj.Value == null)
                {
                    throw new InvalidOperationException("Could not convert input to column data is the input was null.");
                }
                _batchConverter.AppendToColumns(obj.Value, columnsWithKeyLast, _primaryKeyIndices);
                weights.Add(1);
            }
            else
            {
                weights.Add(-1);
            }
            iterations.Add(0);
        }

        protected override async IAsyncEnumerable<ColumnReadEvent> FullLoad(CancellationToken cancellationToken, [EnumeratorCancellation] CancellationToken enumeratorCancellationToken = default)
        {
            Debug.Assert(_lastWatermark != null);
            IColumn[] columns = new Column[_readRelation.BaseSchema.Names.Count];

            for (int i = 0; i < columns.Length; i++)
            {
                columns[i] = new Column(MemoryAllocator);
            }

            PrimitiveList<int> weights = new PrimitiveList<int>(MemoryAllocator);
            PrimitiveList<uint> iterations = new PrimitiveList<uint>(MemoryAllocator);

            await foreach (var ev in _genericDataSource.FullLoadAsync())
            {
                AppendToColumns(columns, weights, iterations, ev);
                _lastWatermark.Value = ev.Watermark;

                if (weights.Count >= 100)
                {
                    yield return new ColumnReadEvent(new EventBatchWeighted(weights, iterations, new EventBatchData(columns)), _lastWatermark.Value);
                    columns = new Column[_readRelation.BaseSchema.Names.Count];
                    for (int i = 0; i < columns.Length; i++)
                    {
                        columns[i] = new Column(MemoryAllocator);
                    }
                    weights = new PrimitiveList<int>(MemoryAllocator);
                    iterations = new PrimitiveList<uint>(MemoryAllocator);
                }
            }

            if (weights.Count > 0)
            {
                yield return new ColumnReadEvent(new EventBatchWeighted(weights, iterations, new EventBatchData(columns)), _lastWatermark.Value);
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
        }

        protected override ValueTask<List<int>> GetPrimaryKeyColumns()
        {
            return ValueTask.FromResult(_primaryKeyIndices);
        }

        protected override Task<IReadOnlySet<string>> GetWatermarkNames()
        {
            return Task.FromResult<IReadOnlySet<string>>(new HashSet<string>() { _watermarkName });
        }
    }
}
