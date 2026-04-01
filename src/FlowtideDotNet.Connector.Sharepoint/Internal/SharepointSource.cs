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
using FlowtideDotNet.Base.Vertices;
using FlowtideDotNet.Connector.Sharepoint.Internal.Decoders;
using FlowtideDotNet.Core;
using FlowtideDotNet.Core.ColumnStore;
using FlowtideDotNet.Core.Compute;
using FlowtideDotNet.Core.Operators.Read;
using FlowtideDotNet.Core.Operators.Write.Column;
using FlowtideDotNet.Storage.DataStructures;
using FlowtideDotNet.Storage.StateManager;
using FlowtideDotNet.Substrait.Relations;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;

namespace FlowtideDotNet.Connector.Sharepoint.Internal
{
    internal class SharepointSourceState
    {
        public long WatermarkVersion { get; set; }

        public string? ODataNextUrl { get; set; }

        public bool FetchFull { get; set; }
    }

    /// <summary>
    /// Sharepoint source, keeps track of rows by their row identifier.
    /// It supports reading delta changes from sharepoint, and also resync with a full load if the delta changes cannot be applied. 
    /// The source keeps track of the next url to fetch for delta changes, and if the url is not present, it will do a full load. 
    /// </summary>
    internal class SharepointSource : ColumnBatchReadBaseOperator
    {
        private readonly SharepointSourceOptions _sharepointSourceOptions;
        private readonly int _idFieldIndex;
        private readonly ReadRelation _readRelation;
        private readonly string _listId;
        private Dictionary<string, IColumnDecoder>? _decoders;
        private IObjectState<SharepointSourceState>? _state;
        private SharepointGraphListClient? _sharepointGraphListClient;

        public SharepointSource(SharepointSourceOptions sharepointSourceOptions, string listId, ReadRelation readRelation, IFunctionsRegister functionsRegister, DataflowBlockOptions options) : base(readRelation, functionsRegister, options)
        {
            _idFieldIndex = readRelation.BaseSchema.Names.FindIndex(name => name.Equals("Id", StringComparison.OrdinalIgnoreCase));
            if (_idFieldIndex < 0)
            {
                throw new NotSupportedException("Sharepoint source requires Id field to be selected.");
            }

            _sharepointSourceOptions = sharepointSourceOptions;
            _readRelation = readRelation;
            _listId = listId;
            this.DeltaLoadInterval = sharepointSourceOptions.DeltaLoadInterval;
        }

        public override string DisplayName => "SharepointList";

        protected override async Task Checkpoint(long checkpointTime)
        {
            Debug.Assert(_state != null);
            await _state.Commit();
        }

        protected override async Task InitializeOrRestore(long restoreTime, IStateManagerClient stateManagerClient)
        {
            _sharepointGraphListClient = new SharepointGraphListClient(_sharepointSourceOptions, StreamName, Name, Logger);
            await _sharepointGraphListClient.Initialize();

            _state = await stateManagerClient.GetOrCreateObjectStateAsync<SharepointSourceState>("sharepoint_state");
            if (_state.Value == null)
            {
                _state.Value = new SharepointSourceState();
            }

            _decoders = await _sharepointGraphListClient.GetColumnDecoders(_listId, _readRelation.BaseSchema.Names, stateManagerClient);
            await base.InitializeOrRestore(restoreTime, stateManagerClient);
        }

        protected override async Task DeltaLoadTrigger(IngressOutput<StreamEventBatch> output, object? state)
        {
            Debug.Assert(_state != null);
            Debug.Assert(_state.Value != null);
            
            if (_state.Value.FetchFull || _state.Value.ODataNextUrl == null)
            {
                await DoFullLoad(output);
                _state.Value.FetchFull = false;
            }
            else
            {
                await DoDeltaLoad(output);
            }
        }

        protected override async IAsyncEnumerable<DeltaReadEvent> DeltaLoad(Func<Task> EnterCheckpointLock, Action ExitCheckpointLock, CancellationToken cancellationToken, [EnumeratorCancellation] CancellationToken enumeratorCancellationToken = default)
        {
            Debug.Assert(_state?.Value != null);
            Debug.Assert(_sharepointGraphListClient != null);
            Debug.Assert(_listId != null);

            if (_state.Value.ODataNextUrl == null)
            {
                throw new InvalidOperationException("No next url to fetch");
            }

            var iterator = _sharepointGraphListClient.GetDeltaFromUrl(_listId, _state.Value.ODataNextUrl);

            Logger.BeforeCheckpointInDelta(StreamName, Name);

            await EnterCheckpointLock();

            Logger.FetchingDelta(StreamName, Name);

            IAsyncEnumerator<HandleDataRowsResult>? enumerator = null;
            try
            {
                enumerator = HandleDataRows(iterator).GetAsyncEnumerator();
            }
            catch (Exception e)
            {
                if (e is TaskCanceledException || e is OperationCanceledException)
                {
                    ExitCheckpointLock();
                    Logger.LogWarning(e, "Error fetching delta, task was cancelled. Pausing for 120 seconds.");
                    await Task.Delay(TimeSpan.FromSeconds(120));
                    yield break;
                }
                else if (e is AggregateException aggregateException)
                {
                    // Microsoft.Vroom.Exceptions.ResyncApplyDifferencesVroomException is thrown but is not accessible, so we check the name of the exception to determine if it's a resync exception.
                    // //If it is, we swallow it and let the full load happen in the next delta load trigger, which will reset the state and fetch all data.
                    if (aggregateException.InnerExceptions.Any(x => x.GetType().Name.Contains("Resync")))
                    {
                        _state.Value.FetchFull = true;
                        ExitCheckpointLock();
                        yield break;
                    }
                }
                else
                {
                    Logger.LogError(e, "Error fetching delta");
                    ExitCheckpointLock();
                    throw;
                }
            }

            if (enumerator == null)
            {
                Logger.LogError("Enumerator is null after trying to get delta data.");
                ExitCheckpointLock();
                yield break;
            }

            while(!cancellationToken.IsCancellationRequested)
            {
                try
                {
                    var movedNext = await enumerator.MoveNextAsync().ConfigureAwait(false);
                    if (!movedNext)
                    {
                        break;
                    }
                }
                catch (Exception e)
                {
                    if (e is TaskCanceledException || e is OperationCanceledException)
                    {
                        ExitCheckpointLock();
                        Logger.LogWarning(e, "Error fetching delta, task was cancelled. Pausing for 120 seconds.");
                        await Task.Delay(TimeSpan.FromSeconds(120));
                        yield break;
                    }
                    else if (e is AggregateException aggregateException)
                    {
                        // Microsoft.Vroom.Exceptions.ResyncApplyDifferencesVroomException is thrown but is not accessible, so we check the name of the exception to determine if it's a resync exception.
                        // //If it is, we swallow it and let the full load happen in the next delta load trigger, which will reset the state and fetch all data.
                        if (aggregateException.InnerExceptions.Any(x => x.GetType().Name.Contains("Resync")))
                        {
                            _state.Value.FetchFull = true;
                            ExitCheckpointLock();
                            yield break;
                        }
                    }
                    else
                    {
                        Logger.LogError(e, "Error fetching delta");
                        ExitCheckpointLock();
                        throw;
                    }
                }

                _state.Value.WatermarkVersion++;
                yield return new DeltaReadEvent(enumerator.Current.eventBatchWeighted, new Base.Watermark(_readRelation.NamedTable.DotSeperated, LongWatermarkValue.Create(_state.Value.WatermarkVersion)));
            }

            ExitCheckpointLock();
        }

        protected override async IAsyncEnumerable<ColumnReadEvent> FullLoad(CancellationToken cancellationToken, [EnumeratorCancellation] CancellationToken enumeratorCancellationToken = default)
        {
            Debug.Assert(_sharepointGraphListClient != null);
            Debug.Assert(_listId != null);
            Debug.Assert(_decoders != null);
            Debug.Assert(_state?.Value != null);

            // Combine cancellation tokens so that we can listen to both the operator cancellation and the enumerator cancellation
            using var combinedCancelToken = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, enumeratorCancellationToken);

            var iterator = _sharepointGraphListClient.GetDeltaFromList(_listId);

            _state.Value.WatermarkVersion++;
            await foreach (var batch in HandleDataRows(iterator))
            {
                combinedCancelToken.Token.ThrowIfCancellationRequested();
                yield return new ColumnReadEvent(batch.eventBatchWeighted, _state.Value.WatermarkVersion);
            }
        }

        struct HandleDataRowsResult
        {
            public EventBatchWeighted eventBatchWeighted;

            public HandleDataRowsResult(EventBatchWeighted eventBatchWeighted)
            {
                this.eventBatchWeighted = eventBatchWeighted;
            }
        }

        private async IAsyncEnumerable<HandleDataRowsResult> HandleDataRows(
            IAsyncEnumerable<Microsoft.Graph.Sites.Item.Lists.Item.Items.Delta.DeltaGetResponse> iterator)
        {
            Debug.Assert(_decoders != null);
            Debug.Assert(_state?.Value != null);
            Debug.Assert(iterator != null);

            bool calledDecodersNewBatch = false;
            await foreach (var page in iterator)
            {
                if (page == null)
                {
                    break;
                }
                if (page.Value == null)
                {
                    break;
                }

                Column[] columns = new Column[_readRelation.BaseSchema.Names.Count];
                for (int i = 0; i < columns.Length; i++)
                {
                    columns[i] = ColumnFactory.Get(MemoryAllocator);
                }
                PrimitiveList<int> weights = new PrimitiveList<int>(MemoryAllocator);
                PrimitiveList<uint> iterations = new PrimitiveList<uint>(MemoryAllocator);

                foreach (var item in page.Value)
                {
                    if (!calledDecodersNewBatch)
                    {
                        // We only call this if data exists
                        await DecodersNewBatch();
                        calledDecodersNewBatch = true;
                    }
                    for (int i = 0; i < _readRelation.BaseSchema.Names.Count; i++)
                    {
                        var name = _readRelation.BaseSchema.Names[i];
                        if (_decoders.TryGetValue(name, out var decoder))
                        {
                            await decoder.Decode(item, columns[i]);
                        }
                    }

                    int weight = 1;
                    if (item.ETag == null)
                    {
                        weight = -1;
                    }
                    weights.Add(weight);
                    iterations.Add(0);
                }

                _state.Value.ODataNextUrl = page.OdataDeltaLink;
                if (weights.Count > 0)
                {
                    yield return new HandleDataRowsResult(new EventBatchWeighted(weights, iterations, new EventBatchData(columns)));
                }
                else
                {
                    for (int i = 0; i < columns.Length; i++)
                    {
                        columns[i].Dispose();
                    }
                    weights.Dispose();
                    iterations.Dispose();
                }
                
            }
        }

        private async Task DecodersNewBatch()
        {
            Debug.Assert(_decoders != null);

            foreach (var decoder in _decoders)
            {
                await decoder.Value.OnNewBatch();
            }
        }

        protected override ValueTask<List<int>> GetPrimaryKeyColumns()
        {
            return ValueTask.FromResult(new List<int>() { _idFieldIndex });
        }

        protected override Task<IReadOnlySet<string>> GetWatermarkNames()
        {
            return Task.FromResult<IReadOnlySet<string>>(new HashSet<string>() { _readRelation.NamedTable.DotSeperated });
        }
    }
}
