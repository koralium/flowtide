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

using FlexBuffers;
using FlowtideDotNet.Base.Vertices.Ingress;
using FlowtideDotNet.Connector.Sharepoint.Internal.Decoders;
using FlowtideDotNet.Core;
using FlowtideDotNet.Core.Operators.Read;
using FlowtideDotNet.Storage.StateManager;
using FlowtideDotNet.Substrait.Relations;
using Microsoft.Extensions.Logging;
using System.Diagnostics;
using System.Threading.Tasks.Dataflow;

namespace FlowtideDotNet.Connector.Sharepoint.Internal
{
    internal class SharepointSourceState {
        public long WatermarkVersion { get; set; }

        public string? ODataNextUrl { get; set; }

        public bool FetchedInitial { get; set; }
    }

    internal class SharepointSource : ReadBaseOperator
    {
        private readonly SharepointSourceOptions sharepointSourceOptions;
        private readonly ReadRelation readRelation;
        private SharepointGraphListClient? sharepointGraphListClient;
        private readonly string listId;
        private Dictionary<string, IColumnDecoder>? _decoders;
        private IObjectState<SharepointSourceState>? _state;
        private Task? _changesTask;

        public SharepointSource(SharepointSourceOptions sharepointSourceOptions, string listId, ReadRelation readRelation, DataflowBlockOptions options) : base(options)
        {
            this.sharepointSourceOptions = sharepointSourceOptions;
            this.readRelation = readRelation;
            this.listId = listId;
        }

        public override string DisplayName => "SharepointList";

        public override Task DeleteAsync()
        {
            return Task.CompletedTask;
        }

        public override Task OnTrigger(string triggerName, object? state)
        {
            if (triggerName == "fetch_delta" && (_changesTask == null || _changesTask.IsCompleted))
            {
                _changesTask = RunTask(FetchDelta);
            }
            return Task.CompletedTask;
        }

        private async Task FetchDelta(IngressOutput<StreamEventBatch> output, object? state)
        {
            Debug.Assert(_state?.Value != null);
            Debug.Assert(sharepointGraphListClient != null);
            Debug.Assert(listId != null);

            if (_state.Value.ODataNextUrl == null)
            {
                throw new InvalidOperationException("No next url to fetch");
            }

            var iterator = sharepointGraphListClient.GetDeltaFromUrl(listId, _state.Value.ODataNextUrl);

            Logger.BeforeCheckpointInDelta(StreamName, Name);
            await output.EnterCheckpointLock();
            try
            {
                Logger.FetchingDelta(StreamName, Name);
                if (await HandleDataRows(iterator, output))
                {
                    _state.Value.WatermarkVersion++;
                    await output.SendWatermark(new Base.Watermark(readRelation.NamedTable.DotSeperated, _state.Value.WatermarkVersion));

                    ScheduleCheckpoint(TimeSpan.FromMilliseconds(1));
                }
            }
            catch(Exception e)
            {
                if (e is TaskCanceledException || e is OperationCanceledException)
                {
                    Logger.LogWarning(e, "Error fetching delta, task was cancelled. Waiting 120 seconds before retrying.");
                    await Task.Delay(TimeSpan.FromSeconds(120));
                }
                else
                {
                    Logger.LogError(e, "Error fetching delta");
                    throw;
                }
            }
            finally
            {
                output.ExitCheckpointLock();
            }
        }

        protected override Task<IReadOnlySet<string>> GetWatermarkNames()
        {
            return Task.FromResult<IReadOnlySet<string>>(new HashSet<string>() { readRelation.NamedTable.DotSeperated });
        }

        protected override async Task InitializeOrRestore(long restoreTime, IStateManagerClient stateManagerClient)
        {
            sharepointGraphListClient = new SharepointGraphListClient(sharepointSourceOptions, StreamName, Name, Logger);
            await sharepointGraphListClient.Initialize();

            _state = await stateManagerClient.GetOrCreateObjectStateAsync<SharepointSourceState>("sharepoint_state");
            if (_state.Value == null)
            {
                _state.Value = new SharepointSourceState();
            }

            _decoders = await sharepointGraphListClient.GetColumnDecoders(listId, readRelation.BaseSchema.Names, stateManagerClient);
        }

        protected override async Task OnCheckpoint(long checkpointTime)
        {
            Debug.Assert(_state != null);
            await _state.Commit();
        }

        private async Task DecodersNewBatch()
        {
            Debug.Assert(_decoders != null);

            foreach(var decoder in _decoders)
            {
                await decoder.Value.OnNewBatch();
            }
        }

        private async Task<bool> HandleDataRows(IAsyncEnumerable<Microsoft.Graph.Sites.Item.Lists.Item.Items.Delta.DeltaGetResponse> iterator, IngressOutput<StreamEventBatch> output)
        {
            Debug.Assert(_decoders != null);
            Debug.Assert(_state?.Value != null);
            Debug.Assert(iterator != null);
            Debug.Assert(output != null);

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
                List<RowEvent> outputData = new List<RowEvent>();
                foreach (var item in page.Value)
                {
                    if (!calledDecodersNewBatch)
                    {
                        // We only call this if data exists
                        await DecodersNewBatch();
                        calledDecodersNewBatch = true;
                    }
                    FlxValue[] data = new FlxValue[readRelation.BaseSchema.Names.Count];
                    for (int i = 0; i < readRelation.BaseSchema.Names.Count; i++)
                    {
                        var name = readRelation.BaseSchema.Names[i];
                        if (_decoders.TryGetValue(name, out var decoder))
                        {
                            data[i] = await decoder.Decode(item);
                        }
                    }

                    int weight = 1;
                    if (item.ETag == null)
                    {
                        weight = -1;
                    }

                    outputData.Add(new RowEvent(weight, 0, new ArrayRowData(data)));
                }
                _state.Value.ODataNextUrl = page.OdataDeltaLink;
                await output.SendAsync(new StreamEventBatch(outputData, readRelation.OutputLength));
            }
            return calledDecodersNewBatch;
        }

        protected override async Task SendInitial(IngressOutput<StreamEventBatch> output)
        {
            Debug.Assert(sharepointGraphListClient != null);
            Debug.Assert(listId != null);
            Debug.Assert(_decoders != null);
            Debug.Assert(_state?.Value != null);

            if (!_state.Value.FetchedInitial)
            {
                var iterator = sharepointGraphListClient.GetDeltaFromList(listId);

                await HandleDataRows(iterator, output);
                _state.Value.WatermarkVersion++;
                await output.SendWatermark(new Base.Watermark(readRelation.NamedTable.DotSeperated, _state.Value.WatermarkVersion));
                _state.Value.FetchedInitial = true;
            }
            
            ScheduleCheckpoint(TimeSpan.FromMilliseconds(1));
            await RegisterTrigger("fetch_delta", TimeSpan.FromSeconds(1));
        }
    }
}
