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
using FlowtideDotNet.Core.Operators.Read;
using FlowtideDotNet.Storage.StateManager;
using FlowtideDotNet.Substrait.Relations;
using Grpc.Core;
using System.Buffers.Binary;
using System.Diagnostics;
using System.Threading.Tasks.Dataflow;
using PermifyProto = Base.V1;

namespace FlowtideDotNet.Connector.Permify.Internal
{
    internal class FlowtidePermifySourceState
    {
        public string? WatchSnapToken { get; set; }

        public bool SentInitial { get; set; }
    }

    internal class PermifyRelationSource : ReadBaseOperator
    {
        private readonly PermifyRowEncoder _rowEncoder;
        private readonly HashSet<string> _watermarkNames;
        private readonly PermifyProto.Data.DataClient _dataClient;
        private readonly PermifyProto.Watch.WatchClient _watchClient;
        private readonly ReadRelation _readRelation;
        private readonly PermifySourceOptions _permifySourceOptions;
        private AsyncServerStreamingCall<PermifyProto.WatchResponse>? _watchStream;
        private IObjectState<FlowtidePermifySourceState>? _state;

        public PermifyRelationSource(ReadRelation readRelation, PermifySourceOptions permifySourceOptions, DataflowBlockOptions options) : base(options)
        {
            _rowEncoder = PermifyRowEncoder.Create(readRelation.BaseSchema.Names);
            _watermarkNames = new HashSet<string>()
            {
                readRelation.NamedTable.DotSeperated
            };
            _dataClient = new PermifyProto.Data.DataClient(permifySourceOptions.Channel);
            _readRelation = readRelation;
            _permifySourceOptions = permifySourceOptions;
            _watchClient = new PermifyProto.Watch.WatchClient(permifySourceOptions.Channel);
        }

        public override string DisplayName => "PermifySource";

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
            return Task.FromResult<IReadOnlySet<string>>(_watermarkNames);
        }

        protected override async Task InitializeOrRestore(long restoreTime, IStateManagerClient stateManagerClient)
        {
            _state = await stateManagerClient.GetOrCreateObjectStateAsync<FlowtidePermifySourceState>("permify_state");
            if (_state.Value == null)
            {
                _state.Value = new FlowtidePermifySourceState();
            }
        }

        protected override async Task OnCheckpoint(long checkpointTime)
        {
            Debug.Assert(_state != null);
            await _state.Commit();
        }

        private Metadata GetMetadata()
        {
            if (_permifySourceOptions.GetMetadata != null)
            {
                return _permifySourceOptions.GetMetadata();
            }
            return new Metadata();
        }

        private Watermark GetWatermark(string token)
        {
            // decode the token from base64 string to long
            var bytes = Convert.FromBase64String(token);
            var offset = BinaryPrimitives.ReadInt64LittleEndian(bytes);
            return new Watermark(_readRelation.NamedTable.DotSeperated, offset);
        }

        private async Task LoadChangesTask(IngressOutput<StreamEventBatch> output, object? state)
        {
            Debug.Assert(_state?.Value != null);
            Debug.Assert(_watchStream != null);

            bool initWatch = false;

            while (!output.CancellationToken.IsCancellationRequested)
            {
                output.CancellationToken.ThrowIfCancellationRequested();

                try
                {
                    // If the watch failed, init it again
                    if (initWatch)
                    {
                        Metadata metadata = GetMetadata();
                        
                        var watchRequest = new PermifyProto.WatchRequest()
                        {
                            TenantId = _permifySourceOptions.TenantId
                        };
                        if (_state.Value.WatchSnapToken != null)
                        {
                            watchRequest.SnapToken = _state.Value.WatchSnapToken;
                        }
                        _watchStream = _watchClient.Watch(watchRequest, metadata);
                        initWatch = false;
                    }
                    SetHealth(true);
                    if (await _watchStream.ResponseStream.MoveNext(output.CancellationToken))
                    {
                        await output.EnterCheckpointLock();
                        List<RowEvent> outData = new List<RowEvent>();
                        foreach(var change in _watchStream.ResponseStream.Current.Changes.DataChanges_)
                        {
                            if (change.TypeCase == PermifyProto.DataChange.TypeOneofCase.Tuple)
                            {
                                int weight = 1;
                                if (change.Operation == PermifyProto.DataChange.Types.Operation.Delete)
                                {
                                    weight = -1;
                                }
                                var rowEvent = _rowEncoder.Encode(change.Tuple, weight);
                                outData.Add(rowEvent);
                            }
                        }
                        _state.Value.WatchSnapToken = _watchStream.ResponseStream.Current.Changes.SnapToken;
                        if (outData.Count > 0)
                        {
                            await output.SendAsync(new StreamEventBatch(outData, _readRelation.OutputLength));
                            await output.SendWatermark(GetWatermark(_state.Value.WatchSnapToken));
                            ScheduleCheckpoint(TimeSpan.FromMilliseconds(1));
                        }
                        output.ExitCheckpointLock();
                    }
                    else
                    {
                        if (string.IsNullOrEmpty(_state.Value.WatchSnapToken))
                        {
                            Logger.WatchApiNotWorking(StreamName, Name);
                        }
                        initWatch = true;
                    }
                }
                catch(Exception e)
                {
                    if (e is RpcException rpcException &&
                        rpcException.Status.StatusCode == StatusCode.Unavailable)
                    {
                        Logger.RecievedGrpcNoErrorRetry(StreamName, Name);
                    }
                    else
                    {
                        SetHealth(false);
                        Logger.ErrorInPermify(e, StreamName, Name);
                    }
                    // Set that the watch should be initialized again
                    initWatch = true;
                }
            }
        }

        protected override async Task SendInitial(IngressOutput<StreamEventBatch> output)
        {
            Debug.Assert(_state?.Value != null);

            var watchRequest = new PermifyProto.WatchRequest
            {
                TenantId = _permifySourceOptions.TenantId,
            };

            if (_state.Value.WatchSnapToken != null)
            {
                watchRequest.SnapToken = _state.Value.WatchSnapToken;
            }
            // Start watch stream before reading data to catch any changes
            _watchStream = _watchClient.Watch(watchRequest, GetMetadata());

            if (!_state.Value.SentInitial)
            {
                // Enter lock so no checkpoint can be done before all data has been sent.
                await output.EnterCheckpointLock();

                var readResponse = await _dataClient.ReadRelationshipsAsync(new PermifyProto.RelationshipReadRequest()
                {
                    Filter = new PermifyProto.TupleFilter(),
                    PageSize = 100,
                    TenantId = _permifySourceOptions.TenantId,
                    Metadata = new PermifyProto.RelationshipReadRequestMetadata()
                });

                while (readResponse.Tuples.Count > 0)
                {
                    List<RowEvent> outData = new List<RowEvent>();
                    foreach (var tuple in readResponse.Tuples)
                    {
                        var rowEvent = _rowEncoder.Encode(tuple, 1);
                        outData.Add(rowEvent);
                    }
                    await output.SendAsync(new StreamEventBatch(outData, _readRelation.OutputLength));

                    if (string.IsNullOrEmpty(readResponse.ContinuousToken))
                    {
                        break;
                    }

                    // Fetch more data
                    readResponse = await _dataClient.ReadRelationshipsAsync(new PermifyProto.RelationshipReadRequest()
                    {
                        Filter = new PermifyProto.TupleFilter(),
                        PageSize = 100,
                        TenantId = _permifySourceOptions.TenantId,
                        Metadata = new PermifyProto.RelationshipReadRequestMetadata(),
                        ContinuousToken = readResponse.ContinuousToken
                    });
                }
                _state.Value.SentInitial = true;
                // Since we cant get the current snap token at this stage, just write a 1.
                await output.SendWatermark(new Base.Watermark(_readRelation.NamedTable.DotSeperated, 1));
                output.ExitCheckpointLock();
                ScheduleCheckpoint(TimeSpan.FromMilliseconds(1));
            }

            _ = RunTask(LoadChangesTask, taskCreationOptions: TaskCreationOptions.LongRunning);
        }
    }
}
