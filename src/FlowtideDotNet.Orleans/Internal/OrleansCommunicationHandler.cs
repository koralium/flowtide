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

using FlowtideDotNet.Core.Operators.Exchange;
using FlowtideDotNet.Orleans.Interfaces;
using FlowtideDotNet.Storage.Memory;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FlowtideDotNet.Orleans.Internal
{
    internal class OrleansCommunicationHandler : ISubstreamCommunicationHandler
    {
        private readonly string streamName;
        private readonly string _substreamName;
        private readonly string selfName;
        private readonly IGrainFactory _grainFactory;
        private Func<IReadOnlySet<int>, int, CancellationToken, Task<IReadOnlyList<SubstreamEventData>>>? _getDataFunction;
        private ISubStreamGrain _streamGrain;
        private Func<long, Task>? _callFailAndRecover;
        private Func<long, long, bool, Task<SubstreamInitializeResponse>>? _targetInitializeRequest;
        private Func<long, long, bool, Task>? _callRecieveCheckpointDone;
        private Func<int, IMemoryAllocator>? _receiveAllocatorResolver;
        private readonly SubstreamEventWireSerializer _wireSerializer = new SubstreamEventWireSerializer();
        // Every handler instance gets a unique epoch, the seed starts at the clock so
        // processes never collide and increments per instance and failure. An abandoned
        // stream instance can then always be told apart from the current one.
        private static long _epochSeed = DateTime.UtcNow.Ticks;
        private long _fetchEpoch = Interlocked.Increment(ref _epochSeed);
        // Tick timestamp of the first consecutive fetch refused as unknown, 0 when fetches
        // are being served. Only touched from the single fetch loop.
        private long _requestorUnknownSince;

        public OrleansCommunicationHandler(string streamName, string substreamName, string selfName, IGrainFactory grainFactory)
        {
            this.streamName = streamName;
            this._substreamName = substreamName;
            this.selfName = selfName;
            this._grainFactory = grainFactory;
            _streamGrain = _grainFactory.GetGrain<ISubStreamGrain>(SubStreamGrainKey.Create(streamName, substreamName));
        }

        public void SetReceiveAllocatorResolver(Func<int, IMemoryAllocator> allocatorResolver)
        {
            _receiveAllocatorResolver = allocatorResolver;
        }

        public void OnStreamFailure()
        {
            // A fetch in flight when this stream failed would consume events the restarted stream
            // needs. Bumping the epoch makes the other substream refuse such fetches; the new epoch
            // (from the shared seed, so it never collides) is announced at the restart handshake.
            Interlocked.Exchange(ref _fetchEpoch, Interlocked.Increment(ref _epochSeed));
        }

        public async Task<IReadOnlyList<SubstreamEventData>> FetchData(IReadOnlySet<int> targetIds, int numberOfEvents, CancellationToken cancellationToken)
        {
            var response = await _streamGrain.FetchDataAsync(new Messages.FetchDataRequest(selfName, targetIds, numberOfEvents, Interlocked.Read(ref _fetchEpoch)));
            if (response.RequestorUnknown)
            {
                // This streams announcement is not current at the serving grain (it lost its epoch
                // table after a reactivation, or an abandoned instance overwrote the announcement).
                // Refused fetches look like empty polls, so no stall watchdog fires and the stream
                // would starve silently; after a grace period the fetch fails and recovery re-announces
                // the epoch. The grace period avoids forcing recoveries for a briefly restarting fetcher.
                if (_requestorUnknownSince == 0)
                {
                    _requestorUnknownSince = Environment.TickCount64;
                }
                else if (TimeSpan.FromMilliseconds(Environment.TickCount64 - _requestorUnknownSince) > SubstreamCommunicationPoint.StallLimit)
                {
                    _requestorUnknownSince = 0;
                    throw new TimeoutException($"Fetches from substream {selfName} have been refused as unknown by {_substreamName} for over {SubstreamCommunicationPoint.StallLimit}, failing and recovering to re-announce the fetch epoch.");
                }
                return Array.Empty<SubstreamEventData>();
            }
            _requestorUnknownSince = 0;
            // This side consumes and owns the pooled payload buffer: cross-silo it holds segments the
            // codec rented, same-silo it is the serving grain's instance. Nothing deserialized
            // references it, so it is safe to release once deserialization is done.
            var payload = response.Events;
            try
            {
                if (payload.Length == 0)
                {
                    return Array.Empty<SubstreamEventData>();
                }
                if (_receiveAllocatorResolver == null)
                {
                    throw new InvalidOperationException("Not initialized");
                }
                // The events arrive as an opaque buffer that can cross silo boundaries, the
                // deserialized events carry a receiver claim like a local fetch hands out.
                // Batch memory is allocated from the read operators that consume the
                // targets, so the received data is accounted on them.
                return _wireSerializer.Deserialize(payload.AsReadOnlySequence(), _receiveAllocatorResolver);
            }
            finally
            {
                payload.Dispose();
            }
        }

        public void Initialize(
            Func<IReadOnlySet<int>, int, CancellationToken, Task<IReadOnlyList<SubstreamEventData>>> getDataFunction,
            Func<long, Task> callFailAndRecover,
            Func<long, long, bool, Task<SubstreamInitializeResponse>> targetInitializeRequest,
            Func<long, long, bool, Task> callRecieveCheckpointDone)
        {
            _getDataFunction = getDataFunction;
            _callFailAndRecover = callFailAndRecover;
            _targetInitializeRequest = targetInitializeRequest;
            _callRecieveCheckpointDone = callRecieveCheckpointDone;
        }

        public async Task<IReadOnlyList<SubstreamEventData>> GetData(IReadOnlySet<int> targetIds, int numberOfEvents, CancellationToken cancellationToken = default)
        {
            if (_getDataFunction == null)
            {
                throw new InvalidOperationException("Not initialized");
            }
            return await _getDataFunction(targetIds, numberOfEvents, cancellationToken);
        }

        public Task SendFailAndRecover(long restoreVersion)
        {
            return _streamGrain.FailAndRecoverAsync(new Messages.FailAndRecoverRequest(selfName, restoreVersion));
        }

        public Task FailAndRecover(long restorePoint)
        {
            if (_callFailAndRecover == null)
            {
                throw new InvalidOperationException("Not initialized");
            }
            return _callFailAndRecover(restorePoint);
        }

        public async Task<SubstreamInitializeResponse> SendInitializeRequest(long restoreVersion, long checkpointEpoch, bool cleanHandoff, CancellationToken cancellationToken)
        {
            var announcedEpoch = Interlocked.Read(ref _fetchEpoch);
            var response = await _streamGrain.InitializeSubstreamRequest(new Messages.InitSubstreamRequest(selfName, restoreVersion, announcedEpoch, checkpointEpoch, cleanHandoff));
            if (response.RecordedFetchEpoch > announcedEpoch)
            {
                // The serving grain holds a higher epoch for this substream than was announced,
                // recorded by an instance that no longer exists: after a silo failover this
                // substream can run on a process whose clock-based seed started earlier than the
                // dead instances, and the +1 drawn per failure never bridges a clock-scale gap.
                // The refusal is answered as an already reconciled success, so without a
                // re-announce every fetch would be refused as unknown and the stream would fail
                // and recover forever. The shared seed is raised above the recorded epoch and the
                // handshake re-run once with a fresh draw. A genuinely stale (zombie) instance can
                // reach this path too, but only from its bounded startup retry loop; the live
                // instance re-announces on every recovery and wins terminally.
                long seed;
                do
                {
                    seed = Interlocked.Read(ref _epochSeed);
                } while (seed < response.RecordedFetchEpoch &&
                         Interlocked.CompareExchange(ref _epochSeed, response.RecordedFetchEpoch, seed) != seed);
                announcedEpoch = Interlocked.Increment(ref _epochSeed);
                Interlocked.Exchange(ref _fetchEpoch, announcedEpoch);
                response = await _streamGrain.InitializeSubstreamRequest(new Messages.InitSubstreamRequest(selfName, restoreVersion, announcedEpoch, checkpointEpoch, cleanHandoff));
            }
            return new SubstreamInitializeResponse(response.NotStarted, response.Success, response.RestoreVersion, response.CheckpointEpoch, response.RecordedCheckpointEpoch, response.CleanReconnect);
        }

        public Task<SubstreamInitializeResponse> TargetInitializeRequest(long restoreVersion, long peerCheckpointEpoch, bool cleanHandoff)
        {
            if (_targetInitializeRequest == null)
            {
                throw new InvalidOperationException("Not initialized");
            }
            return _targetInitializeRequest(restoreVersion, peerCheckpointEpoch, cleanHandoff);
        }

        public Task SendCheckpointDone(long checkpointVersion, long targetCheckpointEpoch, bool coversPeerStopBarrier)
        {
            return _streamGrain.CheckpointDone(new Messages.CheckpointDoneRequest(selfName, checkpointVersion, targetCheckpointEpoch, coversPeerStopBarrier));
        }

        public Task TargetCheckpointDone(long checkpointVersion, long checkpointEpoch, bool coversPeerStopBarrier)
        {
            if (_callRecieveCheckpointDone == null)
            {
                throw new InvalidOperationException("Not initialized");
            }
            return _callRecieveCheckpointDone(checkpointVersion, checkpointEpoch, coversPeerStopBarrier);
        }
    }
}
