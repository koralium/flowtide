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
using FlowtideDotNet.Core.ColumnStore;
using FlowtideDotNet.Core.ColumnStore.Hash;
using FlowtideDotNet.Core.Compute;
using FlowtideDotNet.Core.Compute.Columnar;
using FlowtideDotNet.Storage.Memory;
using FlowtideDotNet.Storage.StateManager;
using FlowtideDotNet.Substrait.Relations;
using System.Collections.Concurrent;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;

namespace FlowtideDotNet.Core.Operators.Exchange
{
    internal class ScatterExecutor : IExchangeKindExecutor
    {
        private readonly BatchHasher? _batchHasher;
        private readonly int _partitionCount;
        private readonly int[][] _partitionsToTargets;
        private readonly bool _singleTargetPerPartition;
        private readonly bool _partitionsPowerOfTwo;
        private readonly IExchangeTarget[] _targets;
        private readonly IExchangeTarget[]? _partitionToSingleTarget;
        private readonly ConcurrentDictionary<int, PullBucketTarget> _exchangeTargetIdToPullBucket;

        /// <summary>
        /// List that contains all standard outputs to make it simple to iterate when giving out the results.
        /// </summary>
        private readonly List<StandardOutputTarget> standardOutputTargetList;
        private readonly SubstreamCommunicationPointFactory _communicationPointFactory;

        public ScatterExecutor(
            ExchangeRelation exchangeRelation,
            SubstreamCommunicationPointFactory communicationPointFactory,
            FunctionsRegister functionsRegister,
            Action<int> targetCallDependenciesDone)
        {
            if (!(exchangeRelation.ExchangeKind is ScatterExchangeKind scatterExchangeKind))
            {
                throw new InvalidOperationException("ExchangeKind is not ScatterExchangeKind");
            }

            if (exchangeRelation.PartitionCount != null)
            {
                _partitionCount = exchangeRelation.PartitionCount.Value;
            }
            else
            {
                _partitionCount = exchangeRelation.Targets.Count;
            }

            standardOutputTargetList = new List<StandardOutputTarget>();
            _exchangeTargetIdToPullBucket = new ConcurrentDictionary<int, PullBucketTarget>();
            _targets = new IExchangeTarget[exchangeRelation.Targets.Count];
            for (int i = 0; i < exchangeRelation.Targets.Count; i++)
            {
                switch (exchangeRelation.Targets[i].Type)
                {
                    case ExchangeTargetType.StandardOutput:
                        var target = new StandardOutputTarget(exchangeRelation.OutputLength);
                        _targets[i] = target;
                        standardOutputTargetList.Add(target);
                        break;
                    case ExchangeTargetType.PullBucket:
                        if (exchangeRelation.Targets[i] is PullBucketExchangeTarget bucketExchangeTarget)
                        {
                            var pullTarget = new PullBucketTarget(exchangeRelation.OutputLength);
                            _targets[i] = pullTarget;
                            _exchangeTargetIdToPullBucket.AddOrUpdate(bucketExchangeTarget.ExchangeTargetId, pullTarget, (key, old) => pullTarget);
                        }
                        else
                        {
                            throw new NotSupportedException("Pull bucket type must implement PullBucketExchangeTarget");
                        }
                        break;
                    case ExchangeTargetType.Substream:
                        if (exchangeRelation.Targets[i] is SubstreamExchangeTarget substreamExchangeTarget)
                        {
                            // The acknowledgement callback carries the target id so the operator
                            // can attribute each checkpoint done to its peer; an acknowledgement
                            // from one peer must not complete another peer's dependency.
                            var exchangeTargetId = substreamExchangeTarget.ExchangeTargetId;
                            var pullTarget = new SubstreamTarget(
                                exchangeTargetId,
                                exchangeRelation.OutputLength,
                                communicationPointFactory.GetCommunicationPoint(substreamExchangeTarget.SubstreamName),
                                () => targetCallDependenciesDone(exchangeTargetId));
                            _targets[i] = pullTarget;
                        }
                        else
                        {
                            throw new NotSupportedException("Substream type must implement SubstreamExchangeTarget");
                        }
                        break;
                    default:
                        throw new NotSupportedException($"{exchangeRelation.Targets[i].Type} is not yet supported");
                }
            }

            // Generate a lookup from partition id to a list of target ids
            _partitionsToTargets = CreatePartitionToTargets(exchangeRelation);
            _singleTargetPerPartition = EachPartitionHasSingleTarget(_partitionsToTargets);
            _partitionsPowerOfTwo = (_partitionsToTargets.Length & (_partitionsToTargets.Length - 1)) == 0;

            if (_singleTargetPerPartition)
            {
                // Create a simple lookup when there is a single partition per target which is the most normal case
                // This allows quicker lookup
                _partitionToSingleTarget = new IExchangeTarget[_partitionsToTargets.Length];
                for (int i = 0; i < _partitionsToTargets.Length; i++)
                {
                    _partitionToSingleTarget[i] = _targets[_partitionsToTargets[i][0]];
                }
            }

            // Create the batch hasher based on the fields. With a single partition every
            // row lands in partition zero, no hash is evaluated: a gather
            // exchange carries no hash fields (a global aggregate can prune every column
            // from the input, a compiled field reference would then read a missing column).
            if (_partitionCount > 1)
            {
                _batchHasher = new BatchHasher(scatterExchangeKind.Fields);
            }
            this._communicationPointFactory = communicationPointFactory;
        }

        private bool EachPartitionHasSingleTarget(int[][] partitionsAndTargets)
            => partitionsAndTargets.All(x => x.Length == 1);

        private int[][] CreatePartitionToTargets(ExchangeRelation exchangeRelation)
        {
            var targets = new List<int>[_partitionCount];

            // Initialize the target lists
            for (int j = 0; j < _partitionCount; j++)
            {
                targets[j] = new List<int>();
            }

            for (int i = 0; i < exchangeRelation.Targets.Count; i++)
            {
                var target = exchangeRelation.Targets[i];
                if (target.PartitionIds.Count == 0)
                {
                    // Add to all partitions if there are no specific partition IDs
                    for (int j = 0; j < _partitionCount; j++)
                    {
                        targets[j].Add(i);
                    }
                }
                else
                {
                    // Add to specified partitions
                    foreach (var partitionId in target.PartitionIds)
                    {
                        targets[partitionId].Add(i);
                    }
                }
            }

            return targets.Select(x => x.ToArray()).ToArray();
        }

        public async Task Initialize(
            long restoreVersion,
            ExchangeRelation exchangeRelation,
            IStateManagerClient stateManagerClient,
            ExchangeOperatorState exchangeOperatorState,
            IMemoryAllocator memoryAllocator,
            Func<long, Task> failAndRecoverFunc,
            TimeSpan stopDrainTimeout)
        {
            for (int i = 0; i < _targets.Length; i++)
            {
                await _targets[i].Initialize(restoreVersion, i, stateManagerClient, exchangeOperatorState, memoryAllocator, failAndRecoverFunc, stopDrainTimeout);
            }
        }

        public async Task OnLockingEvent(ILockingEvent lockingEvent)
        {
            for(int i = 0; i < _targets.Length; i++)
            {
                await _targets[i].OnLockingEvent(lockingEvent);
            }
        }

        public async Task OnLockingEventPrepare(LockingEventPrepare lockingEventPrepare)
        {
            for (int i = 0; i < _targets.Length; i++)
            {
                await _targets[i].OnLockingEventPrepare(lockingEventPrepare);
            }
        }

        public async Task OnWatermark(Watermark watermark)
        {
            for (int i = 0; i < _targets.Length; i++)
            {
                await _targets[i].OnWatermark(watermark);
            }
        }

        public async Task OnInitialDataDone()
        {
            for (int i = 0; i < _targets.Length; i++)
            {
                await _targets[i].OnInitialDataDone();
            }
        }

        private void PartitionDataInternal(StreamEventBatch data)
        {
            Debug.Assert(_batchHasher != null || _partitionCount == 1);
            foreach (var target in _targets)
            {
                target.NewBatch(data.Data);
            }
            int count = data.Data.Count;
            if (count == 0)
            {
                return;
            }

            if (_partitionCount == 1)
            {
                var targetIndices = _partitionsToTargets[0];
                for (int t = 0; t < targetIndices.Length; t++)
                {
                    var target = _targets[targetIndices[t]];
                    for (int i = 0; i < count; i++)
                    {
                        target.AddEvent(data.Data, i);
                    }
                }
                return;
            }

            ReadOnlySpan<uint> result = ReadOnlySpan<uint>.Empty;
            if (_batchHasher != null)
            {
                result = _batchHasher.HashBatch(data.Data.EventBatchData);
            }

            if (_singleTargetPerPartition &&
                _partitionsPowerOfTwo)
            {
                Debug.Assert(_partitionToSingleTarget != null);
                ref uint resultRef = ref MemoryMarshal.GetReference(result);
                ref var targetRef = ref MemoryMarshal.GetReference(_partitionToSingleTarget.AsSpan());
                uint mask = ((uint)_partitionsToTargets.Length) - 1;
                for (int i = 0; i < count; i++)
                {
                    uint hash = Unsafe.Add(ref resultRef, i);
                    int partitionId = (int)(hash & mask);
                    Unsafe.Add(ref targetRef, partitionId).AddEvent(data.Data, i);
                }
            }
            else
            {
                for (int i = 0; i < count; i++)
                {
                    int partitionId = 0;
                    if (result.Length > 0)
                    {
                        partitionId = (int)(result[i] % _partitionCount);
                    }
                    foreach (var target in _partitionsToTargets[partitionId])
                    {
                        _targets[target].AddEvent(data.Data, i);
                    }
                }
            }   
        }

        public async IAsyncEnumerable<KeyValuePair<int, StreamMessage<StreamEventBatch>>> PartitionData(StreamEventBatch data, long time)
        {
            PartitionDataInternal(data);
            foreach (var target in _targets)
            {
                await target.BatchComplete(time);
            }

            for (int i = 0; i < standardOutputTargetList.Count; i++)
            {
                var weightedBatch = standardOutputTargetList[i].GetEvents();

                if (weightedBatch != null)
                {
                    yield return new KeyValuePair<int, StreamMessage<StreamEventBatch>>(
                        i,
                        new StreamMessage<StreamEventBatch>(new StreamEventBatch(weightedBatch), time));
                }
            }
        }

        public async Task AddCheckpointState(ExchangeOperatorState exchangeOperatorState)
        {
            for (int i = 0; i < _targets.Length; i++)
            {
                await _targets[i].AddCheckpointState(exchangeOperatorState);
            }
        }

        public Task GetPullBucketData(int exchangeTargetId, ExchangeFetchDataMessage fetchDataRequest)
        {
            if (_exchangeTargetIdToPullBucket.TryGetValue(exchangeTargetId, out var bucket))
            {
                return bucket.FetchData(fetchDataRequest);
            }
            else
            {
                throw new InvalidOperationException($"{exchangeTargetId} does not exist");
            }
        }

        public Task OnFailure(long recoveryPoint)
        {
            List<Task> tasks = new List<Task>();
            foreach (var target in _targets)
            {
                tasks.Add(target.OnFailure(recoveryPoint));
            }
            return Task.WhenAll(tasks);
        }

        public Task CheckpointDone(long checkpointVersion)
        {
            List<Task> tasks = new List<Task>();
            foreach (var target in _targets)
            {
                tasks.Add(target.CheckpointDone(checkpointVersion));
            }
            return Task.WhenAll(tasks);
        }

        public bool ReadyToStop
        {
            get
            {
                foreach (var target in _targets)
                {
                    if (!target.ReadyToStop)
                    {
                        return false;
                    }
                }
                return true;
            }
        }
    }
}
