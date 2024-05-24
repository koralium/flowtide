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
using FlowtideDotNet.Core.Compute.Internal;
using FlowtideDotNet.Core.Compute;
using FlowtideDotNet.Storage.StateManager;
using FlowtideDotNet.Substrait.Relations;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Diagnostics;
using System.Collections.Concurrent;

namespace FlowtideDotNet.Core.Operators.Exchange
{
    internal class ScatterExecutor : IExchangeKindExecutor
    {
        private readonly Func<RowEvent, uint> _hashFunction;
        private readonly int _partitionCount;
        private readonly int[][] _partitionsToTargets;
        private readonly IExchangeTarget[] _targets;
        private readonly ConcurrentDictionary<int, PullBucketTarget> _exchangeTargetIdToPullBucket;

        /// <summary>
        /// List that contains all standard outputs to make it simple to iterate when giving out the results.
        /// </summary>
        private readonly List<StandardOutputTarget> standardOutputTargetList;

        public ScatterExecutor(ExchangeRelation exchangeRelation, FunctionsRegister functionsRegister)
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
                        var target = new StandardOutputTarget();
                        _targets[i] = target;
                        standardOutputTargetList.Add(target);
                        break;
                    case ExchangeTargetType.PullBucket:
                        if (exchangeRelation.Targets[i] is PullBucketExchangeTarget bucketExchangeTarget)
                        {
                            var pullTarget = new PullBucketTarget();
                            _targets[i] = pullTarget;
                            _exchangeTargetIdToPullBucket.AddOrUpdate(bucketExchangeTarget.ExchangeTargetId, pullTarget, (key, old) => pullTarget);
                        }
                        else
                        {
                            throw new NotSupportedException("Pull bucket type must implement PullBucketExchangeTarget");
                        }
                        break;
                    default:
                        throw new NotSupportedException($"{exchangeRelation.Targets[i].Type} is not yet supported");
                }
            }

            // Generate a lookup from partition id to a list of target ids
            _partitionsToTargets = CreatePartitionToTargets(exchangeRelation);

            // Create the hash function based on the fields
            _hashFunction = HashCompiler.CompileGetHashCode(new List<Substrait.Expressions.Expression>(scatterExchangeKind.Fields), functionsRegister);
        }

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
            ExchangeRelation exchangeRelation, 
            IStateManagerClient stateManagerClient, 
            ExchangeOperatorState exchangeOperatorState)
        {
            for (int i = 0; i < _targets.Length; i++)
            {
                await _targets[i].Initialize(i, stateManagerClient, exchangeOperatorState);
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

        public async IAsyncEnumerable<KeyValuePair<int, StreamMessage<StreamEventBatch>>> PartitionData(StreamEventBatch data, long time)
        {
            Debug.Assert(_hashFunction != null);
            foreach(var e in data.Events)
            {
                var hash = _hashFunction(e);
                int partitionId = (int)(hash % _partitionCount);

                foreach(var target in _partitionsToTargets[partitionId])
                {
                    await _targets[target].AddEvent(e);
                }
            }
            foreach(var target in _targets)
            {
                await target.BatchComplete(time);
            }

            for (int i = 0; i < standardOutputTargetList.Count; i++)
            {
                var eventsList = standardOutputTargetList[i].GetEvents();

                if (eventsList != null)
                {
                    yield return new KeyValuePair<int, StreamMessage<StreamEventBatch>>(
                        i,
                        new StreamMessage<StreamEventBatch>(new StreamEventBatch(eventsList), time));
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
    }
}
