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

namespace FlowtideDotNet.Core.Operators.Exchange
{
    internal class ScatterExecutor : IExchangeKindExecutor
    {
        private readonly Func<RowEvent, uint> _hashFunction;
        private readonly int _partitionCount;
        private readonly int[][] _partitionsToTargets;
        private readonly int _standardOutputCount;
        private readonly IExchangeTarget[] _targets;

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
            _targets = new IExchangeTarget[exchangeRelation.Targets.Count];
            for (int i = 0; i < exchangeRelation.Targets.Count; i++)
            {
                switch (exchangeRelation.Targets[i].Type)
                {
                    case ExchangeTargetType.StandardOutput:
                        var target = new StandardOutputTarget(); ;
                        _targets[i] = target;
                        standardOutputTargetList.Add(target);
                        break;
                    case ExchangeTargetType.PullBucket:
                        _targets[i] = new PullBucketTarget();
                        break;
                    default:
                        throw new NotSupportedException($"{exchangeRelation.Targets[i].Type} is not yet supported");
                }
            }

            _standardOutputCount = exchangeRelation.Targets.Count(x => x.Type == ExchangeTargetType.StandardOutput);
            _partitionsToTargets = CreatePartitionToTargets(exchangeRelation);
            //_isStandardOutput = exchangeRelation.Targets.Select(x => x.Type == ExchangeTargetType.StandardOutput).ToArray();

            // Create the hash function based on the fields
            _hashFunction = HashCompiler.CompileGetHashCode(new List<Substrait.Expressions.Expression>(scatterExchangeKind.Fields), functionsRegister);
        }

        private int[][] CreatePartitionToTargets(ExchangeRelation exchangeRelation)
        {
            var targets = new List<int>[_partitionCount];

            for (int i = 0; i < exchangeRelation.Targets.Count; i++)
            {
                if (exchangeRelation.Targets[i].PartitionIds.Count == 0)
                {
                    for (int j = 0; j < _partitionCount; j++)
                    {
                        if (targets[j] == null)
                        {
                            targets[j] = new List<int>();
                        }
                        targets[j].Add(i);
                    }
                }
                else
                {
                    for (int j = 0; j < exchangeRelation.Targets[i].PartitionIds.Count; j++)
                    {
                        var partitionId = exchangeRelation.Targets[i].PartitionIds[j];
                        if (targets[partitionId] == null)
                        {
                            targets[partitionId] = new List<int>();
                        }
                        targets[partitionId].Add(i);
                    }
                }
                
            }

            return targets.Select(x => x?.ToArray() ?? new int[0]).ToArray();
        }

        public Task Initialize(
            ExchangeRelation exchangeRelation, 
            IStateManagerClient stateManagerClient, 
            ExchangeOperatorState exchangeOperatorState)
        {
            // Create a tree for each pull bucket

            return Task.CompletedTask;
        }

        public Task OnLockingEvent(ILockingEvent lockingEvent)
        {
            return Task.CompletedTask;
        }

        public Task OnLockingEventPrepare(LockingEventPrepare lockingEventPrepare)
        {
            return Task.CompletedTask;
        }

        public Task OnWatermark(Watermark watermark)
        {
            return Task.CompletedTask;
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
    }
}
