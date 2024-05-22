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
        private readonly FunctionsRegister functionsRegister;
        private readonly ScatterExchangeKind _scatterExchangeKind;
        private readonly Func<RowEvent, uint> _hashFunction;
        private readonly int _partitionCount;
        private readonly int[][] _standardOutputTargets;
        private readonly int _standardOutputCount;
        private readonly List<RowEvent>?[] standardOutputArr;


        public ScatterExecutor(ExchangeRelation exchangeRelation, FunctionsRegister functionsRegister)
        {
            if (exchangeRelation.ExchangeKind is ScatterExchangeKind scatterExchangeKind)
            {
                _scatterExchangeKind = scatterExchangeKind;
            }
            else
            {
                throw new InvalidOperationException("ExchangeKind is not ScatterExchangeKind"); ;
            }

            if (exchangeRelation.PartitionCount != null)
            {
                _partitionCount = exchangeRelation.PartitionCount.Value;
            }
            else
            {
                _partitionCount = exchangeRelation.Targets.Count;
            }
            standardOutputArr = new List<RowEvent>[_standardOutputCount];
            _standardOutputCount = exchangeRelation.Targets.Count(x => x.Type == ExchangeTargetType.StandardOutput);
            _standardOutputTargets = CreateStandardOutputTargets(exchangeRelation);
            //_isStandardOutput = exchangeRelation.Targets.Select(x => x.Type == ExchangeTargetType.StandardOutput).ToArray();

            // Create the hash function based on the fields
            _hashFunction = HashCompiler.CompileGetHashCode(new List<Substrait.Expressions.Expression>(scatterExchangeKind.Fields), functionsRegister);

            this.functionsRegister = functionsRegister;
        }

        /// <summary>
        /// Locate standard output targets in order to be able to send data to them.
        /// </summary>
        /// <param name="exchangeRelation"></param>
        /// <returns></returns>
        private int[][] CreateStandardOutputTargets(ExchangeRelation exchangeRelation)
        {
            var standardOutputTargets = new List<int>[_partitionCount];

            int standardOutputCounter = 0;
            for (int i = 0; i < exchangeRelation.Targets.Count; i++)
            {
                if (exchangeRelation.Targets[i].Type == ExchangeTargetType.StandardOutput)
                {
                    for (int j = 0; j < exchangeRelation.Targets[i].PartitionIds.Count; j++)
                    {
                        var partitionId = exchangeRelation.Targets[i].PartitionIds[j];
                        if (standardOutputTargets[partitionId] == null)
                        {
                            standardOutputTargets[partitionId] = new List<int>();
                        }
                        standardOutputTargets[partitionId].Add(standardOutputCounter);
                    }
                    standardOutputCounter++;
                }
            }

            return standardOutputTargets.Select(x => x.ToArray()).ToArray();
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

                foreach(var target in _standardOutputTargets[partitionId])
                {
                    if (standardOutputArr[target] == null)
                    {
                        standardOutputArr[target] = new List<RowEvent>();
                    }
                    standardOutputArr[target]!.Add(e);
                }
            }

            // Output for standard
            for (int i = 0; i < standardOutputArr.Length; i++)
            {
                if (standardOutputArr[i] != null)
                {
                    yield return new KeyValuePair<int, StreamMessage<StreamEventBatch>>(i,
                        new StreamMessage<StreamEventBatch>(new StreamEventBatch(standardOutputArr[i]!), time));
                }
            }
        }
    }
}
