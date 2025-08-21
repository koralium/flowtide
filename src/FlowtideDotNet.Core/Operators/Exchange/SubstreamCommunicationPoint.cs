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
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading.Tasks;

namespace FlowtideDotNet.Core.Operators.Exchange
{
    public struct SubstreamEventData
    {
        public int ExchangeTargetId;
        public IStreamEvent StreamEvent;
    }

    internal class SubstreamCommunicationPoint
    {
        private readonly string substreamName;
        private readonly ISubstreamCommunicationHandler _substreamCommunicationHandler;
        private ConcurrentDictionary<int, TargetInfo> _targetInfos;

        private class TargetInfo
        {
            public readonly object Lock = new object();
            public SubstreamTarget Target { get; }

            public bool HasData { get; set; }

            public TargetInfo(SubstreamTarget target)
            {
                Target = target;
            }
        }

        public SubstreamCommunicationPoint(string substreamName, ISubstreamCommunicationHandler substreamCommunicationHandler)
        {
            _targetInfos = new ConcurrentDictionary<int, TargetInfo>();
            this.substreamName = substreamName;
            this._substreamCommunicationHandler = substreamCommunicationHandler;
            substreamCommunicationHandler.Initialize(GetData);
        }

        public void RegisterSubstreamTarget(int exchangeTargetId, SubstreamTarget target)
        {
            _targetInfos.AddOrUpdate(exchangeTargetId, new TargetInfo(target), (key, existing) => existing);
        }

        public ValueTask TargetHasData(int exchangeTargetId)
        {
            if (_targetInfos.TryGetValue(exchangeTargetId, out var targetInfo))
            {
                lock (targetInfo.Lock)
                {
                    targetInfo.HasData = true;
                }
            }
            return ValueTask.CompletedTask;
        }

        /// <summary>
        /// Fetches events from multiple exchange .
        /// The max event count is distributed as equally as possible across the different targets
        /// to fetch data from all of them if possible.
        /// </summary>
        /// <param name="exchangeTargets"></param>
        /// <param name="maxEventCount"></param>
        /// <param name="cancellationToken"></param>
        /// <returns></returns>
        public async Task<IReadOnlyList<SubstreamEventData>> GetData(IReadOnlySet<int> exchangeTargets, int maxEventCount, CancellationToken cancellationToken)
        {
            List<SubstreamEventData> outputList = new List<SubstreamEventData>();

            bool hasMoreData = true;
            int maxCountPerTarget = Math.Max(1, maxEventCount / exchangeTargets.Count);
            while (!cancellationToken.IsCancellationRequested && hasMoreData && outputList.Count < maxEventCount)
            {
                hasMoreData = false;
                cancellationToken.ThrowIfCancellationRequested();

                foreach (var exchangeTarget in exchangeTargets)
                {
                    // Fetch target info
                    if (_targetInfos.TryGetValue(exchangeTarget, out var targetInfo))
                    {
                        // Fetch 10 events from a target before switching to another target to allow fairness
                        hasMoreData |= await targetInfo.Target.ReadData(outputList, maxCountPerTarget);
                    }
                }
            }

            return outputList;
        }

        public ValueTask SendFailAndRecover(long recoveryPoint)
        {
            return _substreamCommunicationHandler.SendFailAndRecover(recoveryPoint);
        }

    }
}
