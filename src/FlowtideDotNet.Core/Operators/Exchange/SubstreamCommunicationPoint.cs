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
    internal struct SubstreamEventData
    {
        public int ExchangeTargetId;
        public IStreamEvent StreamEvent;
    }

    internal class SubstreamCommunicationPoint
    {
        private readonly string substreamName;
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

        public SubstreamCommunicationPoint(string substreamName)
        {
            _targetInfos = new ConcurrentDictionary<int, TargetInfo>();
            this.substreamName = substreamName;
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

        public async IAsyncEnumerable<SubstreamEventData> GetData(IReadOnlySet<int> exchangeTargets, CancellationToken cancellationToken, [EnumeratorCancellation] CancellationToken enumeratorCancelToken)
        {
            List<IStreamEvent> outputList = new List<IStreamEvent>();

            while (!cancellationToken.IsCancellationRequested && !enumeratorCancelToken.IsCancellationRequested)
            {
                cancellationToken.ThrowIfCancellationRequested();
                enumeratorCancelToken.ThrowIfCancellationRequested();

                foreach (var exchangeTarget in exchangeTargets)
                {
                    // Fetch target info
                    if (_targetInfos.TryGetValue(exchangeTarget, out var targetInfo))
                    {
                        // Fetch 10 events from a target before switching to another target to allow fairness
                        var data = await targetInfo.Target.ReadData(outputList, 10);

                        foreach (var e in outputList)
                        {
                            yield return new SubstreamEventData()
                            {
                                ExchangeTargetId = exchangeTarget,
                                StreamEvent = e
                            };
                        }
                        outputList.Clear();
                    }
                }
            }
        }

        public ValueTask SendFailAndRecover(long recoveryPoint)
        {
            return ValueTask.CompletedTask;
        }

    }
}
