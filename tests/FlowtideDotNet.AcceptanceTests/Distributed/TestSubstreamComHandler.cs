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
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FlowtideDotNet.AcceptanceTests.Distributed
{
    internal class TestSubstreamComHandler : ISubstreamCommunicationHandler
    {
        private readonly Func<long, Task> _sendCheckpointDone;
        private readonly Func<long, Task> _sendFailAndRecover;
        private readonly Func<long, Task<SubstreamInitializeResponse>> _sendInitializeRequest;
        private Func<IReadOnlySet<int>, int, CancellationToken, Task<IReadOnlyList<SubstreamEventData>>>? _getDataFunc;
        private Func<long, Task>? _callFailAndRecover;
        private Func<long, Task<SubstreamInitializeResponse>>? _initializeFromTarget;
        private Func<long, Task>? _callRecieveCheckpointDone;

        public TestSubstreamComHandler(
            Func<long, Task> sendCheckpointDone,
            Func<long, Task> sendFailAndRecover,
            Func<long, Task<SubstreamInitializeResponse>> sendInitializeRequest)
        {
            _sendCheckpointDone = sendCheckpointDone;
            _sendFailAndRecover = sendFailAndRecover;
            _sendInitializeRequest = sendInitializeRequest;
        }

        public Task<IReadOnlyList<SubstreamEventData>> GetData(IReadOnlySet<int> targetIds, int numberOfEvents, CancellationToken cancellationToken)
        {
            if (_getDataFunc == null)
            {
                throw new InvalidOperationException("Not initialized");
            }
            return _getDataFunc(targetIds, numberOfEvents, cancellationToken);
        }



        public Task<IReadOnlyList<SubstreamEventData>> FetchData(IReadOnlySet<int> targetIds, int numberOfEvents, CancellationToken cancellationToken)
        {
            throw new NotImplementedException();
        }

        public void Initialize(Func<IReadOnlySet<int>, int, CancellationToken, Task<IReadOnlyList<SubstreamEventData>>> getDataFunction, Func<long, Task> callFailAndRecover, Func<long, Task<SubstreamInitializeResponse>> initializeFromTarget, Func<long, Task> callRecieveCheckpointDone)
        {
            _getDataFunc = getDataFunction;
            _callFailAndRecover = callFailAndRecover;
            _initializeFromTarget = initializeFromTarget;
            _callRecieveCheckpointDone = callRecieveCheckpointDone;
        }

        public Task SendCheckpointDone(long checkpointVersion)
        {
            return _sendCheckpointDone(checkpointVersion);
        }

        public Task SendFailAndRecover(long restoreVersion)
        {
            return _sendFailAndRecover(restoreVersion);
        }

        public Task<SubstreamInitializeResponse> SendInitializeRequest(long restoreVersion, CancellationToken cancellationToken)
        {
            return _sendInitializeRequest(restoreVersion);
        }
    }
}
