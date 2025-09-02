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

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace FlowtideDotNet.Core.Operators.Exchange
{
    public interface ISubstreamCommunicationHandler
    {
        void Initialize(
            Func<IReadOnlySet<int>, int, CancellationToken, Task<IReadOnlyList<SubstreamEventData>>> getDataFunction,
            Func<long, Task> callFailAndRecover,
            Func<long, Task<SubstreamInitializeResponse>> initializeFromTarget,
            Func<long, Task> callRecieveCheckpointDone);

        Task<IReadOnlyList<SubstreamEventData>> FetchData(
            IReadOnlySet<int> targetIds,
            int numberOfEvents,
            CancellationToken cancellationToken);

        Task SendFailAndRecover(long restoreVersion);

        Task<SubstreamInitializeResponse> SendInitializeRequest(long restoreVersion, CancellationToken cancellationToken);

        Task SendCheckpointDone(long checkpointVersion);
    }
}
