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
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FlowtideDotNet.Orleans.Internal
{
    internal class OrleansCommunicationHandler : ISubstreamCommunicationHandler
    {
        private readonly string _substreamName;
        private readonly string selfName;
        private readonly IGrainFactory _grainFactory;
        private Func<IReadOnlySet<int>, int, CancellationToken, Task<IReadOnlyList<SubstreamEventData>>>? _getDataFunction;
        private IStreamGrain _streamGrain;

        public OrleansCommunicationHandler(string substreamName, string selfName, IGrainFactory grainFactory)
        {
            this._substreamName = substreamName;
            this.selfName = selfName;
            this._grainFactory = grainFactory;
            _streamGrain = _grainFactory.GetGrain<IStreamGrain>(_substreamName);
        }

        public async Task<IReadOnlyList<SubstreamEventData>> FetchData(IReadOnlySet<int> targetIds, int numberOfEvents, CancellationToken cancellationToken)
        {
            var response = await _streamGrain.FetchDataAsync(new Messages.FetchDataRequest(selfName, targetIds, numberOfEvents));
            return response.Data;
        }

        public void Initialize(Func<IReadOnlySet<int>, int, CancellationToken, Task<IReadOnlyList<SubstreamEventData>>> getDataFunction)
        {
            _getDataFunction = getDataFunction;
        }

        public async Task<IReadOnlyList<SubstreamEventData>> GetData(IReadOnlySet<int> targetIds, int numberOfEvents, CancellationToken cancellationToken = default)
        {
            if (_getDataFunction == null)
            {
                throw new InvalidOperationException("Not initialized");
            }
            return await _getDataFunction(targetIds, numberOfEvents, cancellationToken);
        }

        public ValueTask SendFailAndRecover(long restoreVersion)
        {

            throw new NotImplementedException();
        }
    }
}
