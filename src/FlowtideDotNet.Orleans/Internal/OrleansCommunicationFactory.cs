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

namespace FlowtideDotNet.Orleans.Internal
{
    internal class OrleansCommunicationFactory : ISubstreamCommunicationHandlerFactory
    {
        private readonly string _streamName;
        private readonly IGrainFactory _grainFactory;

        public Dictionary<string, OrleansCommunicationHandler> handlers = new Dictionary<string, OrleansCommunicationHandler>();

        public OrleansCommunicationFactory(string streamName, IGrainFactory grainFactory)
        {
            _streamName = streamName;
            _grainFactory = grainFactory;
        }

        public ISubstreamCommunicationHandler GetCommunicationHandler(string targetSubstreamName, string selfSubstreamName)
        {
            var handler = new OrleansCommunicationHandler(_streamName, targetSubstreamName, selfSubstreamName, _grainFactory);
            handlers[targetSubstreamName] = handler;
            return handler;
        }
    }
}
