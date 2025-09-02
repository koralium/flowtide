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

using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FlowtideDotNet.Core.Operators.Exchange
{
    internal class SubstreamCommunicationPointFactory
    {
        private readonly object _lock = new object();
        private readonly Dictionary<string, SubstreamCommunicationPoint> _existing;
        private readonly ILoggerFactory loggerFactory;
        private readonly string? selfSubstreamName;
        private readonly ISubstreamCommunicationHandlerFactory? _communicationHandlerFactory;

        public SubstreamCommunicationPointFactory(ILoggerFactory? loggerFactory = null, string? selfSubstreamName = null, ISubstreamCommunicationHandlerFactory? communicationHandlerFactory = null)
        {
            _existing = new Dictionary<string, SubstreamCommunicationPoint>();
            if (loggerFactory != null)
            {
                this.loggerFactory = loggerFactory;
            }
            else
            {
                this.loggerFactory = NullLoggerFactory.Instance;
            }
            this.selfSubstreamName = selfSubstreamName;
            this._communicationHandlerFactory = communicationHandlerFactory;
        }

        public SubstreamCommunicationPoint GetCommunicationPoint(string targetSubstreamName)
        {
            if (_communicationHandlerFactory == null)
            {
                throw new InvalidOperationException("No communication handler factory provided, cannot create communication point.");
            }
            if (selfSubstreamName == null)
            {
                throw new InvalidOperationException("No self substream name provided, cannot create communication point.");
            }
            lock (_lock)
            {
                if (_existing.TryGetValue(targetSubstreamName, out var existing))
                {
                    return existing;
                }
                existing = new SubstreamCommunicationPoint(loggerFactory.CreateLogger($"substream_com_{targetSubstreamName}"), targetSubstreamName, _communicationHandlerFactory.GetCommunicationHandler(targetSubstreamName, selfSubstreamName));
                _existing.Add(targetSubstreamName, existing);
                return existing;
            }
        }
    }
}
