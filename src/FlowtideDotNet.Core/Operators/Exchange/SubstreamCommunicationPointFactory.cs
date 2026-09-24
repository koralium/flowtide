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
        private readonly bool _announceCleanHandoff;
        private readonly SubstreamDurabilityCoordinator? _durability;
        private readonly SubstreamRecoveryWaves _waves = new SubstreamRecoveryWaves();
        private readonly HashSet<string>? _groupPeers;

        /// <summary>
        /// Null when this stream is not a substream, it then agrees with itself.
        /// </summary>
        internal SubstreamDurabilityCoordinator? Durability => _durability;

        /// <summary>
        /// The recovery this stream is in, shared by all its communication points.
        /// </summary>
        internal SubstreamRecoveryWaves Waves => _waves;

        public SubstreamCommunicationPointFactory(ILoggerFactory? loggerFactory = null, string? selfSubstreamName = null, ISubstreamCommunicationHandlerFactory? communicationHandlerFactory = null, bool announceCleanHandoff = false, SubstreamGroup? group = null)
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
            _announceCleanHandoff = announceCleanHandoff;
            if (group != null && selfSubstreamName != null && group.GroupSize > 1)
            {
                // A substream that exchanges data with nobody agrees with itself.
                _groupPeers = new HashSet<string>(group.Peers);
                _durability = new SubstreamDurabilityCoordinator(
                    this.loggerFactory.CreateLogger($"FlowtideDotNet.substream_durability_{selfSubstreamName}"),
                    selfSubstreamName,
                    group.Peers,
                    group.Distance);
            }
        }

        /// <summary>
        /// The stream's run ended, every point handshakes again at the next start.
        /// </summary>
        internal void OnStreamStopped()
        {
            List<SubstreamCommunicationPoint> points;
            lock (_lock)
            {
                points = new List<SubstreamCommunicationPoint>(_existing.Values);
            }
            foreach (var point in points)
            {
                point.OnStreamStopped();
            }
        }

        internal void AbortPendingOperations()
        {
            List<SubstreamCommunicationPoint> points;
            lock (_lock) points = _existing.Values.ToList();
            foreach (var point in points) point.AbortPendingOperations();
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
            if (_groupPeers != null && !_groupPeers.Contains(targetSubstreamName))
            {
                // A peer the agreement does not wait for would silently weaken the guarantee.
                throw new InvalidOperationException($"Substream '{selfSubstreamName}' exchanges data with '{targetSubstreamName}', which the plan does not show as one of its peers.");
            }
            lock (_lock)
            {
                if (_existing.TryGetValue(targetSubstreamName, out var existing))
                {
                    return existing;
                }
                existing = new SubstreamCommunicationPoint(loggerFactory.CreateLogger($"FlowtideDotNet.substream_com_{selfSubstreamName}_{targetSubstreamName}"), selfSubstreamName, targetSubstreamName, _communicationHandlerFactory.GetCommunicationHandler(targetSubstreamName, selfSubstreamName), _announceCleanHandoff, _durability, _waves);
                _existing.Add(targetSubstreamName, existing);
                return existing;
            }
        }
    }
}
