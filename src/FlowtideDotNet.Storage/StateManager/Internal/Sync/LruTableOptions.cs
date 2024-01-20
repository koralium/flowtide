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
using System;
using System.Collections.Generic;
using System.Diagnostics.Metrics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FlowtideDotNet.Storage.StateManager.Internal.Sync
{
    internal class LruTableOptions
    {
        public LruTableOptions(string streamName, ILogger logger, Meter meter)
        {
            StreamName = streamName;
            Logger = logger;
            Meter = meter;
        }

        public int MaxSize { get; set; } = 10000;

        public long MaxMemoryUsageInBytes { get; set; } = -1;

        public int MinSize { get; set; } = 1000;

        public string StreamName { get; }
        
        public ILogger Logger { get; }
        
        public Meter Meter { get; }
    }
}
