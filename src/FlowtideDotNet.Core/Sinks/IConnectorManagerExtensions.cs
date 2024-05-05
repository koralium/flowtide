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

using FlowtideDotNet.Core.Connectors;
using FlowtideDotNet.Core.Sinks.Blackhole;
using FlowtideDotNet.Core.Sinks;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FlowtideDotNet.Core.Sinks
{
    public static class IConnectorManagerExtensions
    {
        public static IConnectorManager AddConsoleSink(this IConnectorManager manager, string regexPattern)
        {
            manager.AddSink(new ConsoleSinkFactory(regexPattern));
            return manager;
        }

        public static IConnectorManager AddBlackholeSink(this IConnectorManager manager, string regexPattern)
        {
            manager.AddSink(new BlackholeSinkFactory(regexPattern));
            return manager;
        }
    }
}
