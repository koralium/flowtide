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

using FlowtideDotNet.Core;
using FlowtideDotNet.Core.Sources.Configuration.Internal;
using Microsoft.Extensions.Options;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Microsoft.Extensions.DependencyInjection
{
    public static class ConfigurationConnectorManagerExtensions
    {
        public static IConnectorManager AddOptionsSource<TOptions>(this IConnectorManager connectorManager, string tableName, IOptionsMonitor<TOptions> optionsMonitor)
        {
            connectorManager.AddSource(new OptionsMonitorSourceFactory<TOptions>(tableName, optionsMonitor));
            return connectorManager;
        }
    }
}
