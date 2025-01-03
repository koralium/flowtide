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

using FlowtideDotNet.Connector.DeltaLake;
using FlowtideDotNet.Connector.DeltaLake.Internal;

namespace FlowtideDotNet.Core
{
    public static class DeltaLakeConnectorManagerExtensions
    {
        public static IConnectorManager AddDeltaLakeSink(this IConnectorManager connectorManager, string regexPattern, DeltaLakeSinkOptions options)
        {
            connectorManager.AddSink(new DeltaLakeSinkFactory(regexPattern, options));
            return connectorManager;
        }
    }
}
