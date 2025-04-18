﻿// Licensed under the Apache License, Version 2.0 (the "License")
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

using FlowtideDotNet.Connector.Sharepoint;
using FlowtideDotNet.Connector.Sharepoint.Internal;
using FlowtideDotNet.Core.Operators.Write;

namespace FlowtideDotNet.Core
{
    public static class SharepointConnectorManagerExtensions
    {
        public static IConnectorManager AddSharepointSink(this IConnectorManager connectorManager, string regexPattern, SharepointSinkOptions sharepointSinkOptions, ExecutionMode executionMode = ExecutionMode.Hybrid)
        {
            connectorManager.AddSink(new SharepointSinkFactory(regexPattern, sharepointSinkOptions, executionMode));
            return connectorManager;
        }

        public static IConnectorManager AddSharepointSource(this IConnectorManager connectorManager, SharepointSourceOptions sharepointSourceOptions, string prefix = "")
        {
            connectorManager.AddSource(new SharepointSourceFactory(sharepointSourceOptions, prefix));
            return connectorManager;
        }
    }
}
