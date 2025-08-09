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

namespace FlowtideDotNet.Connector.Sharepoint.Internal
{
    internal static partial class Log
    {
        [LoggerMessage(
           EventId = 1,
           Level = LogLevel.Warning,
           Message = "Person or group not found: `{error}`, stream `{stream}`, operator `{operatorId}`")]
        public static partial void PersonOrGroupNotFound(this ILogger logger, string error, string stream, string operatorId);

        [LoggerMessage(
           EventId = 2,
           Level = LogLevel.Trace,
           Message = "Before checkpoint in delta, stream `{stream}`, operator `{operatorId}`")]
        public static partial void BeforeCheckpointInDelta(this ILogger logger, string stream, string operatorId);

        [LoggerMessage(
           EventId = 3,
           Level = LogLevel.Trace,
           Message = "Fetching delta, stream `{stream}`, operator `{operatorId}`")]
        public static partial void FetchingDelta(this ILogger logger, string stream, string operatorId);
    }
}
