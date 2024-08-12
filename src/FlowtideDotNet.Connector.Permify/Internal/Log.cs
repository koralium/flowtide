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
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FlowtideDotNet.Connector.Permify.Internal
{
    internal static partial class Log
    {
        [LoggerMessage(
           EventId = 1,
           Level = LogLevel.Information,
           Message = "Recieved no_error, graceful shutdown, retrying, for `{stream}`, operator: `{operatorId}`")]
        public static partial void RecievedGrpcNoErrorRetry(this ILogger logger, string stream, string operatorId);

        [LoggerMessage(
           EventId = 2,
           Level = LogLevel.Error,
           Message = "Error in Permify source, retrying, retry count: {retryCount}, for `{stream}`, operator: `{operatorId}`")]
        public static partial void ErrorInPermifyWithRetry(this ILogger logger, Exception e, int retryCount, string stream, string operatorId);

        [LoggerMessage(
           EventId = 3,
           Level = LogLevel.Error,
           Message = "Error in Permify source, retrying, for `{stream}`, operator: `{operatorId}`")]
        public static partial void ErrorInPermify(this ILogger logger, Exception e, string stream, string operatorId);

        [LoggerMessage(
           EventId = 4,
           Level = LogLevel.Warning,
           Message = "Failed to write relationships, retrying in {delay}, for `{stream}`, operator: `{operatorId}`")]
        public static partial void FailedToWriteRelationships(this ILogger logger, Exception e, TimeSpan delay, string stream, string operatorId);

        [LoggerMessage(
           EventId = 5,
           Level = LogLevel.Error,
           Message = "Permify watch stream cancelled with no data, check if watch API is turned on, for `{stream}`, operator: `{operatorId}`")]
        public static partial void WatchApiNotWorking(this ILogger logger, string stream, string operatorId);
    }
}
