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

namespace FlowtideDotNet.Connector.MongoDB.Internal
{
    internal static partial class Log
    {
        [LoggerMessage(
           EventId = 1,
           Level = LogLevel.Warning,
           Message = "Failed to write to mongoDB, will retry, stream `{stream}`, operator `{operatorId}`")]
        public static partial void FailedToWriteMongoDB(this ILogger logger, Exception? e, string stream, string operatorId);

        [LoggerMessage(
           EventId = 2,
           Level = LogLevel.Warning,
           Message = "Failed to start change stream for MongoDB, using full load on interval to detect changes., stream `{stream}`, operator `{operatorId}`")]
        public static partial void ChangeStreamDisabledUsingFullLoad(this ILogger logger, Exception? e, string stream, string operatorId);
    }
}
