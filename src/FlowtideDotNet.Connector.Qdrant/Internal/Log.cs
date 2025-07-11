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

namespace FlowtideDotNet.Connector.Qdrant.Internal
{
    internal static partial class Log
    {
        [LoggerMessage(
            EventId = 1,
            Level = LogLevel.Information,
            Message = "Processing changes in stream: `{stream}`")]
        public static partial void UploadChangesStarted(this ILogger logger, string stream);

        [LoggerMessage(
            EventId = 2,
            Level = LogLevel.Information,
            Message = "Change processing done in stream: `{stream}`")]
        public static partial void UploadChangesDone(this ILogger logger, string stream);

        [LoggerMessage(
            EventId = 3,
            Level = LogLevel.Information,
            Message = "Deleting {count} points in stream `{stream}`.")]
        public static partial void DeletingPoints(this ILogger logger, int count, string stream);

        [LoggerMessage(
            EventId = 4,
            Level = LogLevel.Information,
            Message = "Executing {count} operations against Qdrant in stream `{stream}`.")]
        public static partial void HandlingPoints(this ILogger logger, int count, string stream);
    }
}
