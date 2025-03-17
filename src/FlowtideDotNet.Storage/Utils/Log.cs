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

namespace FlowtideDotNet.Storage.Utils
{
    internal static partial class Log
    {
        [LoggerMessage(
           EventId = 1,
           Level = LogLevel.Trace,
           Message = "LRU Table is no longer full for steam `{stream}`")]
        public static partial void LruTableNoLongerFull(this ILogger logger, string stream);

        [LoggerMessage(
           EventId = 2,
           Level = LogLevel.Trace,
           Message = "LRU Table is full, waiting for cleanup to finish for steam `{stream}`")]
        public static partial void LruTableIsFull(this ILogger logger, string stream);

        [LoggerMessage(
           EventId = 3,
           Level = LogLevel.Warning,
           Message = "Cleanup task closed without error, for steam `{stream}`")]
        public static partial void CleanupTaskClosedWithoutError(this ILogger logger, string stream);

        [LoggerMessage(
           EventId = 4,
           Level = LogLevel.Error,
           Message = "Exception in LRU Table cleanup, for steam `{stream}`")]
        public static partial void ExceptionInLruTableCleanup(this ILogger logger, Exception? e, string stream);
    }
}
