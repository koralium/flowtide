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

namespace FlowtideDotNet.Core.Utils
{
    internal static partial class Log
    {
        [LoggerMessage(
           EventId = 1,
           Level = LogLevel.Information,
           Message = "Initializing filter operator in stream: `{stream}`, operator: `{operatorId}`")]
        public static partial void InitializingFilterOperator(this ILogger logger, string stream, string operatorId);

        [LoggerMessage(
           EventId = 2,
           Level = LogLevel.Information,
           Message = "Initializing merge join operator in stream: `{stream}`, operator: `{operatorId}`")]
        public static partial void InitializingMergeJoinOperator(this ILogger logger, string stream, string operatorId);

        [LoggerMessage(
           EventId = 3,
           Level = LogLevel.Information,
           Message = "Initializing normalization operator in stream: `{stream}`, operator: `{operatorId}`")]
        public static partial void InitializingNormalizationOperator(this ILogger logger, string stream, string operatorId);

        [LoggerMessage(
           EventId = 4,
           Level = LogLevel.Information,
           Message = "Initializing project operator in stream: `{stream}`, operator: `{operatorId}`")]
        public static partial void InitializingProjectOperator(this ILogger logger, string stream, string operatorId);

        [LoggerMessage(
           EventId = 5,
           Level = LogLevel.Warning,
           Message = "Block nested loop join in use, it will severely impact performance of the stream, in stream: `{stream}`, operator: `{operatorId}`")]
        public static partial void BlockNestedLoopInUse(this ILogger logger, string stream, string operatorId);

        [LoggerMessage(
           EventId = 6,
           Level = LogLevel.Information,
           Message = "Fetching existing data from data source, in stream: `{stream}`, operator: `{operatorId}`")]
        public static partial void FetchingExistingDataInDataSource(this ILogger logger, string stream, string operatorId);

        [LoggerMessage(
           EventId = 7,
           Level = LogLevel.Information,
           Message = "Done fetching existing data, in stream: `{stream}`, operator: `{operatorId}`")]
        public static partial void DoneFetchingExistingData(this ILogger logger, string stream, string operatorId);
    }
}
