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

namespace FlowtideDotNet.Connector.SqlServer.SqlServer
{
    internal static partial class Log
    {
        [LoggerMessage(
           EventId = 1,
           Level = LogLevel.Information,
           Message = "{changeCount} Changes found from table, {tableName} in stream: `{stream}`, operator: `{operatorId}`")]
        public static partial void ChangesFoundInTable(this ILogger logger, int changeCount, string tableName, string stream, string operatorId);

        [LoggerMessage(
           EventId = 2,
           Level = LogLevel.Warning,
           Message = "Exception fetching changes, will try again in 5 seconds in stream: `{stream}`, operator: `{operatorId}`")]
        public static partial void ExceptionFetchingChanges(this ILogger logger, Exception e, string stream, string operatorId);

        [LoggerMessage(
          EventId = 3,
          Level = LogLevel.Information,
          Message = "Initializing Sql Server Source for table {tableName} in stream: `{stream}`, operator: `{operatorId}`")]
        public static partial void InitializingSqlServerSource(this ILogger logger, string tableName, string stream, string operatorId);

        [LoggerMessage(
          EventId = 4,
          Level = LogLevel.Information,
          Message = "Selecting all data from {tableName} in stream: `{stream}`, operator: `{operatorId}`")]
        public static partial void SelectingAllData(this ILogger logger, string tableName, string stream, string operatorId);

        [LoggerMessage(
          EventId = 5,
          Level = LogLevel.Error,
          Message = "Error reading data from sql server table {tableName} in stream: `{stream}`, operator: `{operatorId}`")]
        public static partial void ErrorReadingData(this ILogger logger, Exception e, string tableName, string stream, string operatorId);

        [LoggerMessage(
          EventId = 6,
          Level = LogLevel.Information,
          Message = "Waiting for {time} seconds in stream: `{stream}`, operator: `{operatorId}`")]
        public static partial void WaitingSeconds(this ILogger logger, int time, string stream, string operatorId);

        [LoggerMessage(
          EventId = 7,
          Level = LogLevel.Information,
          Message = "Retrying count: {retryCount} in stream: `{stream}`, operator: `{operatorId}`")]
        public static partial void RetryingCount(this ILogger logger, int retryCount, string stream, string operatorId);

        [LoggerMessage(
          EventId = 8,
          Level = LogLevel.Information,
          Message = "Starting database update in stream: `{stream}`, operator: `{operatorId}`")]
        public static partial void StartingDatabaseUpdate(this ILogger logger, string stream, string operatorId);

        [LoggerMessage(
          EventId = 9,
          Level = LogLevel.Information,
          Message = "Database update complete in stream: `{stream}`, operator: `{operatorId}`")]
        public static partial void DatabaseUpdateComplete(this ILogger logger, string stream, string operatorId);

        [LoggerMessage(
            EventId = 10,
            Level = LogLevel.Information,
            Message = "Selecting changes from {tableName} in stream: `{stream}`, operator: `{operatorId}`")]
        public static partial void SelectingChanges(this ILogger logger, string tableName, string stream, string operatorId);
    }
}
