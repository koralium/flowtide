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

namespace FlowtideDotNet.Base.Utils
{
    internal static partial class Log
    {
        [LoggerMessage(
            EventId = 1,
            Level = LogLevel.Information,
            Message = "Checkpoint done in stream `{stream}`.")]
        public static partial void CheckpointDone(this ILogger logger, string stream);

        [LoggerMessage(
            EventId = 2,
            Level = LogLevel.Information,
            Message = "Stream `{stream}` is in running state")]
        public static partial void StreamIsInRunningState(this ILogger logger, string stream);

        [LoggerMessage(
            EventId = 3,
            Level = LogLevel.Information,
            Message = "Starting checkpoint for stream `{stream}`.")]
        public static partial void StartingCheckpoint(this ILogger logger, string stream);

        [LoggerMessage(
            EventId = 4,
            Level = LogLevel.Information,
            Message = "Watermark system initialized for stream `{stream}`.")]
        public static partial void WatermarkSystemInitialized(this ILogger logger, string stream);

        [LoggerMessage(
            EventId = 5,
            Level = LogLevel.Information,
            Message = "Starting stream: `{stream}`")]
        public static partial void StartingStream(this ILogger logger, string stream);

        [LoggerMessage(
            EventId = 6,
            Level = LogLevel.Trace,
            Message = "Setting up blocks for stream: `{stream}`")]
        public static partial void SettingUpBlocks(this ILogger logger, string stream);

        [LoggerMessage(
           EventId = 7,
           Level = LogLevel.Trace,
           Message = "Linking blocks together in stream: `{stream}`")]
        public static partial void LinkingBlocks(this ILogger logger, string stream);

        [LoggerMessage(
           EventId = 8,
           Level = LogLevel.Trace,
           Message = "Initializing propagator blocks in stream: `{stream}`")]
        public static partial void InitializingPropagatorBLocks(this ILogger logger, string stream);

        [LoggerMessage(
           EventId = 9,
           Level = LogLevel.Trace,
           Message = "Starting checkpoint done task for stream: `{stream}`")]
        public static partial void StartCheckpointDoneTask(this ILogger logger, string stream);

        [LoggerMessage(
           EventId = 10,
           Level = LogLevel.Trace,
           Message = "Starting state manager checkpoint for stream: `{stream}`")]
        public static partial void StartingStateManagerCheckpoint(this ILogger logger, string stream);

        [LoggerMessage(
           EventId = 11,
           Level = LogLevel.Trace,
           Message = "State manager checkpoint done for stream: `{stream}`")]
        public static partial void StateManagerCheckpointDone(this ILogger logger, string stream);

        [LoggerMessage(
           EventId = 12,
           Level = LogLevel.Trace,
           Message = "Starting compaction on all vertices for stream: `{stream}`")]
        public static partial void StartingCompactionOnVertices(this ILogger logger, string stream);

        [LoggerMessage(
           EventId = 13,
           Level = LogLevel.Trace,
           Message = "Compaction done on all vertices for stream: `{stream}`")]
        public static partial void CompactionDoneOnVertices(this ILogger logger, string stream);

        [LoggerMessage(
           EventId = 14,
           Level = LogLevel.Trace,
           Message = "Initializing egress blocks for stream: `{stream}`")]
        public static partial void InitializingEgressBlocks(this ILogger logger, string stream);

        [LoggerMessage(
           EventId = 15,
           Level = LogLevel.Trace,
           Message = "Initializing ingress blocks for stream: `{stream}`")]
        public static partial void InitializingIngressBlocks(this ILogger logger, string stream);

        [LoggerMessage(
           EventId = 16,
           Level = LogLevel.Information,
           Message = "Initializing watermark system for stream: `{stream}`")]
        public static partial void InitializingWatermarkSystem(this ILogger logger, string stream);

        [LoggerMessage(
           EventId = 17,
           Level = LogLevel.Trace,
           Message = "Checking stream hash consistency on stream: `{stream}`")]
        public static partial void CheckingStreamHashConsistency(this ILogger logger, string stream);

        [LoggerMessage(
           EventId = 18,
           Level = LogLevel.Trace,
           Message = "Checkpoint in operator, stream `{stream}` operator `{operatorId}`")]
        public static partial void CheckpointInOperator(this ILogger logger, string stream, string operatorId);

        [LoggerMessage(
           EventId = 19,
           Level = LogLevel.Trace,
           Message = "Calling egress checkpoint done, current state: `{state}` in stream `{stream}`")]
        public static partial void CallingEgressCheckpointDone(this ILogger logger, string stream, string state);

        [LoggerMessage(
           EventId = 20,
           Level = LogLevel.Trace,
           Message = "Calling checkpoint done in stream `{stream}`, operator `{operatorId}`")]
        public static partial void CallingCheckpointDone(this ILogger logger, string stream, string operatorId);

        [LoggerMessage(
           EventId = 21,
           Level = LogLevel.Error,
           Message = "Checkpoint done function not set on egress, checkpoint wont be able to complete, in stream `{stream}`, operator `{operatorId}`")]
        public static partial void CheckpointDoneFunctionNotSet(this ILogger logger, string stream, string operatorId);

        [LoggerMessage(
           EventId = 22,
           Level = LogLevel.Trace,
           Message = "Locking event in stream: `{stream}`, operator: `{operatorId}`")]
        public static partial void LockingEventInOperator(this ILogger logger, string stream, string operatorId);

        [LoggerMessage(
           EventId = 23,
           Level = LogLevel.Trace,
           Message = "Target {target} in checkpoint, stream: `{stream}`, operator: `{operatorId}`")]
        public static partial void TargetInCheckpoint(this ILogger logger, int target, string stream, string operatorId);

        [LoggerMessage(
          EventId = 24,
          Level = LogLevel.Warning,
          Message = "Recieved watermark without source operator id, stream: `{stream}`, operator: `{operatorId}`")]
        public static partial void RecievedWatermarkWithoutSourceOperator(this ILogger logger, string stream, string operatorId);

        [LoggerMessage(
          EventId = 25,
          Level = LogLevel.Error,
          Message = "Stream error, stream: `{stream}`")]
        public static partial void StreamError(this ILogger logger, Exception? e, string stream);

        [LoggerMessage(
            EventId = 26,
            Level = LogLevel.Information,
            Message = "Stopping stream: `{stream}`")]
        public static partial void StoppingStream(this ILogger logger, string stream);

        [LoggerMessage(
            EventId = 27,
            Level = LogLevel.Information,
            Message = "Stopped stream: `{stream}`")]
        public static partial void StoppedStream(this ILogger logger, string stream);

        [LoggerMessage(
            EventId = 28,
            Level = LogLevel.Information,
            Message = "Starting shutdown checkpoint for stream `{stream}`.")]
        public static partial void StartingShutdownCheckpoint(this ILogger logger, string stream);

        [LoggerMessage(
           EventId = 29,
           Level = LogLevel.Information,
           Message = "Shutdown checkpoint done in stream `{stream}`.")]
        public static partial void ShutdownCheckpointDone(this ILogger logger, string stream);
    }
}
