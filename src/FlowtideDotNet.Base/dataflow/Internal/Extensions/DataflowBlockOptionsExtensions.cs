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

using System.Threading.Tasks.Dataflow;

namespace DataflowStream.dataflow.Internal.Extensions
{
    internal static class DataflowBlockOptionsExtensions
    {
        public static int GetActualMaxDegreeOfParallism(this ExecutionDataflowBlockOptions options)
        {
            return options.MaxDegreeOfParallelism == DataflowBlockOptions.Unbounded ? int.MaxValue : options.MaxDegreeOfParallelism;
        }

        public static int GetActualMaxMessagesPerTask(this DataflowBlockOptions options)
        {
            return options.MaxMessagesPerTask == DataflowBlockOptions.Unbounded ? int.MaxValue : options.MaxMessagesPerTask;
        }

        public static int GetActualMaxMessagesPerTask(this GroupingDataflowBlockOptions options)
        {
            return options.MaxMessagesPerTask == DataflowBlockOptions.Unbounded ? int.MaxValue : options.MaxMessagesPerTask;
        }

        public static long GetActualMaxNumberOfGroups(this GroupingDataflowBlockOptions options)
        {
            return (options.MaxNumberOfGroups == DataflowBlockOptions.Unbounded) ? long.MaxValue : options.MaxNumberOfGroups;
        }

        public static bool GetSupportsParallelExecution(this ExecutionDataflowBlockOptions options)
        {
            return options.MaxDegreeOfParallelism == DataflowBlockOptions.Unbounded || options.MaxDegreeOfParallelism > 1;
        }

        internal static readonly ExecutionDataflowBlockOptions DefaultExecutionDataflowBlockOptions = new ExecutionDataflowBlockOptions();

        public static ExecutionDataflowBlockOptions DefaultOrClone(this ExecutionDataflowBlockOptions options)
        {
            if (options == DefaultExecutionDataflowBlockOptions)
            {
                return options;
            }

            return new ExecutionDataflowBlockOptions
            {
                TaskScheduler = options.TaskScheduler,
                CancellationToken = options.CancellationToken,
                MaxMessagesPerTask = options.MaxMessagesPerTask,
                BoundedCapacity = options.BoundedCapacity,
                NameFormat = options.NameFormat,
                EnsureOrdered = options.EnsureOrdered,
                MaxDegreeOfParallelism = options.MaxDegreeOfParallelism,
                SingleProducerConstrained = options.SingleProducerConstrained
            };
        }

        internal static readonly GroupingDataflowBlockOptions DefaultGroupingDataflowBlockOptions = new GroupingDataflowBlockOptions();

        public static GroupingDataflowBlockOptions DefaultOrClone(this GroupingDataflowBlockOptions options)
        {
            return (options == DefaultGroupingDataflowBlockOptions) ?
                options :
                new GroupingDataflowBlockOptions
                {
                    TaskScheduler = options.TaskScheduler,
                    CancellationToken = options.CancellationToken,
                    MaxMessagesPerTask = options.MaxMessagesPerTask,
                    BoundedCapacity = options.BoundedCapacity,
                    NameFormat = options.NameFormat,
                    EnsureOrdered = options.EnsureOrdered,
                    Greedy = options.Greedy,
                    MaxNumberOfGroups = options.MaxNumberOfGroups
                };
        }

        internal static readonly DataflowBlockOptions DefaultDataflowBlockOptions = new DataflowBlockOptions();

        public static DataflowBlockOptions DefaultOrClone(this DataflowBlockOptions options)
        {
            return (options == DefaultDataflowBlockOptions) ?
                options :
                new DataflowBlockOptions
                {
                    TaskScheduler = options.TaskScheduler,
                    CancellationToken = options.CancellationToken,
                    MaxMessagesPerTask = options.MaxMessagesPerTask,
                    BoundedCapacity = options.BoundedCapacity,
                    NameFormat = options.NameFormat,
                    EnsureOrdered = options.EnsureOrdered
                };
        }
    }
}
