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

using FlowtideDotNet.Substrait.Relations;
using Polly;
using Polly.Retry;

namespace FlowtideDotNet.Connector.SqlServer
{
    public class SqlServerSourceOptions
    {
        public required Func<string> ConnectionStringFunc { get; set; }

        public Func<ReadRelation, IReadOnlyList<string>>? TableNameTransform { get; set; }

        public bool UseDatabaseDefinedInConnectionStringOnly { get; set; }

        /// <summary>
        /// Allows flowtide to do a full reload of the data on interval when watch/change stream is not supported.
        /// Requires that a value is set for <see cref="FullReloadInterval"/> as well. 
        /// </summary>
        public bool EnableFullReload { get; set; }

        /// <summary>
        /// Allows flowtide to do a full reload of the data on interval when change tracking is not supported on a table.
        /// Requires that a value is set for <see cref="FullReloadInterval"/> as well. 
        /// </summary>
        public bool AllowFullReloadOnTablesWithoutChangeTracking { get; set; }

        /// <summary>
        /// The max number of rows that are allowed to for <see cref="EnableFullReload"/>.
        /// If set to null this check is skipped. Default is 1 000 000 rows.
        /// </summary>
        public int? FullLoadMaxRowCount { get; set; } = 1_000_000;

        /// <summary>
        /// Interval for full reload of the data when watch/change stream is not supported.
        /// </summary>
        public TimeSpan? FullReloadInterval { get; set; }

        /// <summary>
        /// Interval for change tracking. Default to 1 second.
        /// </summary>
        public TimeSpan? ChangeTrackingInterval { get; set; } = TimeSpan.FromSeconds(1);

        public SqlServerSourceOptions()
        {

            ResiliencePipeline = new ResiliencePipelineBuilder()
                .AddRetry(new RetryStrategyOptions
                {
                    MaxRetryAttempts = 10,
                    DelayGenerator = (args) =>
                    {
                        if (args.AttemptNumber < 5)
                        {
                            var seconds = args.AttemptNumber == 1 ? 1 : args.AttemptNumber * 5;
                            return ValueTask.FromResult<TimeSpan?>(TimeSpan.FromSeconds(seconds));
                        }

                        return ValueTask.FromResult<TimeSpan?>(TimeSpan.FromMinutes(args.AttemptNumber - 4));
                    },
                })
                .Build();
        }

        /// <summary>
        /// Resilience pipeline for the source.
        /// The default pipeline waits and retries 10 times with increasing intervals.
        /// </summary>
        public ResiliencePipeline ResiliencePipeline { get; set; }

        internal bool IsView { get; set; }

        /// <summary>
        /// Tracks if change tracking is enabled on the table.
        /// </summary>
        internal bool IsChangeTrackingEnabled { get; set; } = true;
    }
}
