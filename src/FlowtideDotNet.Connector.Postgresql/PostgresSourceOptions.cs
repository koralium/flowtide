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

namespace FlowtideDotNet.Connector.PostgreSQL
{
    /// <summary>
    /// Options for the PostgreSQL logical replication source.
    ///
    /// The source uses <b>temporary</b> logical replication slots so that nothing is ever left pinned on the
    /// PostgreSQL server: a slot only lives for as long as the Flowtide stream is running on this node, and is
    /// dropped automatically when the replication connection closes. Because temporary slots cannot be resumed
    /// across a reconnect, the source treats a restart as a re-snapshot, which is reconciled against state by the
    /// underlying read operator (no duplicates or losses).
    /// </summary>
    public class PostgresSourceOptions
    {
        /// <summary>
        /// Function that returns the connection string used for both snapshot reads and the replication connection.
        /// The connecting role must have the REPLICATION attribute (or be a superuser) and the server must be
        /// configured with <c>wal_level = logical</c>.
        /// </summary>
        public required Func<string> ConnectionStringFunc { get; set; }

        /// <summary>
        /// Optional transform from a read relation to the actual PostgreSQL table name parts (schema, table).
        /// When not set, the named table from the query is used directly.
        /// </summary>
        public Func<ReadRelation, IReadOnlyList<string>>? TableNameTransform { get; set; }

        /// <summary>
        /// Controls whether each table gets its own replication slot (<see cref="PostgresReplicationMode.PerTable"/>)
        /// or all tables for the same database share one slot (<see cref="PostgresReplicationMode.Shared"/>).
        /// Defaults to <see cref="PostgresReplicationMode.Shared"/>.
        /// </summary>
        public PostgresReplicationMode ReplicationMode { get; set; } = PostgresReplicationMode.Shared;

        /// <summary>
        /// When set, the source uses an existing, user-managed publication with this name instead of creating and
        /// managing one itself. The publication must already contain every table the stream reads from.
        /// When null (the default) Flowtide creates and manages a publication automatically.
        /// </summary>
        public string? PublicationName { get; set; }

        /// <summary>
        /// Prefix used when generating replication slot names. The full name also includes a hash of the stream and
        /// table set so concurrent streams do not collide.
        /// </summary>
        public string SlotPrefix { get; set; } = "flowtide";

        /// <summary>
        /// Prefix used when generating auto-managed publication names. Ignored when <see cref="PublicationName"/> is set.
        /// </summary>
        public string PublicationPrefix { get; set; } = "flowtide";

        /// <summary>
        /// How often a standby status update (LSN feedback) is sent to PostgreSQL while streaming, allowing the server
        /// to release WAL during the live session. Defaults to 10 seconds.
        /// </summary>
        public TimeSpan StatusUpdateInterval { get; set; } = TimeSpan.FromSeconds(10);

        /// <summary>
        /// How often the source polls the replication stream for accumulated changes. Defaults to 1 second.
        /// </summary>
        public TimeSpan DeltaLoadInterval { get; set; } = TimeSpan.FromSeconds(1);

        /// <summary>
        /// Capacity of the per-table bounded channel used in <see cref="PostgresReplicationMode.Shared"/> mode.
        /// When a table's channel is full the shared reader blocks (head-of-line blocking). Defaults to 1024 batches.
        /// </summary>
        public int ChannelCapacity { get; set; } = 1024;

        /// <summary>
        /// Number of rows read per batch during the initial snapshot. Defaults to 10000.
        /// </summary>
        public int SnapshotBatchSize { get; set; } = 10000;

        public PostgresSourceOptions()
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
        /// Resilience pipeline used for snapshot reads and replication reconnects.
        /// The default pipeline waits and retries 10 times with increasing intervals.
        /// </summary>
        public ResiliencePipeline ResiliencePipeline { get; set; }
    }
}
