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

using FlowtideDotNet.Base.Engine;

namespace FlowtideDotNet.Lineage.DataHub.Internal
{
    // The committed status of one check id, the timestamp changes only with the values.
    internal sealed record DataHubCheckStatus(CheckState State, long ActiveIssues, long FailingRows, long TimestampMillis);

    internal sealed record DataHubCheckPart(long Generation, string CheckId, bool Replicated);

    internal sealed record DataHubAssertionResult(CheckState State, long ActiveIssues, long FailingRows, long TimestampMillis);

    // One assertion per check and target dataset, its parts are the check ids of every substream and partition.
    internal sealed class DataHubAssertion
    {
        public DataHubAssertion(DataHubEntityBuilder builder, string datasetUrn, string streamName, string message, string @namespace, string tableName)
        {
            Builder = builder;
            DatasetUrn = datasetUrn;
            StreamName = streamName;
            Message = message;
            Namespace = @namespace;
            TableName = tableName;
        }

        // Aspects that only change with a registration.
        public DataHubEntityBuilder Builder { get; }

        public string DatasetUrn { get; }

        public string StreamName { get; }

        public string Message { get; }

        public string Namespace { get; }

        public string TableName { get; }

        public List<DataHubCheckPart> Parts { get; } = new List<DataHubCheckPart>();

        // Null until a part has reported, parts of other processes never do.
        public DataHubAssertionResult? GetResult(Func<long, string, DataHubCheckStatus?> getStatus)
        {
            var statuses = new List<(DataHubCheckPart Part, DataHubCheckStatus Status)>();
            foreach (var part in Parts)
            {
                if (getStatus(part.Generation, part.CheckId) is DataHubCheckStatus status)
                {
                    statuses.Add((part, status));
                }
            }
            if (statuses.Count == 0)
            {
                return null;
            }

            // Not evaluated counts as passed, DataHub's INIT would mark the dataset as failing during startup.
            var state = statuses.Any(x => x.Status.State == CheckState.Failed) ? CheckState.Failed : CheckState.Passed;
            // Partition copies see disjoint rows, replicated copies the same rows.
            var failingRows = statuses.All(x => x.Part.Replicated) ? statuses.Max(x => x.Status.FailingRows) : statuses.Sum(x => x.Status.FailingRows);
            // An issue can be active in several partitions, the max never counts it twice.
            var activeIssues = statuses.Max(x => x.Status.ActiveIssues);
            return new DataHubAssertionResult(state, activeIssues, failingRows, statuses.Max(x => x.Status.TimestampMillis));
        }
    }
}
