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

namespace FlowtideDotNet.Lineage.DataHub
{
    /// <summary>
    /// Kind of entity served to DataHub.
    /// </summary>
    public enum DataHubEntityType
    {
        /// <summary>
        /// A stream.
        /// </summary>
        DataFlow,

        /// <summary>
        /// The writes of a stream to one dataset.
        /// </summary>
        DataJob,

        /// <summary>
        /// A table read or written by a stream.
        /// </summary>
        Dataset
    }

    /// <summary>
    /// Entity passed to the aspect provider.
    /// </summary>
    public sealed class DataHubEntityContext
    {
        internal DataHubEntityContext(DataHubEntityType entityType, string urn, string? streamName, string? @namespace, string? tableName)
        {
            EntityType = entityType;
            Urn = urn;
            StreamName = streamName;
            Namespace = @namespace;
            TableName = tableName;
        }

        /// <summary>
        /// Kind of entity.
        /// </summary>
        public DataHubEntityType EntityType { get; }

        /// <summary>
        /// Entity urn.
        /// </summary>
        public string Urn { get; }

        /// <summary>
        /// Logical stream name, null for datasets.
        /// </summary>
        public string? StreamName { get; }

        /// <summary>
        /// Lineage namespace of the table, null for flows.
        /// </summary>
        public string? Namespace { get; }

        /// <summary>
        /// Connector table name, null for flows.
        /// </summary>
        public string? TableName { get; }
    }
}
