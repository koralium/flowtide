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
    /// Check passed to the incident priority resolver.
    /// </summary>
    public sealed class DataHubIncidentContext
    {
        internal DataHubIncidentContext(string streamName, string checkMessage, string assertionUrn, string datasetUrn, string @namespace, string tableName)
        {
            StreamName = streamName;
            CheckMessage = checkMessage;
            AssertionUrn = assertionUrn;
            DatasetUrn = datasetUrn;
            Namespace = @namespace;
            TableName = tableName;
        }

        /// <summary>
        /// Logical stream name.
        /// </summary>
        public string StreamName { get; }

        /// <summary>
        /// Message template of the check.
        /// </summary>
        public string CheckMessage { get; }

        /// <summary>
        /// Urn of the check's assertion.
        /// </summary>
        public string AssertionUrn { get; }

        /// <summary>
        /// Urn of the dataset the check guards.
        /// </summary>
        public string DatasetUrn { get; }

        /// <summary>
        /// Lineage namespace of the table.
        /// </summary>
        public string Namespace { get; }

        /// <summary>
        /// Connector table name.
        /// </summary>
        public string TableName { get; }
    }
}
