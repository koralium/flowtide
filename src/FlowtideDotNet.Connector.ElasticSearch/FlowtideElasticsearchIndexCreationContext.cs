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

using Elastic.Clients.Elasticsearch;
using Elastic.Clients.Elasticsearch.IndexManagement;
using FlowtideDotNet.Substrait.Relations;

namespace FlowtideDotNet.Connector.ElasticSearch
{
    /// <summary>
    /// Passed to <see cref="FlowtideElasticsearchOptions.OnIndexCreation"/> before the sink creates an index that does not exist.
    /// </summary>
    public sealed class FlowtideElasticsearchIndexCreationContext
    {
        internal FlowtideElasticsearchIndexCreationContext(ElasticsearchClient client, WriteRelation writeRelation, string indexName, CreateIndexRequest request)
        {
            Client = client;
            WriteRelation = writeRelation;
            IndexName = indexName;
            Request = request;
        }

        /// <summary>
        /// Client the sink uses to create the index, created from <see cref="FlowtideElasticsearchOptions.ConnectionSettings"/>.
        /// </summary>
        public ElasticsearchClient Client { get; }

        /// <summary>
        /// The write relation the index is created for.
        /// </summary>
        public WriteRelation WriteRelation { get; }

        /// <summary>
        /// Name of the index, from <see cref="FlowtideElasticsearchOptions.GetIndexNameFunc"/> or the table name.
        /// </summary>
        public string IndexName { get; }

        /// <summary>
        /// The create index request, sent by the sink after the hook returns.
        ///
        /// Mappings already contain the properties from <see cref="FlowtideElasticsearchOptions.CustomMappings"/>.
        /// Settings and aliases are null until set.
        /// The index name is always reset to <see cref="IndexName"/>.
        /// </summary>
        public CreateIndexRequest Request { get; }
    }
}
