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
using Elastic.Clients.Elasticsearch.Mapping;
using FlowtideDotNet.Base;
using FlowtideDotNet.Core.Operators.Write;
using FlowtideDotNet.Substrait.Relations;

namespace FlowtideDotNet.Connector.ElasticSearch
{
    public class FlowtideElasticsearchOptions
    {
        /// <summary>
        /// The elasticsearch client settings
        /// It is a function to allow fetching new credentials
        /// </summary>
        public required Func<ElasticsearchClientSettings> ConnectionSettings { get; set; }

        /// <summary>
        /// Action to apply custom mappings to the index
        /// This will be called on startup.
        /// 
        /// If the index does not exist the properties will be empty, and they are sent in the create index request.
        /// </summary>
        public Action<Properties>? CustomMappings { get; set; }

        /// <summary>
        /// Returns the index name for a write relation, defaults to the table name.
        /// </summary>
        public Func<WriteRelation, string>? GetIndexNameFunc { get; set; }

        /// <summary>
        /// Called before the sink creates an index that does not exist.
        /// Change the request in the context to set settings such as shards, replicas, refresh interval and analyzers, aliases or mappings.
        ///
        /// Not called if the index already exists, settings are never applied to an existing index.
        /// Runs on startup after <see cref="CustomMappings"/>, it can run concurrently and in multiple processes for the same index.
        /// If another process creates the index first, only the mapping properties are applied.
        /// Settings, aliases and other mapping options such as Dynamic are discarded.
        /// </summary>
        public Func<FlowtideElasticsearchIndexCreationContext, Task>? OnIndexCreation { get; set; }

        /// <summary>
        /// Function that gets called after the initial data has been saved to elasticsearch.
        /// Parameters are the elasticsearch client, the write relation and the index name.
        /// 
        /// This function can be used for instance to create an alias to the index.
        /// </summary>
        public Func<ElasticsearchClient, WriteRelation, string, Task>? OnInitialDataSent { get; set; }

        /// <summary>
        /// Called each time after data has been sent to elasticsearch
        /// </summary>
        public Func<ElasticsearchClient, WriteRelation, string, Watermark, Task>? OnDataSent { get; set; }

        public ExecutionMode ExecutionMode { get; set; } = ExecutionMode.Hybrid;
    }
}