﻿// Licensed under the Apache License, Version 2.0 (the "License")
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
using FlowtideDotNet.AcceptanceTests.Internal;
using FlowtideDotNet.Base;
using FlowtideDotNet.Connector.CosmosDB.Tests;
using FlowtideDotNet.Core;
using FlowtideDotNet.Core.Engine;
using FlowtideDotNet.Substrait.Relations;

namespace FlowtideDotNet.Connector.ElasticSearch.Tests
{
    internal class ElasticsearchTestStream : FlowtideTestStream
    {
        private readonly ElasticSearchFixture elasticSearchFixture;
        private readonly Action<Properties>? customMapping;
        private readonly Func<ElasticsearchClient, WriteRelation, string, Task>? onInitialDataSent;
        private readonly Func<ElasticsearchClient, WriteRelation, string, Watermark, Task>? onDataSent;

        public ElasticsearchTestStream(
            ElasticSearchFixture elasticSearchFixture,
            string testName,
            Action<Properties>? customMapping = null,
            Func<ElasticsearchClient, WriteRelation, string, Task>? onInitialDataSent = null,
            Func<ElasticsearchClient, WriteRelation, string, Watermark, Task>? onDataSent = null)
            : base(testName)
        {
            this.elasticSearchFixture = elasticSearchFixture;
            this.customMapping = customMapping;
            this.onInitialDataSent = onInitialDataSent;
            this.onDataSent = onDataSent;
        }

        protected override void AddWriteResolvers(IConnectorManager connectorManager)
        {
            connectorManager.AddElasticsearchSink("*", new FlowtideElasticsearchOptions()
            {
                ConnectionSettings = elasticSearchFixture.GetConnectionSettings,
                CustomMappings = customMapping,
                OnDataSent = onDataSent,
                OnInitialDataSent = onInitialDataSent
            });
        }
    }
}
