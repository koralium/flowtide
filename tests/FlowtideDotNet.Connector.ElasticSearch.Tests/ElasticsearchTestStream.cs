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

using FlowtideDotNet.AcceptanceTests.Internal;
using FlowtideDotNet.Connector.CosmosDB.Tests;
using FlowtideDotNet.Core.Engine;
using Nest;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FlowtideDotNet.Connector.ElasticSearch.Tests
{
    internal class ElasticsearchTestStream : FlowtideTestStream
    {
        private readonly ElasticSearchFixture elasticSearchFixture;
        private readonly Action<IProperties>? customMapping;

        public ElasticsearchTestStream(ElasticSearchFixture elasticSearchFixture, string testName, Action<IProperties>? customMapping = null) : base(testName)
        {
            this.elasticSearchFixture = elasticSearchFixture;
            this.customMapping = customMapping;
        }

        protected override void AddWriteResolvers(ReadWriteFactory factory)
        {
            factory.AddElasticsearchSink("*", elasticSearchFixture.GetConnectionSettings(), customMappings: customMapping);
        }
    }
}
