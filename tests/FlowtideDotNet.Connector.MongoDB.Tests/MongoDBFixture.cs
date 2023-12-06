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

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Testcontainers.MongoDb;

namespace FlowtideDotNet.Connector.MongoDB.Tests
{
    public class MongoDBFixture : IAsyncLifetime
    {
        private readonly MongoDbContainer _mongoDbContainer =
        new MongoDbBuilder().Build();

        public string GetConnectionString()
        {
            return _mongoDbContainer.GetConnectionString();
        }

        public async Task DisposeAsync()
        {
            await _mongoDbContainer.DisposeAsync();
        }

        public async Task InitializeAsync()
        {
            await _mongoDbContainer.StartAsync();
        }
    }
}
