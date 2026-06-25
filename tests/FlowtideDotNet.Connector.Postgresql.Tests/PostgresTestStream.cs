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
using FlowtideDotNet.Connector.PostgreSQL;
using FlowtideDotNet.Core;

namespace FlowtideDotNet.Connector.PostgreSQL.Tests
{
    /// <summary>
    /// A test stream that reads from PostgreSQL and writes into the in-memory mock sink so assertions can be made
    /// with <see cref="FlowtideTestStream.AssertCurrentDataEqual{T}"/>.
    /// </summary>
    internal sealed class PostgresTestStream : FlowtideTestStream
    {
        private readonly PostgresSourceOptions _options;

        public PostgresTestStream(string testName, PostgresSourceOptions options) : base(testName)
        {
            _options = options;
        }

        protected override void AddReadResolvers(IConnectorManager connectorManager)
        {
            connectorManager.AddPostgresSource(_options);
        }
    }
}
