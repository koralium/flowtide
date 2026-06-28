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
using FlowtideDotNet.Storage.Persistence;
using FlowtideDotNet.Storage.Persistence.Reservoir;
using FlowtideDotNet.Storage.Persistence.Reservoir.Internal;
using FlowtideDotNet.Storage.Persistence.Reservoir.MemoryDisk;

namespace FlowtideDotNet.Connector.PostgreSQL.Tests
{
    /// <summary>
    /// A test stream that reads from PostgreSQL and writes into the in-memory mock sink so assertions can be made
    /// with <see cref="FlowtideTestStream.AssertCurrentDataEqual{T}"/>.
    /// </summary>
    internal sealed class PostgresTestStream : FlowtideTestStream
    {
        private readonly PostgresSourceOptions _options;
        private readonly MemoryFileProvider? _fileProvider;

        public PostgresTestStream(string testName, PostgresSourceOptions options, MemoryFileProvider? fileProvider = null) : base(testName)
        {
            _options = options;
            _fileProvider = fileProvider;
        }

        protected override void AddReadResolvers(IConnectorManager connectorManager)
        {
            connectorManager.AddPostgresSource(_options);
        }

        protected override IPersistentStorage CreatePersistentStorage(string testName, bool ignoreSameDataCheck)
        {
            // When a shared file provider is supplied, checkpoint data survives the stream instance, so a second
            // stream over the same provider restores from the first stream's checkpoint (simulating a restart).
            if (_fileProvider != null)
            {
                return new ReservoirPersistentStorage(new ReservoirStorageOptions { FileProvider = _fileProvider });
            }
            return base.CreatePersistentStorage(testName, ignoreSameDataCheck);
        }
    }
}
