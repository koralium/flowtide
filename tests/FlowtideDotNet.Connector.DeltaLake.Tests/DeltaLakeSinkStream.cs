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
using FlowtideDotNet.Core;
using FlowtideDotNet.Storage.Persistence;
using FlowtideDotNet.Storage.Persistence.Reservoir;
using FlowtideDotNet.Storage.Persistence.Reservoir.Internal;
using Stowage;

namespace FlowtideDotNet.Connector.DeltaLake.Tests
{
    internal class DeltaLakeSinkStream : FlowtideTestStream
    {
        private readonly IFileStorage storage;
        private readonly Action<DeltaLakeOptions>? configureOptions;
        private readonly KeepAliveMemoryFileProvider? stateFiles;
        private readonly Func<IPersistentStorage, IPersistentStorage>? wrapState;
        private readonly bool legacySink;

        /// <param name="stateFiles">Shared state files, a new stream over the same files restores the last checkpoint.</param>
        /// <param name="wrapState">Wraps the persistent storage, for example to observe page writes.</param>
        /// <param name="legacySink">Writes with the sink as it was before Phase 2.</param>
        public DeltaLakeSinkStream(
            string testName,
            IFileStorage storage,
            Action<DeltaLakeOptions>? configureOptions = null,
            KeepAliveMemoryFileProvider? stateFiles = null,
            Func<IPersistentStorage, IPersistentStorage>? wrapState = null,
            bool legacySink = false) : base(testName)
        {
            this.storage = storage;
            this.configureOptions = configureOptions;
            this.stateFiles = stateFiles;
            this.wrapState = wrapState;
            this.legacySink = legacySink;
        }

        protected override void AddWriteResolvers(IConnectorManager connectorManger)
        {
            var options = new DeltaLakeOptions()
            {
                StorageLocation = storage
            };
            configureOptions?.Invoke(options);
            if (legacySink)
            {
                connectorManger.AddSink(new LegacyDeltaLakeSinkFactory(options));
            }
            else
            {
                connectorManger.AddDeltaLakeSink(options);
            }
        }

        protected override IPersistentStorage CreatePersistentStorage(string testName, bool ignoreSameDataCheck)
        {
            var state = stateFiles != null
                ? new ReservoirPersistentStorage(new ReservoirStorageOptions() { FileProvider = stateFiles })
                : base.CreatePersistentStorage(testName, ignoreSameDataCheck);
            return wrapState != null ? wrapState(state) : state;
        }
    }
}
