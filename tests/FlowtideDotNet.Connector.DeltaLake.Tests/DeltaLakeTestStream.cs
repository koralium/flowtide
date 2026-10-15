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
using FlowtideDotNet.Core.ColumnStore;
using FlowtideDotNet.Core.ColumnStore.ObjectConverter;
using FlowtideDotNet.Storage.Memory;
using FlowtideDotNet.Storage.Persistence;
using FlowtideDotNet.Storage.Persistence.Reservoir;
using FlowtideDotNet.Storage.Persistence.Reservoir.Internal;
using Stowage;

namespace FlowtideDotNet.Connector.DeltaLake.Tests
{
    internal class DeltaLakeTestStream : FlowtideTestStream
    {
        private readonly IFileStorage location;
        private readonly bool oneVersionPerCheckpoint;
        private readonly KeepAliveMemoryFileProvider? stateFiles;
        private readonly List<EventBatchData>? published;
        private readonly Action<DeltaLakeOptions>? configure;

        /// <param name="stateFiles">Shared state files, a new stream over the same files restores the last checkpoint.</param>
        /// <param name="recordPublished">Keeps a copy of every published snapshot for <see cref="WaitForPublished"/>.</param>
        /// <param name="configure">Changes the source options after the defaults are set.</param>
        public DeltaLakeTestStream(string testName, IFileStorage location, bool oneVersionPerCheckpoint = true, KeepAliveMemoryFileProvider? stateFiles = null, bool recordPublished = false, Action<DeltaLakeOptions>? configure = null) : base(testName)
        {
            this.location = location;
            this.oneVersionPerCheckpoint = oneVersionPerCheckpoint;
            this.stateFiles = stateFiles;
            published = recordPublished ? new List<EventBatchData>() : null;
            this.configure = configure;
        }

        protected override void AddReadResolvers(IConnectorManager connectorManger)
        {
            connectorManger.AddDeltaLakeSource(CreateOptions());

            connectorManger.AddCatalog("catalogtest", c =>
            {
                c.AddDeltaLakeSource(CreateOptions());
            });
        }

        private DeltaLakeOptions CreateOptions()
        {
            var options = new DeltaLakeOptions() { StorageLocation = location, OneVersionPerCheckpoint = oneVersionPerCheckpoint, DeltaCheckInterval = TimeSpan.FromMilliseconds(100) };
            configure?.Invoke(options);
            return options;
        }

        protected override IPersistentStorage CreatePersistentStorage(string testName, bool ignoreSameDataCheck)
        {
            return stateFiles != null
                ? new ReservoirPersistentStorage(new ReservoirStorageOptions() { FileProvider = stateFiles })
                : base.CreatePersistentStorage(testName, ignoreSameDataCheck);
        }

        // Copied, the sink disposes a snapshot when it publishes the next one.
        protected override void OnDataUpdate(EventBatchData actualData)
        {
            if (published != null)
            {
                var copy = new EventBatchData(actualData.Columns.Select(c => (IColumn)c.Copy(GlobalMemoryManager.Instance)).ToArray());
                lock (published)
                {
                    published.Add(copy);
                }
            }
            base.OnDataUpdate(actualData);
        }

        /// <summary>
        /// The n-th published snapshot, counted from one; WaitForUpdate can skip versions.
        /// </summary>
        public async Task<EventBatchData> WaitForPublished(int n)
        {
            Assert.NotNull(published);
            while (true)
            {
                lock (published)
                {
                    if (published.Count >= n)
                    {
                        return published[n - 1];
                    }
                }
                await WaitForUpdate();
            }
        }

        public async Task AssertPublishedEqual<T>(int n, IEnumerable<T> data)
        {
            var actual = await WaitForPublished(n);
            using var expected = BatchConverter.ConvertToBatchSorted(data, GlobalMemoryManager.Instance);
            EventBatchAssertion.Equal(expected, actual);
        }
    }
}
