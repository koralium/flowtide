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

using System.Collections.Concurrent;
using Tenray.ZoneTree.AbstractFileStream;
using Tenray.ZoneTree.Logger;
using Tenray.ZoneTree.Options;
using Tenray.ZoneTree.Serializers;
using Tenray.ZoneTree.WAL;

namespace FlowtideDotNet.Core.Storage.Internal
{
    internal class WalProvider : IWriteAheadLogProvider, IDisposable
    {
        readonly ILogger Logger;

        readonly IFileStreamProvider FileStreamProvider;
        private readonly long maxSegmentId;
        readonly ConcurrentDictionary<string, object> WALTable = new();

        public string WalDirectory { get; }

        public WalProvider(
        ILogger logger,
        IFileStreamProvider fileStreamProvider,
        long maxSegmentId,
        string walDirectory = "data")
        {
            Logger = logger;
            FileStreamProvider = fileStreamProvider;
            this.maxSegmentId = maxSegmentId;
            WalDirectory = walDirectory;
            FileStreamProvider.CreateDirectory(walDirectory);
        }

        public Task WaitUntilCompleted()
        {
            List<Task> tasks = new List<Task>();
            foreach (var val in WALTable.Values)
            {
                if (val is CheckWal checkWal)
                {
                    tasks.Add(checkWal.Completed);
                }
            }
            return Task.WhenAll(tasks);
        }



        public void DropStore()
        {
            if (FileStreamProvider.DirectoryExists(WalDirectory))
                FileStreamProvider.DeleteDirectory(WalDirectory, true);
        }

        public IWriteAheadLog<TKey, TValue> GetOrCreateWAL<TKey, TValue>(long segmentId, string category, WriteAheadLogOptions options, ISerializer<TKey> keySerializer, ISerializer<TValue> valueSerialize)
        {
            if (WALTable.TryGetValue(segmentId + category, out var value))
            {
                return (IWriteAheadLog<TKey, TValue>)value;
            }

            //if (segmentId > maxSegmentId)
            //{
            //    var v = new EmptyWal<TKey, TValue>();
            //    WALTable.TryAdd(segmentId + category, v);
            //    return v;
            //}

            var readExisting = segmentId <= maxSegmentId;

            (var walPath, var walMode) =
            DetectWalPathAndWriteAheadLogMode(segmentId, category, options);

            var wal = new CheckpointWal<TKey, TValue>(
                        Logger,
                        FileStreamProvider,
                        keySerializer,
                        valueSerialize,
                        walPath,
                        options,
                        readExisting)
            {
                EnableIncrementalBackup = options.EnableIncrementalBackup
            };
            WALTable.TryAdd(segmentId + category, wal);

            return wal;
        }

        (string walPath, WriteAheadLogMode walMode)
        DetectWalPathAndWriteAheadLogMode(
        long segmentId, string category, WriteAheadLogOptions options)
        {
            var walPath = Path.Combine(WalDirectory, category, segmentId + ".wal.");
            var walMode = options.WriteAheadLogMode;

            // Sync = 0
            // SyncCompressed = 1
            // AsyncCompressed = 2
            // None = 3 (no file)
            for (var i = 0; i < 3; ++i)
            {
                if ((WriteAheadLogMode)i == walMode)
                    continue;
                if (FileStreamProvider.FileExists(walPath + i))
                {
                    walMode = (WriteAheadLogMode)i;
                    break;
                }
            }
            return (walPath + (int)walMode, walMode);
        }

        public IWriteAheadLog<TKey, TValue> GetWAL<TKey, TValue>(long segmentId, string category)
        {
            if (WALTable.TryGetValue(segmentId + category, out var value))
            {
                return (IWriteAheadLog<TKey, TValue>)value;
            }
            return null;
        }

        public void InitCategory(string category)
        {
            var categoryPath = Path.Combine(WalDirectory, category);
            FileStreamProvider.CreateDirectory(categoryPath);
        }

        public bool RemoveWAL(long segmentId, string category)
        {
            if (WALTable.Remove(segmentId + category, out var wal))
            {


                //var w = (CheckWal)wal;
                //w.Drop();
                return true;
            }
            return false;
        }

        public void Dispose()
        {
            foreach(var checkpointwal in WALTable)
            {
                if (checkpointwal.Value is CheckWal wal)
                {
                    wal.Dispose();
                }
            }
        }
    }
}
