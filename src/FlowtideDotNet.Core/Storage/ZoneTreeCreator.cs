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

using FlowtideDotNet.Core.Storage.Internal;
using Tenray.ZoneTree;
using Tenray.ZoneTree.Comparers;
using Tenray.ZoneTree.Options;
using Tenray.ZoneTree.Serializers;

namespace FlowtideDotNet.Core.Storage
{
    public static class ZoneTreeCreator
    {
        public static IStorageTree<StreamEvent> CreateTree(string streamName, string operatorName, string treeName, long maxSegmentId)
        {
            return CreateTree(streamName, operatorName, treeName, new StreamEventSerializer(), new StreamEventComparer(), maxSegmentId);
        }

        public static IStorageTree<T> CreateTree<T>(string streamName, string operatorName, string treeName, ISerializer<T> serializer, IRefComparer<T> comparer, long maxSegmentId)
        {
            var dataPath = $"data/{streamName}/{operatorName}/{treeName}";
            WalProvider? walProvider = null;
            var zoneTree = new ZoneTreeFactory<T, int>(new FileStreamProvider())
                .SetDataDirectory(dataPath)
                .SetKeySerializer(serializer)
                .SetValueSerializer(new Int32Serializer())
                .SetComparer(comparer)
                .SetIsValueDeletedDelegate((in int x) => x == 0)
                .SetMutableSegmentMaxItemCount(int.MaxValue)
                .SetWriteAheadLogProvider(opt =>
                {
                    walProvider = new WalProvider(opt.Logger, opt.RandomAccessDeviceManager.FileStreamProvider, maxSegmentId, dataPath);
                    return walProvider;
                })
                .ConfigureDiskSegmentOptions(o =>
                {
                    o.DiskSegmentMode = Tenray.ZoneTree.Options.DiskSegmentMode.MultiPartDiskSegment;
                })
               .OpenOrCreate();

            if (walProvider == null)
            {
                throw new NotSupportedException("Wal provider could not be created");
            }

            return new ZoneTreeContainer<T>(zoneTree, walProvider);
        }

        internal static (IZoneTree<TK,TV>, WalProvider) CreateTree<TK, TV>(
            string streamName,
            string operatorName, 
            string treeName, 
            ISerializer<TK> serializer, 
            ISerializer<TV> valueSerializer, 
            IRefComparer<TK> kcomparer, 
            long maxSegmentId,
            IsValueDeletedDelegate<TV> valueDeleted)
        {
            var dataPath = $"data/{streamName}/{operatorName}/{treeName}";
            WalProvider? walProvider = null;
            var zoneTree = new ZoneTreeFactory<TK, TV>(new FileStreamProvider())
                .SetDataDirectory(dataPath)
                .SetKeySerializer(serializer)
                .SetValueSerializer(valueSerializer)
                .SetComparer(kcomparer)
                .SetMutableSegmentMaxItemCount(int.MaxValue)
                .SetIsValueDeletedDelegate(valueDeleted)
                .SetWriteAheadLogProvider(opt =>
                {
                    walProvider = new WalProvider(opt.Logger, opt.RandomAccessDeviceManager.FileStreamProvider, maxSegmentId, dataPath);
                    return walProvider;
                })
                .ConfigureDiskSegmentOptions(o =>
                {
                    o.DiskSegmentMode = Tenray.ZoneTree.Options.DiskSegmentMode.MultiPartDiskSegment;
                })
               .OpenOrCreate();

            if (walProvider == null)
            {
                throw new NotSupportedException("Wal provider could not be created");
            }

            return (zoneTree, walProvider);
        }
    }
}
