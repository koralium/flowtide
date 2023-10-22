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

namespace FlowtideDotNet.Core.Storage
{
    internal class ZoneTreeContainer<T> : IStorageTree<T>
    {
        private readonly IZoneTree<T, int> tree;
        private readonly WalProvider walProvider;

        public ZoneTreeContainer(IZoneTree<T, int> tree, WalProvider walProvider)
        {
            this.tree = tree;
            this.walProvider = walProvider;
        }

        public long CurrentSegmentId
        {
            get
            {
                return tree.Maintenance.ReadOnlySegments.LastOrDefault()?.SegmentId ?? tree.Maintenance.DiskSegment?.SegmentId ?? 0;
            }
        }

        public void Add(T streamEvent, int weight)
        {
            tree.Upsert(streamEvent, weight);
        }

        public IStorageTree<T>.Weights UpsertAndGetWeights(T streamEvent, int w)
        {
            int previousWeight = 0;
            int weight = w;
            tree.TryAtomicAddOrUpdate(streamEvent, w, bool (ref int v) =>
            {
                previousWeight = v;
                v += w;
                weight = v;
                return true;
            });
            return new IStorageTree<T>.Weights(previousWeight, weight);
        }

        public Task Checkpoint()
        {
            tree.Maintenance.MoveMutableSegmentForward();
            return walProvider.WaitUntilCompleted();
        }

        public Task Compact()
        {
            var mergeThread = tree.Maintenance.StartMergeOperation();
            mergeThread?.Join();
            return Task.CompletedTask;
        }

        public int GetWeights(T streamEvent)
        {
            if (tree.TryGet(streamEvent, out var val))
            {
                return val;
            }
            return 0;
        }

        public IZoneTreeIterator<T, int> CreateIterator()
        {
            return tree.CreateIterator(IteratorType.NoRefresh);
        }

        public void Dispose()
        {
            tree.Dispose();
        }

        public Task DeleteAsync()
        {
            tree.Maintenance.DestroyTree();
            return Task.CompletedTask;
        }
    }
}
