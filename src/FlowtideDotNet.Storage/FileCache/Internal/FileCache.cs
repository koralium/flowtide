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

using FlowtideDotNet.Storage.FileCache.Internal;
using FlowtideDotNet.Storage.FileCache.Internal.Unix;
using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using System.Runtime.InteropServices;

namespace FlowtideDotNet.Storage.FileCache
{
    internal class FileCache : IDisposable
    {
        private struct FreePage : IComparable<FreePage>
        {
            public FreePage(long size, LinkedListNode<Allocation>? node = null)
            {
                Size = size;
                Node = node;
            }

            public long Size { get; }
            public LinkedListNode<Allocation>? Node { get; set; }

            public int CompareTo(FreePage other)
            {
                var c = Size.CompareTo(other.Size);
                if (c != 0)
                {
                    return c;
                }
                if (Node == null)
                {
                    return -1;
                }
                if (other.Node == null)
                {
                    return 1;
                }
                return Node.ValueRef.position.CompareTo(other.Node.ValueRef.position);
            }

            public override bool Equals([NotNullWhen(true)] object? obj)
            {
                if (obj is FreePage page)
                {
                    return this.CompareTo(page) == 0;
                }

                return false;
            }

            public override int GetHashCode()
            {
                return HashCode.Combine(Size, Node?.ValueRef.position);
            }

            public static bool operator ==(FreePage left, FreePage right)
            {
                return left.Equals(right);
            }

            public static bool operator !=(FreePage left, FreePage right)
            {
                return !(left == right);
            }

            public static bool operator <(FreePage left, FreePage right)
            {
                return left.CompareTo(right) < 0;
            }

            public static bool operator <=(FreePage left, FreePage right)
            {
                return left.CompareTo(right) <= 0;
            }

            public static bool operator >(FreePage left, FreePage right)
            {
                return left.CompareTo(right) > 0;
            }

            public static bool operator >=(FreePage left, FreePage right)
            {
                return left.CompareTo(right) >= 0;
            }

            public override string ToString()
            {
                return $"Size: {Size}, Node: {Node?.ValueRef.position}";
            }
        }

        private readonly long cacheSegmentSize;
        private readonly object m_lock = new object();
        private readonly FileCacheOptions fileCacheOptions;
        private readonly string fileName;
        private readonly int m_sectorSize;
        private readonly Dictionary<long, LinkedListNode<Allocation>> allocatedPages = new Dictionary<long, LinkedListNode<Allocation>>();
        private readonly LinkedList<Allocation> memoryNodes = new LinkedList<Allocation>();
        private readonly SortedSet<FreePage> _freePages = new SortedSet<FreePage>();

        private readonly Dictionary<int, IFileCacheWriter> segmentWriters = new Dictionary<int, IFileCacheWriter>();
        private readonly Func<int, IFileCacheWriter> createWriterFunc;
        private bool disposedValue;

        public FileCache(FileCacheOptions fileCacheOptions, string fileName)
        {
            cacheSegmentSize = fileCacheOptions.SegmentSize;
            this.fileCacheOptions = fileCacheOptions;
            this.fileName = fileName;

            m_sectorSize = GetSectorSize();

            if (RuntimeInformation.IsOSPlatform(OSPlatform.Linux) && fileCacheOptions.UseDirectIOOnLinux)
            {
                createWriterFunc = (i) => new FileCacheUnixDirectWriter(GenerateFileName(i), m_sectorSize, fileCacheOptions);
            }
            else
            {
                createWriterFunc = (i) => new FileCacheSegmentWriter(GenerateFileName(i), fileCacheOptions);
            }
        }

        private int GetSectorSize()
        {
            if (RuntimeInformation.IsOSPlatform(OSPlatform.Windows))
            {
                if (WindowsNative.GetDiskFreeSpace(GenerateFileName(0), out uint lpSectorsPerCluster,
                                        out uint sectorSize,
                                        out uint lpNumberOfFreeClusters,
                                        out uint lpTotalNumberOfClusters))
                {
                    return (int)sectorSize;
                }
                return 512;
            }
            else if (RuntimeInformation.IsOSPlatform(OSPlatform.Linux))
            {
                return UnixFileSystemInfo.GetAlignment(GenerateFileName(0));
            }
            return 512;
        }

        public void Free(in long pageKey)
        {
            lock (m_lock)
            {
                Free_NoLock(pageKey);
            }
        }

        private string GenerateFileName(int fileNumber)
        {
            return Path.Combine(fileCacheOptions.DirectoryPath, $"fileCache.{fileName}.{fileNumber}.data");
        }

        private void CheckifSegmentCanBeRemoved(LinkedListNode<Allocation> node)
        {
            if (((node.Previous != null && node.Previous.ValueRef.fileNumber != node.ValueRef.fileNumber) || node.Previous == null) &&
                        (node.Next == null || node.ValueRef.fileNumber != node.Next.ValueRef.fileNumber))
            {
                // Remove the node completely
                node.ValueRef.removeLocation = "109";
                _freePages.Remove(new FreePage(node.ValueRef.allocatedSize, node));
                memoryNodes.Remove(node);
                if (segmentWriters.TryGetValue(node.ValueRef.fileNumber, out var segment))
                {
                    segment.Dispose();
                    segmentWriters.Remove(node.ValueRef.fileNumber);
                }
                else
                {
                    throw new InvalidOperationException("Segment not found");
                }
            }
        }

        private void Free_NoLock(in long pageKey)
        {
            Debug.Assert(Monitor.IsEntered(m_lock));
            if (allocatedPages.TryGetValue(pageKey, out var node))
            {
                if (node.Next != null && node.Next.ValueRef.pageKey == null && node.Next.ValueRef.fileNumber == node.ValueRef.fileNumber)
                {
                    node.ValueRef.pageKey = null;
                    node.ValueRef.allocatedSize = node.ValueRef.allocatedSize + node.Next.ValueRef.allocatedSize;
                    // remove the next node since we took its size
                    node.Next.ValueRef.removeLocation = "134";
                    _freePages.Remove(new FreePage(node.Next.ValueRef.allocatedSize, node.Next));
                    memoryNodes.Remove(node.Next);
                    allocatedPages.Remove(pageKey);
                    

                    // Check if we can remove this segment
                    CheckifSegmentCanBeRemoved(node);
                }
                // Check if the previous node exist and it is free, if so, give the size of this node to the previous node.
                // They must also be on the same file segment to be able to be merged.
                if (node.Previous != null && node.Previous.ValueRef.pageKey == null && node.Previous.ValueRef.fileNumber == node.ValueRef.fileNumber)
                {
                    var previousNode = node.Previous;
                    // Remove the previous node since we need to update its location since it increased in since
                    _freePages.Remove(new FreePage(node.Previous.ValueRef.allocatedSize, node.Previous));
                    node.Previous.ValueRef.allocatedSize = node.Previous.ValueRef.allocatedSize + node.ValueRef.allocatedSize;
                    // Readd the previous node
                    _freePages.Add(new FreePage(node.Previous.ValueRef.allocatedSize, node.Previous));

                    // remove the node
                    if (node.List != null)
                    {
                        node.ValueRef.removeLocation = "153";
                        memoryNodes.Remove(node);
                    }
                    allocatedPages.Remove(pageKey);

                    CheckifSegmentCanBeRemoved(previousNode);
                }
                // Check if this is the only free page in a segment, this allows deletion of the entire segment.
                else if (node.List != null &&
                        ((node.Previous != null && node.Previous.ValueRef.fileNumber != node.ValueRef.fileNumber) || node.Previous == null) &&
                        (node.Next == null || node.ValueRef.fileNumber != node.Next.ValueRef.fileNumber))
                {
                    // Remove the node completely
                    if (node.List != null)
                    {
                        _freePages.Remove(new FreePage(node.ValueRef.allocatedSize, node));
                        node.ValueRef.removeLocation = "168";
                        memoryNodes.Remove(node);
                    }

                    
                    allocatedPages.Remove(pageKey);
                    // Schedule a remove file task for that segment
                    if (segmentWriters.TryGetValue(node.ValueRef.fileNumber, out var segment))
                    {
                        segment.Dispose();
                        segmentWriters.Remove(node.ValueRef.fileNumber);
                    }
                    else
                    {
                        throw new InvalidOperationException("Segment not found");
                    }
                }
                // Check if there is a free node after this node
                else
                {
                    // Only free this allocation
                    node.ValueRef.pageKey = null;
                    allocatedPages.Remove(pageKey);
                }

                if (node.List != null)
                {
                    // Add the page to the free pages if it is not removed
                    _freePages.Add(new FreePage(node.ValueRef.allocatedSize, node));
                }
            }
        }

        public void FreeAll()
        {
            lock (m_lock)
            {
                var keys = allocatedPages.Keys.ToList();
                foreach (var key in keys)
                {
                    Free_NoLock(key);
                }
            }
        }

        public void Allocate(long pageKey, int size)
        {
            lock (m_lock)
            {
                Allocate_NoLock(pageKey, size);
            }
        }

        private void Allocate_NoLock(long pageKey, int size)
        {
            Debug.Assert(Monitor.IsEntered(m_lock));

            var allocationSize = UpperPowerOfTwo(size);

            // If there are no memory nodes, initialize them and a segment file.
            if (memoryNodes.Count == 0)
            {
                segmentWriters.Add(0, createWriterFunc(0));
                var startNode = memoryNodes.AddLast(new Allocation()
                {
                    fileNumber = 0,
                    pageKey = null,
                    position = 0,
                    allocatedSize = cacheSegmentSize,
                    size = 0
                });
                _freePages.Add(new FreePage(cacheSegmentSize, startNode));
            }

            // Get pages that are free and can fit the allocation size
            var freePagesView = _freePages.GetViewBetween(new FreePage(allocationSize), new FreePage(long.MaxValue));

            var page = freePagesView.FirstOrDefault();

            if (page.Node == null)
            {
                throw new InvalidOperationException("Cache is full");
            }

            var iteratorNode = page.Node;
            iteratorNode.ValueRef.pageKey = pageKey;
            iteratorNode.ValueRef.size = size;

            allocatedPages.Add(pageKey, iteratorNode);

            var newNode = new LinkedListNode<Allocation>(new Allocation()
            {
                pageKey = null,
                position = iteratorNode.ValueRef.position + allocationSize,
                allocatedSize = iteratorNode.ValueRef.allocatedSize - allocationSize
            });
            iteratorNode.ValueRef.allocatedSize = allocationSize;

            memoryNodes.AddAfter(iteratorNode, newNode);
            _freePages.Remove(page);
            _freePages.Add(new FreePage(newNode.ValueRef.allocatedSize, newNode));
        }

        /// <summary>
        /// Write data for a page key to storage
        /// </summary>
        /// <param name="pageKey"></param>
        /// <param name="data"></param>
        /// <returns></returns>
        public void WriteAsync(long pageKey, byte[] data)
        {
            long position = 0;
            IFileCacheWriter? segmentWriter = null;
            lock (m_lock)
            {
                if (allocatedPages.TryGetValue(pageKey, out var node))
                {
                    // Check if the current node has enough size
                    if (node.ValueRef.allocatedSize >= data.Length)
                    {
                        if (segmentWriters.TryGetValue(node.ValueRef.fileNumber, out segmentWriter))
                        {
                            position = node.ValueRef.position;
                            node.ValueRef.size = data.Length;
                        }
                    }
                    else
                    {
                        // Free the previous page
                        Free(pageKey);
                        // Create a new allocation
                        Allocate_NoLock(pageKey, data.Length);
                        if (allocatedPages.TryGetValue(pageKey, out var newNode))
                        {
                            if (segmentWriters.TryGetValue(newNode.ValueRef.fileNumber, out segmentWriter))
                            {
                                position = newNode.ValueRef.position;
                                newNode.ValueRef.size = data.Length;
                            }
                        }
                    }
                }
                else
                {
                    Allocate_NoLock(pageKey, data.Length);
                    if (allocatedPages.TryGetValue(pageKey, out var newNode))
                    {
                        if (segmentWriters.TryGetValue(newNode.ValueRef.fileNumber, out segmentWriter))
                        {
                            position = newNode.ValueRef.position;
                            newNode.ValueRef.size = data.Length;
                        }
                    }
                }
            }

            if (segmentWriter == null)
            {
                throw new InvalidOperationException("Segment not found");
            }

            segmentWriter.Write(position, data);
        }

        public bool Exists(long pageKey)
        {
            lock (m_lock)
            {
                return allocatedPages.ContainsKey(pageKey);
            }
        }

        public byte[] Read(long pageKey)
        {
            IFileCacheWriter? segmentWriter = default;
            long position = 0;
            int size = 0;
            lock (m_lock)
            {
                if (allocatedPages.TryGetValue(pageKey, out var node) &&
                    segmentWriters.TryGetValue(node.ValueRef.fileNumber, out var segment))
                {
                    position = node.ValueRef.position;
                    size = node.ValueRef.size;
                    segmentWriter = segment;
                }
            }

            if (segmentWriter != null)
            {
                return segmentWriter.Read(position, size);
            }
            throw new InvalidOperationException("Segment not found");
        }

        private int UpperPowerOfTwo(int v)
        {
            // sector size is the smallest allocation size
            if (v < m_sectorSize)
            {
                return m_sectorSize;
            }
            v--;
            v |= v >> 1;
            v |= v >> 2;
            v |= v >> 4;
            v |= v >> 8;
            v |= v >> 16;
            v++;
            return v;

        }

        public void Flush()
        {
            lock(m_lock)
            {
                foreach(var writer in segmentWriters)
                {
                    writer.Value.Flush();
                }
            }
        }

        public void ClearTemporaryAllocations()
        {
            lock (m_lock)
            {
                foreach(var writer in segmentWriters)
                {
                    writer.Value.ClearTemporaryAllocations();
                }
            }
        }

        protected virtual void Dispose(bool disposing)
        {
            if (!disposedValue)
            {
                if (disposing)
                {
                    lock (m_lock)
                    {
                        allocatedPages.Clear();
                        memoryNodes.Clear();
                        _freePages.Clear();
                        foreach(var segment in segmentWriters)
                        {
                            segment.Value.Dispose();
                        }
                        segmentWriters.Clear();
                    }
                }
                disposedValue = true;
            }
        }

        public void Dispose()
        {
            // Do not change this code. Put cleanup code in 'Dispose(bool disposing)' method
            Dispose(disposing: true);
            GC.SuppressFinalize(this);
        }
    }
}
