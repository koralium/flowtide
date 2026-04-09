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

using FlowtideDotNet.Storage.Memory;
using FlowtideDotNet.Storage.Persistence.Reservoir.Internal;
using FlowtideDotNet.Storage.Persistence.Reservoir.MemoryDisk;
using Microsoft.Extensions.Logging.Abstractions;
using System.Buffers;
using Xunit;
using FlowtideDotNet.Storage.DataStructures;
using System.IO.Pipelines;
using FlowtideDotNet.Storage.Persistence.Reservoir;

namespace FlowtideDotNet.Storage.Tests.Reservoir
{
    public class CheckpointHandlerTests
    {
        internal class MockPagesFile : PagesFile
        {
            public override PrimitiveList<long> PageIds { get; }
            public override PrimitiveList<int> PageOffsets { get; }
            public override PrimitiveList<uint> Crc32s { get; }
            public override int FileSize => 100;
            public override ulong Crc64 => 12345;

            public MockPagesFile(IMemoryAllocator allocator, long pageId)
            {
                PageIds = new PrimitiveList<long>(allocator);
                PageIds.Add(pageId);
                PageOffsets = new PrimitiveList<int>(allocator);
                PageOffsets.Add(0);
                PageOffsets.Add(10);
                Crc32s = new PrimitiveList<uint>(allocator);
                Crc32s.Add(123);
            }

            public override void AdvanceTo(SequencePosition consumed) { }
            public override void AdvanceTo(SequencePosition consumed, SequencePosition examined) { }
            public override void CancelPendingRead() { }
            public override void Complete(System.Exception? exception = null) { }
            public override void DoneWriting() { }
            public override ValueTask<ReadResult> ReadAsync(System.Threading.CancellationToken cancellationToken = default) => new ValueTask<ReadResult>(new ReadResult(System.Buffers.ReadOnlySequence<byte>.Empty, false, true));
            public override void Return() { }
            public override bool TryRead(out ReadResult result) { result = new ReadResult(System.Buffers.ReadOnlySequence<byte>.Empty, false, true); return true; }
        }

        [Fact]
        public void TestIsBundleFile()
        {
            var handler = new CheckpointHandler(new MemoryFileProvider(), MemoryPool<byte>.Shared, GlobalMemoryManager.Instance, 5, NullLogger.Instance);

            Assert.False(handler.IsBundleFile_Test(1), "Normal file ID incorrectly classified as bundle file!");
            Assert.True(handler.IsBundleFile_Test(ulong.MaxValue), "Max ulong ID should be classified as bundle file!");
        }

        [Fact]
        public async Task TestDeletedPagesClears()
        {
            var provider = new MemoryFileProvider();
            var handler = new CheckpointHandler(provider, MemoryPool<byte>.Shared, GlobalMemoryManager.Instance, 5, NullLogger.Instance);

            handler.AddDeletedPages(new HashSet<long>() { 101 });
            await handler.FinishCheckpoint(null);

            Assert.Empty(handler.DeletedPages_Test);
        }

        [Fact]
        public async Task TestPageFileLocationIsRemoved()
        {
            var provider = new MemoryFileProvider();
            var allocator = GlobalMemoryManager.Instance;
            var pool = MemoryPool<byte>.Shared;
            var handler = new CheckpointHandler(provider, pool, allocator, 5, NullLogger.Instance);

            var page = new MockPagesFile(allocator, 101);

            await handler.EnqueueFileAsync(page);
            await handler.FinishCheckpoint(null);

            handler.AddDeletedPages(new HashSet<long>() { 101 });
            await handler.FinishCheckpoint(null);

            Assert.False(handler.PageFileLocations_Test.ContainsKey(101), "_pageFileLocations is leaking deleted pages!");
        }

        [Fact]
        public async Task TestWriteSnapshotRespectsDeletedPages()
        {
            var provider = new MemoryFileProvider();
            var allocator = GlobalMemoryManager.Instance;
            var pool = MemoryPool<byte>.Shared;
            var handler = new CheckpointHandler(provider, pool, allocator, 5, NullLogger.Instance);

            var page = new MockPagesFile(allocator, 202);

            await handler.EnqueueFileAsync(page);
            await handler.FinishCheckpoint(null);

            // Force snapshot
            handler.ForceSnapshotNext_Test();
            handler.AddDeletedPages(new HashSet<long>() { 202 });
            await handler.FinishCheckpoint(null);

            var filesInfo = handler.GetAllFileInformation().ToList();
            Assert.Empty(filesInfo);
        }

        [Fact]
        public async Task TestModifiedFileIdsClears()
        {
            var provider = new MemoryFileProvider();
            var handler = new CheckpointHandler(provider, MemoryPool<byte>.Shared, GlobalMemoryManager.Instance, 5, NullLogger.Instance);

            var page = new MockPagesFile(GlobalMemoryManager.Instance, 303);
            await handler.EnqueueFileAsync(page);
            await handler.FinishCheckpoint(null);

            Assert.Empty(handler.ModifiedFileIds_Test);
        }

        [Fact]
        public async Task TestModifiedSinceLastCheckpointResets()
        {
            var provider = new MemoryFileProvider();
            var handler = new CheckpointHandler(provider, MemoryPool<byte>.Shared, GlobalMemoryManager.Instance, 5, NullLogger.Instance);

            var page = new MockPagesFile(GlobalMemoryManager.Instance, 404);
            await handler.EnqueueFileAsync(page);

            Assert.True(handler.ModifiedSinceLastCheckpoint_Test, "Flag should fly true when modifications occur.");

            await handler.FinishCheckpoint(null);

            Assert.False(handler.ModifiedSinceLastCheckpoint_Test, "Flag should reset to false after a checkpoint successfully saves.");
        }

        private class MockLeakStorageProvider : MemoryFileProvider
        {
            public int DataPipesCompleted { get; private set; }
            public int CheckpointPipesCompleted { get; private set; }
            public int CheckpointFileCount { get; private set; }
            public int DataFileCount { get; private set; }

            public override async Task<PipeReader> ReadDataFileAsync(ulong fileId, int fileSize, System.Threading.CancellationToken cancellationToken = default)
            {
                var reader = await base.ReadDataFileAsync(fileId, fileSize, cancellationToken);
                DataFileCount++;
                return new TrackPipeReader(reader, () => DataPipesCompleted++);
            }

            public override async Task<PipeReader> ReadCheckpointFileAsync(CheckpointId checkpointVersion, System.Threading.CancellationToken cancellationToken = default)
            {
                var reader = await base.ReadCheckpointFileAsync(checkpointVersion, cancellationToken);
                CheckpointFileCount++;
                return new TrackPipeReader(reader, () => CheckpointPipesCompleted++);
            }
        }

        private class TrackPipeReader : PipeReader
        {
            private readonly PipeReader _inner;
            private readonly System.Action _onComplete;
            public TrackPipeReader(PipeReader inner, System.Action onComplete) { _inner = inner; _onComplete = onComplete; }
            public override void Complete(System.Exception? exception = null) { _onComplete(); _inner.Complete(exception); }
            public override void AdvanceTo(SequencePosition consumed) => _inner.AdvanceTo(consumed);
            public override void AdvanceTo(SequencePosition consumed, SequencePosition examined) => _inner.AdvanceTo(consumed, examined);
            public override void CancelPendingRead() => _inner.CancelPendingRead();
            public override ValueTask<ReadResult> ReadAsync(System.Threading.CancellationToken cancellationToken = default) => _inner.ReadAsync(cancellationToken);
            public override bool TryRead(out ReadResult result) => _inner.TryRead(out result);
        }

        [Fact]
        public async Task TestRecoverToClearsStalePageLocations()
        {
            var provider = new MemoryFileProvider();
            var allocator = GlobalMemoryManager.Instance;
            var pool = MemoryPool<byte>.Shared;
            var handler = new CheckpointHandler(provider, pool, allocator, 5, NullLogger.Instance);

            // Write page 501 and checkpoint (version 1)
            var page1 = new MockPagesFile(allocator, 501);
            await handler.EnqueueFileAsync(page1);
            await handler.FinishCheckpoint(null);

            // Write page 502 and checkpoint (version 2)
            var page2 = new MockPagesFile(allocator, 502);
            await handler.EnqueueFileAsync(page2);
            await handler.FinishCheckpoint(null);

            // Both pages should exist before recovery
            Assert.True(handler.PageFileLocations_Test.ContainsKey(501));
            Assert.True(handler.PageFileLocations_Test.ContainsKey(502));

            // Recover to version 1 — page 502 should no longer exist
            await handler.RecoverTo(1, default);

            Assert.True(handler.PageFileLocations_Test.ContainsKey(501),
                "Page 501 should exist after recovering to version 1");
            Assert.False(handler.PageFileLocations_Test.ContainsKey(502),
                "Page 502 should NOT exist after recovering to version 1 — stale entry leaked from version 2");
        }

        [Fact]
        public async Task TestPipelineReaderCompletes()
        {
            var provider = new MockLeakStorageProvider();
            var allocator = GlobalMemoryManager.Instance;
            var pool = MemoryPool<byte>.Shared;
            
            // Create some bundle files to trigger the recovery loop
            var writerHandler = new CheckpointHandler(provider, pool, allocator, 5, NullLogger.Instance);
            var merged = new MergedBlobFileWriter(pool, allocator);
            merged.StartAddingSequences(1);
            merged.AddSequence(1, 12, new ReadOnlySequence<byte>(new byte[10]));
            merged.FinishAddingSequences();
            merged.Finish();
            await writerHandler.FinishCheckpoint(merged);

            // Instantiate another to invoke recover
            var handler = new CheckpointHandler(provider, pool, allocator, 5, NullLogger.Instance);
            await handler.RecoverToLatest(default);

            Assert.True(provider.DataFileCount > 0);
            
            Assert.Equal(provider.CheckpointFileCount, provider.CheckpointPipesCompleted);
            Assert.Equal(provider.DataFileCount, provider.DataPipesCompleted);
        }
    }
}
