using FlowtideDotNet.Storage.Exceptions;
using FlowtideDotNet.Storage.Memory;
using FlowtideDotNet.Storage.Persistence;
using FlowtideDotNet.Storage.Persistence.Reservoir.Internal;
using FlowtideDotNet.Storage.Persistence.Reservoir.LocalDisk;
using FlowtideDotNet.Storage.Persistence.Reservoir.MemoryDisk;
using Microsoft.Extensions.Logging.Abstractions;
using System.Buffers;

namespace FlowtideDotNet.Storage.Tests.Reservoir
{
    public class ReservoirSessionTests
    {
        public ReservoirSessionTests()
        {
        }

        [Theory]
        [InlineData(false)]
        [InlineData(true)]
        [Trait("Category", "AdversarialReviewRegression")]
        public async Task CommittedDeletionRemainsUnreadableBeforeTheStorageCheckpoint(bool deserializePage)
        {
            var provider = new TestDataProvider();
            using var storage = new ReservoirPersistentStorage(new Persistence.Reservoir.ReservoirStorageOptions
            {
                FileProvider = provider
            });
            await storage.InitializeAsync(new StorageInitializationMetadata("committed_deletion", NullLoggerFactory.Instance, GlobalMemoryManager.Instance));
            using var session = storage.CreateSession();
            await session.Write(100, new SerializableObject(BitConverter.GetBytes(42)));
            await session.Commit();
            await storage.CheckpointAsync(new byte[] { 1 }, false);
            await session.Delete(100);
            await session.Commit();

            // Committed tombstones must hide previous persisted values.
            await Assert.ThrowsAsync<FlowtidePersistentStorageException>(async () =>
            {
                if (deserializePage)
                {
                    var page = await session.Read(100, new TestPageSerializer());
                    page.Return();
                }
                else
                {
                    await session.Read(100);
                }
            });
            await storage.CheckpointAsync(new byte[] { 2 }, false);
            await Assert.ThrowsAsync<FlowtidePersistentStorageException>(async () => await session.Read(100));
        }

        [Theory]
        [InlineData(false)]
        [InlineData(true)]
        [Trait("Category", "FullBranchReviewRegression")]
        public async Task CompactedPagesRemainDeletedAfterCheckpointAndRecovery(bool recoverSnapshot)
        {
            var provider = new TestDataProvider();
            using var storage = new ReservoirPersistentStorage(new Persistence.Reservoir.ReservoirStorageOptions
            {
                FileProvider = provider,
                MaxFileSize = 1024,
                CompactionFileSizeRatioThreshold = 0.9f,
                SnapshotCheckpointInterval = 1
            });
            await storage.InitializeAsync(new StorageInitializationMetadata("compacted_deletion", NullLoggerFactory.Instance, GlobalMemoryManager.Instance));
            using var session = storage.CreateSession();
            await storage.CheckpointAsync(new byte[] { 1 }, false);
            var payload = new byte[500];
            payload[0] = 42;
            await session.Write(100, new SerializableObject(payload));
            await session.Commit();
            await storage.CheckpointAsync(new byte[] { 2 }, false);
            Assert.Equal(payload, (await session.Read(100)).ToArray());
            await session.Delete(100);
            await session.Commit();
            await Assert.ThrowsAsync<FlowtidePersistentStorageException>(async () => await session.Read(100));

            await storage.CheckpointAsync(new byte[] { 3 }, false);
            if (recoverSnapshot)
            {
                await session.Commit();
                await storage.CheckpointAsync(new byte[] { 4 }, false);
                await storage.RecoverAsync(4);
            }

            // Compaction must not publish locations for deleted pages.
            await Assert.ThrowsAsync<FlowtidePersistentStorageException>(async () => await session.Read(100));
        }

        [Theory]
        [InlineData(false, false)]
        [InlineData(true, false)]
        [InlineData(false, true)]
        [InlineData(true, true)]
        [Trait("Category", "BundleDeletionRegression")]
        public async Task QueuedPagesRemainDeletedAfterCheckpointAndSnapshotRecovery(bool commitBeforeDelete, bool recoverSnapshot)
        {
            var provider = new TestDataProvider();
            using var storage = new ReservoirPersistentStorage(new Persistence.Reservoir.ReservoirStorageOptions
            {
                FileProvider = provider,
                MaxFileSize = 1024,
                CompactionFileSizeRatioThreshold = 0,
                SnapshotCheckpointInterval = 1
            });
            var metadata = new StorageInitializationMetadata("queued_deletion", NullLoggerFactory.Instance, GlobalMemoryManager.Instance);
            await storage.InitializeAsync(metadata);
            using var session = storage.CreateSession();
            await storage.CheckpointAsync(new byte[] { 1 }, false);
            await session.Write(100, new SerializableObject(BitConverter.GetBytes(42)));
            await session.Write(101, new SerializableObject(BitConverter.GetBytes(43)));
            await session.Commit();
            await storage.CheckpointAsync(new byte[] { 2 }, false);
            await session.Write(100, new SerializableObject(BitConverter.GetBytes(84)));
            if (commitBeforeDelete)
            {
                await session.Commit();
            }
            await session.Delete(100);
            await session.Commit();
            await Assert.ThrowsAsync<FlowtidePersistentStorageException>(async () => await session.Read(100));

            await storage.CheckpointAsync(new byte[] { 3 }, false);
            if (recoverSnapshot)
            {
                await session.Commit();
                await storage.CheckpointAsync(new byte[] { 4 }, false);
                // Fresh storage must replay the persisted snapshot.
                using var recoveredStorage = new ReservoirPersistentStorage(new Persistence.Reservoir.ReservoirStorageOptions
                {
                    FileProvider = provider
                });
                await recoveredStorage.InitializeAsync(metadata);
                using var recoveredSession = recoveredStorage.CreateSession();
                Assert.Equal(storage.CurrentVersion, recoveredStorage.CurrentVersion);
                Assert.Equal(new byte[] { 4 }, (await recoveredSession.Read(1)).ToArray());
                Assert.Equal(BitConverter.GetBytes(43), (await recoveredSession.Read(101)).ToArray());
                await Assert.ThrowsAsync<FlowtidePersistentStorageException>(async () => await recoveredSession.Read(100));
            }
            else
            {
                // Queued page writes cannot override later committed deletions.
                Assert.Equal(BitConverter.GetBytes(43), (await session.Read(101)).ToArray());
                await Assert.ThrowsAsync<FlowtidePersistentStorageException>(async () => await session.Read(100));
            }
        }

        [Theory]
        [InlineData(false)]
        [InlineData(true)]
        [Trait("Category", "BundleRecreationRegression")]
        public async Task RecreatingAnUpdatedPagePreservesUntouchedPagesAfterCleanupAndRecovery(bool recoverSnapshot)
        {
            var provider = new TestDataProvider();
            using var storage = new ReservoirPersistentStorage(new Persistence.Reservoir.ReservoirStorageOptions
            {
                FileProvider = provider,
                MaxFileSize = 1024,
                CompactionFileSizeRatioThreshold = 0,
                SnapshotCheckpointInterval = 1
            });
            var metadata = new StorageInitializationMetadata("recreated_page_cleanup", NullLoggerFactory.Instance, GlobalMemoryManager.Instance);
            await storage.InitializeAsync(metadata);
            using var session = storage.CreateSession();
            await storage.CheckpointAsync(new byte[] { 1 }, false);
            await session.Write(100, new SerializableObject(BitConverter.GetBytes(42)));
            await session.Write(101, new SerializableObject(BitConverter.GetBytes(43)));
            await session.Commit();
            await storage.CheckpointAsync(new byte[] { 2 }, false);
            await session.Write(100, new SerializableObject(BitConverter.GetBytes(84)));
            await session.Delete(100);
            await session.Write(100, new SerializableObject(BitConverter.GetBytes(126)));
            await session.Commit();
            await storage.CheckpointAsync(new byte[] { 3 }, false);
            Assert.Equal(BitConverter.GetBytes(126), (await session.Read(100)).ToArray());
            Assert.Equal(BitConverter.GetBytes(43), (await session.Read(101)).ToArray());

            // Advance checkpoints until obsolete files can be deleted.
            for (byte version = 4; version <= 6; version++)
            {
                await session.Commit();
                await storage.CheckpointAsync(new byte[] { version }, false);
            }
            await storage.CompactAsync(0, 0);

            if (recoverSnapshot)
            {
                // Fresh storage must replay the surviving checkpoint.
                using var recoveredStorage = new ReservoirPersistentStorage(new Persistence.Reservoir.ReservoirStorageOptions
                {
                    FileProvider = provider
                });
                await recoveredStorage.InitializeAsync(metadata);
                using var recoveredSession = recoveredStorage.CreateSession();
                Assert.Equal(storage.CurrentVersion, recoveredStorage.CurrentVersion);
                Assert.Equal(new byte[] { 6 }, (await recoveredSession.Read(1)).ToArray());
                Assert.Equal(BitConverter.GetBytes(126), (await recoveredSession.Read(100)).ToArray());
                Assert.Equal(BitConverter.GetBytes(43), (await recoveredSession.Read(101)).ToArray());
            }
            else
            {
                // Cleanup must preserve files containing untouched live pages.
                Assert.Equal(BitConverter.GetBytes(126), (await session.Read(100)).ToArray());
                Assert.Equal(BitConverter.GetBytes(43), (await session.Read(101)).ToArray());
            }
        }

        [Fact]
        public async Task TestReadYourDeletes()
        {
            var provider = new TestDataProvider();
            var persistentStorage = new ReservoirPersistentStorage(new Persistence.Reservoir.ReservoirStorageOptions() { FileProvider = provider });
            await persistentStorage.InitializeAsync(new StorageInitializationMetadata("a", NullLoggerFactory.Instance, GlobalMemoryManager.Instance));

            var session = persistentStorage.CreateSession();
            // Initial write and commit
            await session.Write(100, new SerializableObject(new byte[] { 1, 2, 3, 4 }));
            await session.Commit();
            await persistentStorage.CheckpointAsync(new byte[] { 1 }, false);

            // Now delete in a new transaction/session work
            await session.Delete(100);

            await Assert.ThrowsAsync<FlowtidePersistentStorageException>(async () =>
            {
                await session.Read(100);
            });
        }

        [Fact]
        public async Task TestLargeWriteFileRolling()
        {
            var provider = new TestDataProvider();
            var persistentStorage = new ReservoirPersistentStorage(new Persistence.Reservoir.ReservoirStorageOptions() 
            { 
                FileProvider = provider,
                MaxFileSize = 1024 * 1024 // 1MB to force rolling after ~100 keys
            });
            await persistentStorage.InitializeAsync(new StorageInitializationMetadata("a", NullLoggerFactory.Instance, GlobalMemoryManager.Instance));

            var session = persistentStorage.CreateSession();
            
            int keyCount = 1100;
            byte[] payload = new byte[10 * 1024]; // 10KB
            new Random().NextBytes(payload);

            int startOffset = 100;
            for (int i = 0; i < keyCount; i++)
            {
                await session.Write(i + startOffset, new SerializableObject(payload));
            }

            await session.Commit();
            await persistentStorage.CheckpointAsync(new byte[] { 1 }, false);

            // Recover and verify a few keys
            {
                var persistentStorage2 = new ReservoirPersistentStorage(new Persistence.Reservoir.ReservoirStorageOptions() { FileProvider = provider });
                await persistentStorage2.InitializeAsync(new StorageInitializationMetadata("a", NullLoggerFactory.Instance, GlobalMemoryManager.Instance));
                await persistentStorage2.RecoverAsync(persistentStorage.CurrentVersion - 1);
                var session2 = persistentStorage2.CreateSession();

                var data = await session2.Read(startOffset);
                Assert.Equal(payload, data.ToArray());

                var dataLast = await session2.Read(startOffset + keyCount - 1);
                Assert.Equal(payload, dataLast.ToArray());
            }
        }

        [Fact]
        public async Task TestConcurrentWriters()
        {
            var provider = new TestDataProvider();
            var persistentStorage = new ReservoirPersistentStorage(new Persistence.Reservoir.ReservoirStorageOptions() { FileProvider = provider });
            await persistentStorage.InitializeAsync(new StorageInitializationMetadata("a", NullLoggerFactory.Instance, GlobalMemoryManager.Instance));

            // Two sessions writing non-overlapping keys concurrently
            var session1 = persistentStorage.CreateSession();
            var session2 = persistentStorage.CreateSession();

            var task1 = Task.Run(async () =>
            {
                for (int i = 100; i < 200; i++)
                {
                    await session1.Write(i, new SerializableObject(new byte[] { 1 }));
                }
                await session1.Commit();
            });

            var task2 = Task.Run(async () =>
            {
                for (int i = 200; i < 300; i++)
                {
                    await session2.Write(i, new SerializableObject(new byte[] { 2 }));
                }
                await session2.Commit();
            });

            await Task.WhenAll(task1, task2);

            await persistentStorage.CheckpointAsync(new byte[] { 1 }, false);

            // Recover and verify
            {
                var persistentStorage2 = new ReservoirPersistentStorage(new Persistence.Reservoir.ReservoirStorageOptions() { FileProvider = provider });
                await persistentStorage2.InitializeAsync(new StorageInitializationMetadata("a", NullLoggerFactory.Instance, GlobalMemoryManager.Instance));
                await persistentStorage2.RecoverAsync(persistentStorage.CurrentVersion - 1);
                var session3 = persistentStorage2.CreateSession();

                var val1 = await session3.Read(150);
                Assert.Equal(new byte[] { 1 }, val1.ToArray());

                var val2 = await session3.Read(250);
                Assert.Equal(new byte[] { 2 }, val2.ToArray());
            }
        }

        /// <summary>
        /// Concurrent writes during file roll must not throw.
        /// </summary>
        [Fact]
        public async Task ConcurrentWritesDoNotThrowDuringFileRolling()
        {
            var provider = new TestDataProvider();
            var persistentStorage = new ReservoirPersistentStorage(new Persistence.Reservoir.ReservoirStorageOptions()
            {
                FileProvider = provider,
                MaxFileSize = 100
            });
            await persistentStorage.InitializeAsync(new StorageInitializationMetadata("roll_race", NullLoggerFactory.Instance, GlobalMemoryManager.Instance));

            var session = persistentStorage.CreateSession();
            var reservoirSession = Assert.IsType<ReservoirPersistentSession>(session);

            var rollEntered = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
            var secondWriteEntered = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);

            reservoirSession.FileRollHookForTests = () =>
            {
                rollEntered.TrySetResult(true);
                secondWriteEntered.Task.Wait();
            };

            var firstWrite = Task.Run(async () =>
            {
                await session.Write(1, new SerializableObject(new byte[150]));
            });

            await rollEntered.Task.WaitAsync(TimeSpan.FromSeconds(5));

            var secondWrite = Task.Run(async () =>
            {
                await session.Write(2, new SerializableObject(new byte[50]));
            });

            await Task.Delay(50);
            secondWriteEntered.TrySetResult(true);

            // Concurrent writes during file roll must not throw.
            await Task.WhenAll(firstWrite, secondWrite);
        }

        /// <summary>
        /// BlobFileWriter finish must truncate last segment end index.
        /// </summary>
        [Fact]
        public void BlobFileWriterFinishTruncatesLastSegmentEndIndex()
        {
            var writer = new BlobFileWriter(_ => { }, MemoryPool<byte>.Shared, GlobalMemoryManager.Instance);
            writer.Write(1, new SerializableObject(new byte[] { 1, 2, 3 }));
            writer.Finish();

            // BlobFileWriter finish must truncate last segment end index.
            Assert.Equal(writer.CurrentIndex, writer.CurrentSegment.End);
        }

        /// <summary>
        /// Temporary read must not return pooled memory directly.
        /// </summary>
        [Fact]
        public async Task TemporaryReadDoesNotReturnBufferFromDisposedFileWriter()
        {
            var provider = new TestDataProvider();
            var persistentStorage = new ReservoirPersistentStorage(new Persistence.Reservoir.ReservoirStorageOptions()
            {
                FileProvider = provider
            });
            await persistentStorage.InitializeAsync(new StorageInitializationMetadata("a", NullLoggerFactory.Instance, GlobalMemoryManager.Instance));

            var session1 = persistentStorage.CreateSession();
            var payload = new byte[] { 1, 2, 3, 4 };
            // Write payload and commit to temporary locations.
            await session1.Write(100, new SerializableObject(payload));
            await session1.Commit();

            var session2 = persistentStorage.CreateSession();
            // Read temporary location before storage checkpoint seals it.
            var readMemory = await session2.Read(100);

            // Checkpoint disposes temporary file writer and pooled buffers.
            await persistentStorage.CheckpointAsync(new byte[] { 1 }, false);

            // Returned memory must be an owned independent buffer.
            Assert.True(System.Runtime.InteropServices.MemoryMarshal.TryGetArray(readMemory, out var segment));
            Assert.Equal(payload.Length, segment.Array!.Length);
        }

        /// <summary>
        /// Storage checkpoint failure must reset taking checkpoint flag.
        /// </summary>
        [Fact]
        public async Task FailedStorageCheckpointResetsTakingCheckpointFlagForLaterCommits()
        {
            var provider = new TestDataProvider();
            var persistentStorage = new ReservoirPersistentStorage(new Persistence.Reservoir.ReservoirStorageOptions()
            {
                FileProvider = provider
            });
            await persistentStorage.InitializeAsync(new StorageInitializationMetadata("a", NullLoggerFactory.Instance, GlobalMemoryManager.Instance));

            var session1 = persistentStorage.CreateSession();
            await session1.Write(100, new SerializableObject(new byte[] { 1 }));
            await session1.Commit();

            provider.InjectWriteException(_ => new IOException("Disk failure"));
            // Injected failure during storage checkpoint leaves flag set.
            await Assert.ThrowsAsync<IOException>(async () =>
            {
                await persistentStorage.CheckpointAsync(new byte[] { 1 }, false);
            });

            provider.InjectWriteException(null);
            var session2 = persistentStorage.CreateSession();
            await session2.Write(200, new SerializableObject(new byte[] { 2 }));

            // Session commit must succeed after clearing injected fault.
            var ex = await Record.ExceptionAsync(async () => await session2.Commit());
            Assert.Null(ex);
        }

        [Theory]
        [InlineData(false)]
        [InlineData(true)]
        public async Task RecreatedPageRemainsReadableAfterItsFileRolls(bool deserializePage)
        {
            var provider = new TestDataProvider();
            using var persistentStorage = new ReservoirPersistentStorage(new Persistence.Reservoir.ReservoirStorageOptions()
            {
                FileProvider = provider,
                MaxFileSize = 1024
            });
            await persistentStorage.InitializeAsync(new StorageInitializationMetadata(
                "recreated_page_roll", NullLoggerFactory.Instance, GlobalMemoryManager.Instance));
            using var session = persistentStorage.CreateSession();
            await session.Write(100, new SerializableObject(BitConverter.GetBytes(1)));
            await session.Commit();
            await persistentStorage.CheckpointAsync(new byte[] { 1 }, false);
            await session.Delete(100);
            await session.Commit();

            provider.BlockWrites();
            try
            {
                var replacement = new byte[2048];
                BitConverter.GetBytes(2).CopyTo(replacement, 0);
                await session.Write(100, new SerializableObject(replacement));
                await session.Commit();
                Assert.Equal(2, BitConverter.ToInt32((await session.Read(100)).Span));
            }
            finally
            {
                provider.UnblockWrites();
            }

            // File completion must preserve the latest readable value.
            var deadline = DateTime.UtcNow.AddSeconds(10);
            while (persistentStorage.TemporaryLocationExists(100) && DateTime.UtcNow < deadline)
            {
                await Task.Delay(1);
            }
            Assert.False(persistentStorage.TemporaryLocationExists(100));
            if (deserializePage)
            {
                var page = await session.Read(100, new TestPageSerializer());
                try
                {
                    Assert.Equal(2, page.Value);
                }
                finally
                {
                    page.Return();
                }
            }
            else
            {
                Assert.Equal(2, BitConverter.ToInt32((await session.Read(100)).Span));
            }
        }

        /// <summary>
        /// Recreated page must be readable after prior commit.
        /// </summary>
        [Fact]
        public async Task RecreatedPageCanBeReadAfterPreviousCommitDeletedIt()
        {
            var provider = new TestDataProvider();
            var persistentStorage = new ReservoirPersistentStorage(new Persistence.Reservoir.ReservoirStorageOptions()
            {
                FileProvider = provider
            });
            await persistentStorage.InitializeAsync(new StorageInitializationMetadata("a", NullLoggerFactory.Instance, GlobalMemoryManager.Instance));

            var session1 = persistentStorage.CreateSession();
            var session2 = persistentStorage.CreateSession();
            // Initial page write and commit into persistent storage.
            await session1.Write(100, new SerializableObject(new byte[] { 1 }));
            await session1.Commit();

            // Delete page and commit deletion to persistent storage.
            await session1.Delete(100);
            await session1.Commit();

            // Recreate page with updated content from another session.
            await session2.Write(100, new SerializableObject(new byte[] { 2 }));
            await session2.Commit();
            await persistentStorage.CheckpointAsync(new byte[] { 1 }, false);

            // Recreated page must be readable after subsequent commit.
            var memory = await session1.Read(100);
            Assert.Equal(new byte[] { 2 }, memory.ToArray());
        }

        /// <summary>
        /// Session reset removes temporary locations allowing subsequent writes.
        /// </summary>
        [Fact]
        public async Task ReservoirSessionResetRemovesTemporaryLocationsAllowingSubsequentWrites()
        {
            var provider = new TestDataProvider();
            var persistentStorage = new ReservoirPersistentStorage(new Persistence.Reservoir.ReservoirStorageOptions()
            {
                FileProvider = provider
            });
            await persistentStorage.InitializeAsync(new StorageInitializationMetadata("a", NullLoggerFactory.Instance, GlobalMemoryManager.Instance));

            var session = (ReservoirPersistentSession)persistentStorage.CreateSession();

            // Write page to session without committing changes.
            await session.Write(100, new SerializableObject(new byte[] { 1 }));

            // Reset uncommitted session to discard written changes.
            session.Reset();

            // Rewriting discarded page must succeed after reset.
            var ex = await Record.ExceptionAsync(async () => await session.Write(100, new SerializableObject(new byte[] { 2 })));
            Assert.Null(ex);
        }

        [Fact]
        [Trait("Category", "SessionDisposeRegression")]
        public async Task DisposingUncommittedSessionRemovesItsTemporaryPageLocations()
        {
            var provider = new TestDataProvider();
            using var persistentStorage = new ReservoirPersistentStorage(new Persistence.Reservoir.ReservoirStorageOptions()
            {
                FileProvider = provider
            });
            await persistentStorage.InitializeAsync(new StorageInitializationMetadata("dispose_temp_leak", NullLoggerFactory.Instance, GlobalMemoryManager.Instance));

            using (var session1 = persistentStorage.CreateSession())
            {
                // Write page to session without committing changes.
                await session1.Write(100, new SerializableObject(new byte[] { 1 }));
            }

            using var session2 = persistentStorage.CreateSession();

            // Disposing an uncommitted session must purge temporary locations.
            var ex = await Record.ExceptionAsync(async () => await session2.Write(100, new SerializableObject(new byte[] { 2 })));
            Assert.Null(ex);
        }

        [Fact]
        [Trait("Category", "SessionResetLockRegression")]
        public async Task SessionResetBlocksUntilInFlightWriteCompletes()
        {
            var provider = new TestDataProvider();
            using var persistentStorage = new ReservoirPersistentStorage(new Persistence.Reservoir.ReservoirStorageOptions()
            {
                FileProvider = provider
            });
            await persistentStorage.InitializeAsync(new StorageInitializationMetadata("reset_lock_gate", NullLoggerFactory.Instance, GlobalMemoryManager.Instance));

            var session = (ReservoirPersistentSession)persistentStorage.CreateSession();
            var serializer = new TestPageSerializer();
            using var gate = new ManualResetEventSlim(false);
            serializer.ArmSerializeGate(gate, 1);

            var writeTask = Task.Run(async () => await session.Write(100, new SerializableObject(new TestPage(1), serializer)));
            await serializer.SerializeEntered;

            var resetTask = Task.Run(() => session.Reset());
            await Task.Delay(100);

            // Reset must block until write lock is released.
            Assert.False(resetTask.IsCompleted);

            gate.Set();
            await writeTask;
            await resetTask;
        }

        [Fact]
        [Trait("Category", "SegmentSizingRegression")]
        public async Task SerializedPageWritesPreserveSingleSegmentForNonPowerOfTwoSizes()
        {
            var provider = new TestDataProvider();
            using var persistentStorage = new ReservoirPersistentStorage(new Persistence.Reservoir.ReservoirStorageOptions()
            {
                FileProvider = provider
            });
            await persistentStorage.InitializeAsync(new StorageInitializationMetadata("segment_size_gate", NullLoggerFactory.Instance, GlobalMemoryManager.Instance));

            using var session = persistentStorage.CreateSession();
            await session.Write(100, new SerializableObject(new byte[20000]));

            Assert.True(persistentStorage.TryGetTemporaryLocation(100, out var location));

            // Large serialized writes must stay single segment.
            Assert.True(location.data.IsSingleSegment);
        }
    }
}
