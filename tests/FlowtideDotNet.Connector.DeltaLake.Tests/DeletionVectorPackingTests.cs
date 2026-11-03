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

using FlowtideDotNet.AcceptanceTests.Entities;
using FlowtideDotNet.AcceptanceTests.Internal;
using FlowtideDotNet.Connector.DeltaLake.Internal;
using FlowtideDotNet.Connector.DeltaLake.Internal.Catalog;
using FlowtideDotNet.Connector.DeltaLake.Internal.Delta;
using FlowtideDotNet.Connector.DeltaLake.Internal.Delta.Actions;
using FlowtideDotNet.Connector.DeltaLake.Internal.Delta.DeletionVectors;
using FlowtideDotNet.Connector.DeltaLake.Internal.Delta.DeletionVectors.RoaringBitmap;
using FlowtideDotNet.Connector.DeltaLake.Internal.Deletes;
using Stowage;
using System.Buffers.Binary;
using System.Collections.Concurrent;
using System.IO.Hashing;
using System.Text.Json;
using static FlowtideDotNet.Connector.DeltaLake.Tests.DeltaTestKit;
using Delta = FlowtideDotNet.Connector.DeltaLake.Internal.Delta.Schema.Types;

namespace FlowtideDotNet.Connector.DeltaLake.Tests
{
    public class DeletionVectorPackingTests
    {
        private const long Rows = 50_000;

        // Differ per file, so a swapped descriptor shows
        private static long[] Positions(int file, int count)
        {
            return Enumerable.Range(0, count).Select(i => i * 7L + file).ToArray();
        }

        // Checks each file by the protocol's layout, not our reader
        private static async Task AssertContainers(IFileStorage storage, string table, IEnumerable<(DeletionVector Vector, long[]? Positions)> members)
        {
            foreach (var group in members.GroupBy(x => x.Vector.AbsolutePath))
            {
                AssertContainer(await ReadBytes(storage, $"{table}/{group.Key}"), group.OrderBy(x => x.Vector.Offset));
            }
        }

        private static void AssertContainer(byte[] file, IEnumerable<(DeletionVector Vector, long[]? Positions)> members)
        {
            Assert.Equal(1, file[0]);
            long next = 1;
            foreach (var (vector, positions) in members)
            {
                Assert.Equal("u", vector.StorageType);
                Assert.Equal(next, vector.Offset);
                var start = (int)next;
                var size = BinaryPrimitives.ReadInt32BigEndian(file.AsSpan(start));
                Assert.Equal(vector.SizeInBytes, size);
                var data = file.AsSpan(start + 4, size);
                Assert.Equal(1681511377, BinaryPrimitives.ReadInt32LittleEndian(data));
                Assert.Equal(Crc32.HashToUInt32(data), BinaryPrimitives.ReadUInt32BigEndian(file.AsSpan(start + 4 + size)));
                using var bitmap = new MemoryStream(data.Slice(4).ToArray());
                using var reader = new BinaryReader(bitmap);
                var decoded = RoaringBitmapArray.Deserialize(reader);
                if (positions != null)
                {
                    Assert.Equal(positions, decoded);
                }
                Assert.Equal(decoded.Count(), decoded.Cardinality);
                Assert.Equal(decoded.Cardinality, vector.Cardinality);
                next = start + 4 + size + 4;
            }
            Assert.Equal(file.Length, next);
        }

        // One delete output call, a vector never opens parquet
        private sealed class Walk
        {
            private readonly Delta.StructType _table;

            public Walk(Delta.StructType? table = null)
            {
                _table = table ?? new Delta.StructType(new[] { new Delta.StructField("a", new Delta.LongType(), true, new Dictionary<string, object>()) });
                var options = new JsonSerializerOptions();
                options.Converters.Add(new Internal.Delta.Stats.DeltaStatisticsConverter(_table));
                Overlay = new CatalogOverlay(1, PruningLayout.Create(_table, Array.Empty<string>(), 8), options, null);
            }

            public HookFileStorage Storage { get; } = new HookFileStorage(Files.Of.InternalMemory("./" + ScratchOwners.ShortId()));

            public List<DeltaAction> Actions { get; } = new List<DeltaAction>();

            public List<string> Created { get; } = new List<string>();

            public CatalogOverlay Overlay { get; }

            public List<DeltaAddAction> Adds => Actions.Where(x => x.Add != null).Select(x => x.Add!).ToList();

            public List<string> VectorWrites => Storage.Requests.Where(x => x.StartsWith("OpenWrite ") && x.Contains("deletion_vector_")).ToList();

            public async Task Run(IReadOnlyList<long[]> vectors, long rows = Rows, int? fileBytes = null, IReadOnlyDictionary<int, long>? fileRows = null)
            {
                var deletes = new Dictionary<int, ModifiableDeleteVector>();
                var files = new Dictionary<int, DeltaFileRecord>();
                var footers = new Dictionary<int, long>();
                for (int i = 0; i < vectors.Count; i++)
                {
                    var vector = new ModifiableDeleteVector(EmptyDeleteVector.Instance);
                    foreach (var position in vectors[i])
                    {
                        vector.Add(position);
                    }
                    deletes[i] = vector;
                    var count = fileRows != null && fileRows.TryGetValue(i, out var known) ? known : rows;
                    files[i] = new DeltaFileRecord() { Path = $"data{i}.parquet", Size = 1, PartitionValues = new Dictionary<string, string>(), Statistics = $"{{\"numRecords\":{count}}}" };
                    footers[i] = count;
                }
                var output = new DeleteOutput(Storage, "/t", "t", _table, false, true, null, _ => false, _ => Task.CompletedTask, fileBytes == null ? null : new DeleteLimits() { DeletionVectorFileBytes = fileBytes.Value });
                await output.WriteAsync(deletes, files, footers, Actions, 1000, Overlay, Created);
            }

            public IEnumerable<(DeletionVector, long[]?)> Members(IReadOnlyList<long[]> vectors)
            {
                return Adds.Select((add, i) => (add.DeletionVector!, (long[]?)vectors[i]));
            }

            // Each owned file written once, each holding an add
            public async Task AssertFiles(IReadOnlyList<long[]> vectors, int count)
            {
                Assert.Equal(count, Created.Count);
                Assert.Equal(count, Created.Distinct().Count());
                Assert.Equal(Created.Select(x => $"OpenWrite /t/{x}"), VectorWrites);
                Assert.Equal(Created.OrderBy(x => x, StringComparer.Ordinal), Adds.Select(x => x.DeletionVector!.AbsolutePath!).Distinct().OrderBy(x => x, StringComparer.Ordinal));
                Assert.Equal(vectors.Count, Adds.Count);
                await AssertContainers(Storage, "/t", Members(vectors));
            }
        }

        [Fact]
        public async Task ACommitsVectorsShareOneFile()
        {
            var vectors = new[] { Positions(0, 3), Positions(1, 700), Positions(2, 40) };
            var walk = new Walk();

            await walk.Run(vectors);

            Assert.Single(walk.VectorWrites);
            var name = Assert.Single(walk.Created);
            Assert.Equal($"OpenWrite /t/{name}", walk.VectorWrites[0]);
            Assert.Equal(new[] { "remove data0.parquet", "add data0.parquet", "remove data1.parquet", "add data1.parquet", "remove data2.parquet", "add data2.parquet" },
                walk.Actions.Select(x => x.Remove != null ? $"remove {x.Remove.Path}" : $"add {x.Add!.Path}"));
            Assert.All(walk.Adds, x => Assert.Equal(name, x.DeletionVector!.AbsolutePath));
            await AssertContainers(walk.Storage, "/t", walk.Members(vectors));
            // The catalog gets the same descriptors the commit holds
            Assert.Equal(walk.Adds.Select(x => x.DeletionVector!.UniqueId), walk.Overlay.Updates.Select(x => x.Record.DeletionVector!.UniqueId));
            Assert.Equal(walk.Adds.Select(x => x.DeletionVector!.Cardinality), walk.Overlay.Updates.Select(x => x.Record.DeletionVector!.Cardinality));
        }

        [Fact]
        public async Task OneVectorKeepsItsBytes()
        {
            var vectors = new[] { new long[] { 3, 7, 70_000 } };
            var walk = new Walk();

            await walk.Run(vectors, rows: 1_000_000);

            Assert.Single(walk.VectorWrites);
            var name = Assert.Single(walk.Created);
            Assert.Equal(1, walk.Adds[0].DeletionVector!.Offset);
            await AssertContainers(walk.Storage, "/t", walk.Members(vectors));
            Assert.Equal("010000002ED1D339640100000000000000000000003A300000020000000000010001000000180000001C0000000300070070114E6F5D37", Convert.ToHexString(await ReadBytes(walk.Storage, $"/t/{name}")));
        }

        [Fact]
        public async Task ARemovedFileWritesNoVectorFile()
        {
            var walk = new Walk();

            await walk.Run(new[] { new long[] { 0 } }, rows: 1);

            Assert.Empty(walk.VectorWrites);
            Assert.Empty(walk.Created);
            Assert.Empty(walk.Adds);
        }

        [Fact]
        public async Task AWalkThatFailsAfterVectorsOwnsNoFile()
        {
            var walk = new Walk();

            await Assert.ThrowsAsync<InvalidDataException>(() => walk.Run(new[] { Positions(0, 3), Positions(1, 3), new long[] { Rows } }));

            Assert.Empty(walk.Created);
            Assert.Empty(walk.Storage.Requests);
        }

        [Fact]
        public async Task AnExactFitStaysInTheFile()
        {
            var vectors = Enumerable.Range(0, 4).Select(x => Positions(x, 16)).ToArray();
            var single = new Walk();
            await single.Run(vectors.Take(1).ToArray());
            var entry = single.Adds[0].DeletionVector!.SizeInBytes + DeletionVectorWriter.FrameOverhead;

            var fits = new Walk();
            await fits.Run(vectors, fileBytes: 1 + 2 * entry);
            await fits.AssertFiles(vectors, 2);

            var over = new Walk();
            await over.Run(vectors, fileBytes: 2 * entry);
            await over.AssertFiles(vectors, 4);
        }

        [Fact]
        public async Task ALargeVectorIsAloneInItsFile()
        {
            var vectors = "ssBsBBss".Select((kind, i) => kind == 's' ? Positions(i, 16) : Mature(i)).ToArray();
            var walk = new Walk();

            await walk.Run(vectors, fileBytes: 1_000);

            await walk.AssertFiles(vectors, 6);
        }

        [Fact]
        public async Task AFileNearTheDefaultLimitStaysValid()
        {
            Assert.Equal(1_048_576, DeleteLimits.Default.DeletionVectorFileBytes);
            var vectors = Enumerable.Range(0, 128).Select(Mature).ToArray();
            var walk = new Walk();

            await walk.Run(vectors);

            await walk.AssertFiles(vectors, 2);
            Assert.Equal(1_045_465, (await ReadBytes(walk.Storage, $"/t/{walk.Created[0]}")).Length);
            // The last vector of the full file, through our reader
            Assert.Equal(vectors[126], await new DeletionVectorReader(walk.Storage, "/t").Read(walk.Adds[126].DeletionVector!));
        }

        // Under a tenth of the rows, a pattern per file
        private static long[] Mature(int file)
        {
            return Enumerable.Range(0, 4_950).Select(i => i * 10L + file % 10).ToArray();
        }

        [Fact]
        public async Task MixedOutcomesShareOneFile()
        {
            // Vector, removal, rewrite, inexact survivor, vector
            var vectors = new[] { new long[] { 1 }, new long[] { 0 }, new long[] { 0, 1 }, new long[] { 0, 1 }, new long[] { 2 } };
            var walk = new Walk(DeleteOutputTests.Timestamps);
            using (var write = await walk.Storage.OpenWrite("/t/data2.parquet"))
            {
                await write.WriteAsync(DeleteOutputTests.InexactFile("nanos", whole: true));
            }
            using (var write = await walk.Storage.OpenWrite("/t/data3.parquet"))
            {
                await write.WriteAsync(DeleteOutputTests.InexactFile("nanos"));
            }
            walk.Storage.ClearRequests();

            await walk.Run(vectors, rows: 1_000, fileRows: new Dictionary<int, long>() { [1] = 1, [2] = 10, [3] = 10 });

            Assert.Equal(new[] { 1, 2 }, walk.Overlay.Removes);
            Assert.Equal(new[] { 0, 3, 4 }, walk.Overlay.Updates.Select(x => x.Id));
            var name = Assert.Single(walk.Created);
            Assert.Single(walk.VectorWrites);
            Assert.All(walk.Adds, x => Assert.Equal(name, x.DeletionVector!.AbsolutePath));
            Assert.Equal(new[] { "data0.parquet", "data3.parquet", "data4.parquet" }, walk.Adds.Select(x => x.Path));
            await AssertContainers(walk.Storage, "/t", new[] { 0, 3, 4 }.Select((x, i) => (walk.Adds[i].DeletionVector!, (long[]?)vectors[x])));
        }

        [Fact]
        public async Task AFailedOpenStaysOwned()
        {
            var walk = new Walk();
            walk.Storage.Before = (verb, path) => verb == "OpenWrite" ? throw new IOException("Simulated open failure") : Task.CompletedTask;

            await Assert.ThrowsAsync<IOException>(() => walk.Run(new[] { Positions(0, 3), Positions(1, 3) }));

            Assert.Single(walk.Created);
        }

        [Fact]
        public async Task ATornWriteStaysOwned()
        {
            var walk = new Walk();
            walk.Storage.TearAfterBytes = 8;
            walk.Storage.TearWriteOf = ".bin";

            await Assert.ThrowsAsync<CrashException>(() => walk.Run(new[] { Positions(0, 3), Positions(1, 3) }));

            Assert.Equal(1, walk.Storage.Tears);
            Assert.Single(walk.Created);
        }

        [Theory]
        [InlineData(false)]
        [InlineData(true)]
        public async Task AFailedWriteStillClosesTheStream(bool atFlush)
        {
            var walk = new Walk();
            walk.Storage.FaultAtFlush = atFlush;
            walk.Storage.FaultWriteOf = ".bin";

            await Assert.ThrowsAsync<CrashException>(() => walk.Run(new[] { Positions(0, 3), Positions(1, 3) }));

            var name = Assert.Single(walk.Created);
            Assert.Contains($"Closed /t/{name}", walk.Storage.Requests);
        }

        [Fact]
        public async Task AFailedCloseFailsTheWalk()
        {
            var walk = new Walk();
            walk.Storage.DeferPublication = _ => Task.FromException(new CrashException());

            await Assert.ThrowsAsync<CrashException>(() => walk.Run(new[] { Positions(0, 3), Positions(1, 3) }));

            Assert.Single(walk.Created);
        }

        [Fact]
        public async Task TheWalkWaitsForTheClose()
        {
            var walk = new Walk();
            var gate = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            var parked = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            walk.Storage.DeferPublication = _ => gate.Task;
            walk.Storage.PublicationParked = _ => parked.TrySetResult();

            var run = Task.Run(() => walk.Run(new[] { Positions(0, 3), Positions(1, 3) }));
            try
            {
                await parked.Task.WaitAsync(TimeSpan.FromMinutes(1));
                Assert.False(run.IsCompleted);
            }
            finally
            {
                gate.SetResult();
            }
            await run.WaitAsync(TimeSpan.FromMinutes(1));
        }

        [Fact]
        public async Task AWalkThatFailsAfterARollOwnsOnlyTheWrittenFile()
        {
            var walk = new Walk();

            await Assert.ThrowsAsync<InvalidDataException>(() => walk.Run(new[] { Positions(0, 3), Positions(1, 3), new long[] { Rows } }, fileBytes: 1));

            var name = Assert.Single(walk.Created);
            Assert.Equal(new[] { $"OpenWrite /t/{name}" }, walk.VectorWrites);
        }

        // About 50 rows per file, so 0, 100, 200 differ
        private static async Task<(OwnedHookFileStorage Storage, DeltaLakeSinkStream Stream)> Start(string name, int vectorFileBytes = 1_048_576, Action<DeltaLakeOptions>? configure = null)
        {
            var storage = new OwnedHookFileStorage(Files.Of.InternalMemory($"./{name}"));
            var stream = new DeltaLakeSinkStream(name, storage, Options(vectorFileBytes, configure));
            stream.WaitForUpdateDoesNotRequireDataChange();
            AddUsers(stream, 600);
            await stream.StartStream(UserInsert);
            await WaitForVersion(storage, "test", stream, 0);
            await SettlePublications(stream);
            return (storage, stream);
        }

        private static Action<DeltaLakeOptions> Options(int vectorFileBytes, Action<DeltaLakeOptions>? configure = null)
        {
            return options =>
            {
                options.CheckpointInterval = 0;
                options.MaxFileSizeBytes = 1_500;
                options.DeleteLimitsOverride = new DeleteLimits() { DeletionVectorFileBytes = vectorFileBytes };
                configure?.Invoke(options);
            };
        }

        private static bool IsVector(IOPath path) => path.Full.EndsWith(".bin");

        private static IEnumerable<DeltaAddAction> VectorAdds(IEnumerable<DeltaAction> actions)
        {
            return actions.Where(x => x.Add?.DeletionVector != null).Select(x => x.Add!);
        }

        private static async Task WaitForFailure(FlowtideTestStream stream, int failures)
        {
            var deadline = DateTime.UtcNow + TimeSpan.FromMinutes(2);
            while (stream.FailureNotificationCount == failures)
            {
                Assert.True(DateTime.UtcNow < deadline, "The stream did not fail in time");
                // A surfaced error is the stream's failure, wrapped
                if (await Record.ExceptionAsync(() => stream.SchedulerTick()) != null)
                {
                    return;
                }
                await Task.Delay(50);
            }
        }

        private static void AddUsers(FlowtideTestStream stream, int count)
        {
            for (int i = 0; i < count; i++)
            {
                stream.AddOrUpdateUser(Fixed(i));
            }
        }

        private static User Fixed(int key)
        {
            return new User() { UserKey = key, FirstName = $"first name of {key * 7919}", LastName = "Last" };
        }

        private static List<User> Users(params int[] keys)
        {
            return keys.Select(Fixed).ToList();
        }

        [Fact]
        public async Task ALaterDeleteMergesFromAnOffset()
        {
            var (storage, stream) = await Start(nameof(ALaterDeleteMergesFromAnOffset));
            await using var _ = stream;

            stream.DeleteUsers(Users(0, 100, 200));
            await WaitForVersion(storage, "test", stream, 1);
            var first = (await ReadCommitActions(storage, "test", 1)).Where(x => x.Add?.DeletionVector != null).Select(x => x.Add!).ToList();
            Assert.Equal(3, first.Count);
            var name = first[0].DeletionVector!.AbsolutePath;
            Assert.All(first, x => Assert.Equal(name, x.DeletionVector!.AbsolutePath));
            var packed = first[1];
            Assert.True(packed.DeletionVector!.Offset > 1);
            Assert.Equal(1, packed.DeletionVector.Cardinality);

            // A later delete from the file past the first vector
            var reread = await new DeletionVectorReader(storage, "/test").Read(packed.DeletionVector);
            Assert.Single(reread);
            stream.DeleteUsers(Users(101));
            await WaitForVersion(storage, "test", stream, 2);

            var second = await ReadCommitActions(storage, "test", 2);
            var removed = second.Single(x => x.Remove != null).Remove!;
            Assert.Equal(packed.Path, removed.Path);
            Assert.Equal(packed.DeletionVector!.UniqueId, removed.DeletionVector!.UniqueId);
            var moved = second.Single(x => x.Add != null).Add!;
            Assert.NotEqual(name, moved.DeletionVector!.AbsolutePath);
            Assert.Equal(1, moved.DeletionVector!.Offset);
            Assert.Equal(2, moved.DeletionVector.Cardinality);
            var merged = await new DeletionVectorReader(storage, "/test").Read(moved.DeletionVector);
            Assert.Equal(new[] { reread.Single(), reread.Single() + 1 }, merged);
            Assert.True(stream.Sinks[0].DeleteStatisticsForTests!.VectorsRead > 0);
        }

        [Fact]
        public async Task ACommitNamesItsFileOnceInTheHeader()
        {
            var (storage, stream) = await Start(nameof(ACommitNamesItsFileOnceInTheHeader));
            await using var _ = stream;

            stream.DeleteUsers(Users(0, 100, 200));
            await WaitForVersion(storage, "test", stream, 1);

            var adds = VectorAdds(await ReadCommitActions(storage, "test", 1)).ToList();
            await AssertContainers(storage, "/test", adds.Select(x => (x.DeletionVector!, (long[]?)null)));
            var created = new List<string>();
            await CommitHeaderReader.ReadAsync(storage.Inner, CommitPath("test", 1), HeaderMode.Strict, created.Add);
            Assert.Equal(new[] { adds[0].DeletionVector!.AbsolutePath }, created.Where(x => x.EndsWith(".bin")));
        }

        // Before its stage it deletes its files, the retry publishes
        [Theory]
        [InlineData("open")]
        [InlineData("tear")]
        [InlineData("close")]
        [InlineData("write")]
        [InlineData("flush")]
        [InlineData("changedata")]
        public async Task AFailedAttemptDeletesItsFile(string point)
        {
            var testName = nameof(AFailedAttemptDeletesItsFile) + point;
            var (storage, stream) = await Start(testName, configure: options => options.WriteChangeDataOnNewTables = point == "changedata");
            await using var _ = stream;
            var opened = new ConcurrentQueue<string>();
            var armed = 1;
            storage.Before = (verb, path) =>
            {
                if (verb == "OpenWrite" && IsVector(path))
                {
                    opened.Enqueue(path.Full);
                }
                if (verb == "OpenWrite" && (point == "open" ? IsVector(path) : point == "changedata" && path.Full.Contains("/_change_data/")) && Interlocked.Exchange(ref armed, 0) == 1)
                {
                    throw new CrashException();
                }
                return Task.CompletedTask;
            };
            if (point == "tear")
            {
                storage.TearAfterBytes = 8;
                storage.TearWriteOf = ".bin";
            }
            if (point == "close")
            {
                storage.DeferPublication = path => IsVector(path) && Interlocked.Exchange(ref armed, 0) == 1 ? Task.FromException(new CrashException()) : null;
            }
            if (point is "write" or "flush")
            {
                storage.FaultAtFlush = point == "flush";
                storage.FaultWriteOf = ".bin";
            }
            var failures = stream.FailureNotificationCount;

            stream.DeleteUsers(Users(0, 100, 200));
            await WaitForFailure(stream, failures);

            Assert.True(opened.TryPeek(out var failed));
            Assert.Contains($"Rm {failed}", storage.Requests);
            Assert.False(await storage.Inner.Exists(failed!));
            if (point == "tear")
            {
                Assert.Equal(1, storage.Tears);
            }
            if (point is "write" or "flush")
            {
                // Closed before the delete, a later close could publish it again
                var requests = storage.Requests.ToList();
                Assert.InRange(requests.IndexOf($"Closed {failed}"), 0, requests.IndexOf($"Rm {failed}") - 1);
            }

            await WaitForVersion(storage, "test", stream, 1);
            var adds = VectorAdds(await ReadCommitActions(storage, "test", 1)).ToList();
            Assert.Equal(3, adds.Count);
            Assert.DoesNotContain(adds, x => failed!.EndsWith(x.DeletionVector!.AbsolutePath!));
            await AssertContainers(storage, "/test", adds.Select(x => (x.DeletionVector!, (long[]?)null)));
        }

        // Each attempt fails at the rewrite, after the vectors
        private static Func<string, IOPath, Task> FailRewrites(ConcurrentQueue<string> opened)
        {
            return (verb, path) =>
            {
                if (verb == "OpenWrite" && IsVector(path))
                {
                    opened.Enqueue(path.Full);
                }
                if (verb == "OpenWrite" && path.Full.Contains("/part-"))
                {
                    throw new IOException("Simulated rewrite failure");
                }
                return Task.CompletedTask;
            };
        }

        private static List<User> SomeThenALateFile()
        {
            return Users(new[] { 0, 100, 200 }.Concat(Enumerable.Range(560, 40)).ToArray());
        }

        [Fact]
        public async Task AFailedRewriteWritesNoVectorFile()
        {
            var (storage, stream) = await Start(nameof(AFailedRewriteWritesNoVectorFile));
            await using var _ = stream;
            var opened = new ConcurrentQueue<string>();
            storage.Before = FailRewrites(opened);
            storage.ClearRequests();
            var failures = stream.FailureNotificationCount;

            stream.DeleteUsers(SomeThenALateFile());
            await WaitForFailure(stream, failures);

            Assert.Contains(storage.Requests, x => x.StartsWith("OpenWrite ") && x.Contains("/part-"));
            Assert.DoesNotContain(storage.Requests, x => x.Contains("deletion_vector_"));
            storage.Before = null;
        }

        [Fact]
        public async Task AFailedRewriteAfterARollDeletesTheWrittenFiles()
        {
            var (storage, stream) = await Start(nameof(AFailedRewriteAfterARollDeletesTheWrittenFiles), vectorFileBytes: 1);
            await using var _ = stream;
            var opened = new ConcurrentQueue<string>();
            storage.Before = FailRewrites(opened);
            var failures = stream.FailureNotificationCount;

            stream.DeleteUsers(SomeThenALateFile());
            await WaitForFailure(stream, failures);

            // The third vector is still buffered when the walk fails
            var attempt = opened.Take(2).ToList();
            Assert.Equal(2, attempt.Count);
            foreach (var name in attempt)
            {
                Assert.Contains($"Rm {name}", storage.Requests);
                Assert.False(await storage.Inner.Exists(name));
            }
            storage.Before = null;
        }

        [Fact]
        public async Task ATornSecondFileDeletesBoth()
        {
            var (storage, stream) = await Start(nameof(ATornSecondFileDeletesBoth), vectorFileBytes: 1);
            await using var _ = stream;
            var opened = new ConcurrentQueue<string>();
            storage.Before = (verb, path) =>
            {
                if (verb == "OpenWrite" && IsVector(path))
                {
                    opened.Enqueue(path.Full);
                    if (opened.Count == 2)
                    {
                        storage.TearAfterBytes = 8;
                        storage.TearWriteOf = path.Full.Substring(path.Full.LastIndexOf('/') + 1);
                    }
                }
                return Task.CompletedTask;
            };
            var failures = stream.FailureNotificationCount;

            stream.DeleteUsers(Users(0, 100, 200));
            await WaitForFailure(stream, failures);

            Assert.Equal(1, storage.Tears);
            foreach (var name in opened.Take(2))
            {
                Assert.Contains($"Rm {name}", storage.Requests);
                Assert.False(await storage.Inner.Exists(name));
            }
        }

        // After the flush only the orphan sweep reaches a file
        [Theory]
        [InlineData(1_048_576, 1)]
        [InlineData(1, 3)]
        public async Task AFailedStageKeepsItsFiles(int vectorFileBytes, int files)
        {
            var testName = nameof(AFailedStageKeepsItsFiles) + files;
            var (storage, stream) = await Start(testName, vectorFileBytes);
            await using var _ = stream;
            var opened = new ConcurrentQueue<string>();
            var failed = 0;
            List<string>? attempt = null;
            storage.Before = (verb, path) =>
            {
                if (verb == "OpenWrite" && IsVector(path))
                {
                    opened.Enqueue(path.Full);
                }
                if (verb == "OpenWrite" && path.Full.EndsWith(".tmp") && Interlocked.Exchange(ref failed, 1) == 0)
                {
                    attempt = opened.ToList();
                    throw new IOException("Simulated stage failure");
                }
                return Task.CompletedTask;
            };
            storage.ClearRequests();
            var failures = stream.FailureNotificationCount;

            stream.DeleteUsers(Users(0, 100, 200));
            await WaitForFailure(stream, failures);

            Assert.NotNull(attempt);
            Assert.Equal(files, attempt!.Count);
            Assert.DoesNotContain(storage.Requests, x => x.StartsWith("Rm ") && x.Contains("deletion_vector_"));
            foreach (var name in attempt)
            {
                Assert.True(await storage.Inner.Exists(name));
            }
        }

        [Fact]
        public async Task TheStageWaitsForTheFileToClose()
        {
            var (storage, stream) = await Start(nameof(TheStageWaitsForTheFileToClose));
            await using var _ = stream;
            var gate = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            var parked = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            storage.DeferPublication = path => IsVector(path) ? gate.Task : null;
            storage.PublicationParked = _ => parked.TrySetResult();
            storage.ClearRequests();

            stream.DeleteUsers(Users(0, 100, 200));
            var deadline = DateTime.UtcNow + TimeSpan.FromMinutes(2);
            try
            {
                while (!parked.Task.IsCompleted)
                {
                    Assert.True(DateTime.UtcNow < deadline, "The file's close did not start in time");
                    await stream.SchedulerTick();
                    await Task.Delay(50);
                }
                Assert.DoesNotContain(storage.Requests, x => x.EndsWith(".tmp"));
            }
            finally
            {
                gate.SetResult();
            }
            await WaitForVersion(storage, "test", stream, 1);
        }

        [Fact]
        public async Task EveryVectorFileIsWrittenInsideAGeneration()
        {
            DeltaLakeSink? sink = null;
            var (storage, stream) = await Start(nameof(EveryVectorFileIsWrittenInsideAGeneration), vectorFileBytes: 1, configure: options => options.SinkCreatedForTests = created => sink = created);
            await using var _ = stream;
            var outside = new ConcurrentQueue<string>();
            var writes = 0;
            void Check(string when, IOPath path)
            {
                var producers = sink?.ProducersForTests;
                if (producers == null || producers.Entry.Live(producers.Entry.Generation) == 0)
                {
                    outside.Enqueue($"{when} {path.Full}");
                }
            }
            storage.Before = (verb, path) =>
            {
                if (verb == "OpenWrite" && IsVector(path))
                {
                    Interlocked.Increment(ref writes);
                    Check("open", path);
                }
                return Task.CompletedTask;
            };
            // A completed deferral still parks the disposal
            storage.DeferPublication = path => IsVector(path) ? Task.CompletedTask : null;
            storage.PublicationParked = path => Check("dispose", path);

            stream.DeleteUsers(Users(0, 100, 200));
            await WaitForVersion(storage, "test", stream, 1);

            Assert.Empty(outside);
            Assert.Equal(3, writes);
        }

        [Theory]
        [InlineData(true)]
        [InlineData(false)]
        public async Task ASourceReadsEveryVectorOfAFile(bool oneVersionPerCheckpoint)
        {
            var testName = nameof(ASourceReadsEveryVectorOfAFile) + oneVersionPerCheckpoint;
            var (storage, stream) = await Start(testName);
            await using var _ = stream;
            stream.DeleteUsers(Users(0, 100, 200));
            await WaitForVersion(storage, "test", stream, 1);
            var expected = (int[] deleted) => Enumerable.Range(0, 600).Except(deleted).Select(x => new { userkey = x, name = Fixed(x).FirstName });

            await using var source = new DeltaLakeTestStream(testName + "Source", new HookFileStorage(storage.Inner), oneVersionPerCheckpoint, recordPublished: true);
            await source.StartStream("INSERT INTO output SELECT userkey, name FROM test");

            var next = 1;
            if (oneVersionPerCheckpoint)
            {
                await source.AssertPublishedEqual(next++, expected(Array.Empty<int>()));
            }
            await source.AssertPublishedEqual(next++, expected(new[] { 0, 100, 200 }));

            // The next version replaces a vector past the file's first
            stream.DeleteUsers(Users(101));
            await WaitForVersion(storage, "test", stream, 2);
            await source.AssertPublishedEqual(next, expected(new[] { 0, 100, 101, 200 }));
        }

        private static int Fetches(HookFileStorage storage, string? vectorFile)
        {
            return storage.Requests.Count(x => x.StartsWith("OpenRead ", StringComparison.Ordinal) && x.EndsWith(vectorFile!, StringComparison.Ordinal));
        }

        [Fact]
        public async Task ALaterDeleteFetchesAPackedFileOnce()
        {
            var (storage, stream) = await Start(nameof(ALaterDeleteFetchesAPackedFileOnce));
            await using var _ = stream;
            stream.DeleteUsers(Users(0, 100, 200));
            await WaitForVersion(storage, "test", stream, 1);
            var written = VectorAdds(await ReadCommitActions(storage, "test", 1)).ToList();
            Assert.Equal(3, written.Count);
            var name = written[0].DeletionVector!.AbsolutePath;
            Assert.All(written, x => Assert.Equal(name, x.DeletionVector!.AbsolutePath));
            storage.ClearRequests();

            stream.DeleteUsers(Users(1, 101, 201));
            await WaitForVersion(storage, "test", stream, 2);

            Assert.Equal(1, Fetches(storage, name));
            var moved = VectorAdds(await ReadCommitActions(storage, "test", 2)).ToList();
            Assert.Equal(3, moved.Count);
            foreach (var add in moved)
            {
                var old = written.Single(x => x.Path == add.Path);
                var first = (await new DeletionVectorReader(storage, "/test").Read(old.DeletionVector!)).Single();
                Assert.Equal(new[] { first, first + 1 }, await new DeletionVectorReader(storage, "/test").Read(add.DeletionVector!));
            }
        }

        // One version per checkpoint reads the delete as a change
        [Theory]
        [InlineData(true)]
        [InlineData(false)]
        public async Task ASourceFetchesAPackedFileOnce(bool oneVersionPerCheckpoint)
        {
            var testName = nameof(ASourceFetchesAPackedFileOnce) + oneVersionPerCheckpoint;
            var (storage, stream) = await Start(testName);
            await using var _ = stream;
            stream.DeleteUsers(Users(0, 100, 200));
            await WaitForVersion(storage, "test", stream, 1);
            var name = VectorAdds(await ReadCommitActions(storage, "test", 1)).First().DeletionVector!.AbsolutePath;
            var expected = (int[] deleted) => Enumerable.Range(0, 600).Except(deleted).Select(x => new { userkey = x, name = Fixed(x).FirstName });

            var reads = new HookFileStorage(storage.Inner);
            await using var source = new DeltaLakeTestStream(testName + "Source", reads, oneVersionPerCheckpoint, recordPublished: true);
            await source.StartStream("INSERT INTO output SELECT userkey, name FROM test");

            var next = 1;
            if (oneVersionPerCheckpoint)
            {
                await source.AssertPublishedEqual(next++, expected(Array.Empty<int>()));
            }
            await source.AssertPublishedEqual(next++, expected(new[] { 0, 100, 200 }));
            Assert.Equal(1, Fetches(reads, name));
            reads.ClearRequests();

            stream.DeleteUsers(Users(1, 101, 201));
            await WaitForVersion(storage, "test", stream, 2);
            await source.AssertPublishedEqual(next, expected(new[] { 0, 1, 100, 101, 200, 201 }));
            var replaced = VectorAdds(await ReadCommitActions(storage, "test", 2)).First().DeletionVector!.AbsolutePath;
            Assert.Equal(1, Fetches(reads, name));
            Assert.Equal(1, Fetches(reads, replaced));
        }

        // One packed file took the row after its first
        private static async Task AssertMoved(IFileStorage storage, long version, List<DeltaAddAction> written)
        {
            var actions = await ReadCommitActions(storage, "test", version);
            var moved = actions.Single(x => x.Add != null).Add!;
            var removed = actions.Single(x => x.Remove != null).Remove!;
            var old = written.Single(x => x.Path == moved.Path);
            Assert.Equal(old.DeletionVector!.UniqueId, removed.DeletionVector!.UniqueId);
            var first = (await new DeletionVectorReader(storage, "/test").Read(old.DeletionVector)).Single();
            Assert.Equal(new[] { first, first + 1 }, await new DeletionVectorReader(storage, "/test").Read(moved.DeletionVector!));
        }

        [Fact]
        public async Task VectorsAtAnOffsetSurviveACheckpointAndARestart()
        {
            var testName = nameof(VectorsAtAnOffsetSurviveACheckpointAndARestart);
            var stateFiles = new KeepAliveMemoryFileProvider();
            var storage = new OwnedHookFileStorage(Files.Of.InternalMemory($"./{testName}"));
            var configure = (DeltaLakeOptions options) =>
            {
                options.MaxFileSizeBytes = 1_500;
                options.CheckpointInterval = 1;
            };
            List<DeltaAddAction> written;

            await using (var first = new DeltaLakeSinkStream(testName, storage, configure, stateFiles))
            {
                first.WaitForUpdateDoesNotRequireDataChange();
                AddUsers(first, 600);
                await first.StartStream(UserInsert);
                await WaitForVersion(storage, "test", first, 0);

                first.DeleteUsers(Users(0, 100, 200));
                await WaitForVersion(storage, "test", first, 1);
                written = VectorAdds(await ReadCommitActions(storage, "test", 1)).ToList();
                Assert.Contains(written, x => x.DeletionVector!.Offset > 1);
                await first.Maintenance.TickAndSettleAsync(1);
                await WaitForCheckpoint(storage, "test", first, 1);
                await SettlePublications(first);
                await first.StopStream();
            }

            // Through the checkpoint, the same descriptors and vectors
            var table = (await DeltaTransactionReader.ReadTable(storage, "test", 1))!;
            Assert.Equal(1, table.StartCheckpointVersion);
            var read = table.AddFiles.Where(x => x.DeletionVector != null).ToList();
            Assert.Equal(written.Select(x => x.DeletionVector!.UniqueId).OrderBy(x => x, StringComparer.Ordinal), read.Select(x => x.DeletionVector!.UniqueId).OrderBy(x => x, StringComparer.Ordinal));
            await AssertContainers(storage, "/test", read.Select(x => (x.DeletionVector!, (long[]?)null)));

            await using (var second = new DeltaLakeSinkStream(testName, storage, configure, stateFiles))
            {
                second.WaitForUpdateDoesNotRequireDataChange();
                AddUsers(second, 600);
                second.DeleteUsers(Users(0, 100, 200));
                await second.StartStream(UserInsert);

                second.DeleteUsers(Users(101));
                await WaitForVersion(storage, "test", second, 2);
                await AssertMoved(storage, 2, written);
                await SettlePublications(second);
                await second.StopStream();
            }

            // A restart past the delete reads both kinds of vector
            await using (var third = new DeltaLakeSinkStream(testName, storage, configure, stateFiles))
            {
                third.WaitForUpdateDoesNotRequireDataChange();
                AddUsers(third, 600);
                third.DeleteUsers(Users(0, 100, 200));
                third.DeleteUsers(Users(101));
                await third.StartStream(UserInsert);

                third.DeleteUsers(Users(201));
                await WaitForVersion(storage, "test", third, 3);
                await AssertMoved(storage, 3, written);

                // The vector version 2 wrote takes one more row
                third.DeleteUsers(Users(102));
                await WaitForVersion(storage, "test", third, 4);
                var replaced = (await ReadCommitActions(storage, "test", 2)).Single(x => x.Add != null).Add!;
                var fourth = await ReadCommitActions(storage, "test", 4);
                Assert.Equal(replaced.DeletionVector!.UniqueId, fourth.Single(x => x.Remove != null).Remove!.DeletionVector!.UniqueId);
                var kept = (await new DeletionVectorReader(storage, "/test").Read(replaced.DeletionVector)).ToList();
                Assert.Equal(kept.Append(kept[^1] + 1), await new DeletionVectorReader(storage, "/test").Read(fourth.Single(x => x.Add != null).Add!.DeletionVector!));
            }
        }
    }
}
