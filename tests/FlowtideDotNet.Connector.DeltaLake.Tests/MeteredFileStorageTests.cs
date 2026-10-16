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

using FlowtideDotNet.Connector.DeltaLake.Internal.Delta.Checkpoints;
using FlowtideDotNet.Connector.DeltaLake.Internal.Maintenance;
using Microsoft.Extensions.Time.Testing;
using Stowage;
using Stowage.Impl;
using System.Text;
using System.Text.Json;

namespace FlowtideDotNet.Connector.DeltaLake.Tests
{
    [Collection(nameof(PinnedClientCollection))]
    public class MeteredFileStorageTests : IDisposable
    {
        private static readonly DateTimeOffset s_now = new DateTimeOffset(2026, 10, 15, 0, 0, 0, TimeSpan.Zero);
        private static readonly TimeSpan s_second = TimeSpan.FromSeconds(1);
        private static readonly TimeSpan s_timeout = TimeSpan.FromSeconds(30);

        private readonly string _directory = Path.Combine(Path.GetTempPath(), "mf-" + ScratchOwners.ShortId());
        private readonly ScratchOwners _owners = new ScratchOwners();

        public MeteredFileStorageTests()
        {
            Directory.CreateDirectory(_directory);
        }

        public void Dispose()
        {
            _owners.Release();
            try
            {
                Directory.Delete(_directory, true);
            }
            catch (IOException)
            {
            }
            catch (UnauthorizedAccessException)
            {
            }
        }

        // Records how it was disposed and can fail every write
        private sealed class TrackingStream : MemoryStream
        {
            public bool FailWrites { get; set; }

            // Closing fails, as a final flush can
            public bool FailDispose { get; set; }

            public int Disposes { get; private set; }

            public int AsyncDisposes { get; private set; }

            public override void Write(byte[] buffer, int offset, int count)
            {
                if (FailWrites)
                {
                    throw new IOException("network");
                }
                base.Write(buffer, offset, count);
            }

            public override void Write(ReadOnlySpan<byte> buffer)
            {
                if (FailWrites)
                {
                    throw new IOException("network");
                }
                base.Write(buffer);
            }

            public override ValueTask WriteAsync(ReadOnlyMemory<byte> buffer, CancellationToken cancellationToken = default)
            {
                return FailWrites ? ValueTask.FromException(new IOException("network")) : base.WriteAsync(buffer, cancellationToken);
            }

            protected override void Dispose(bool disposing)
            {
                if (disposing)
                {
                    Disposes++;
                }
                base.Dispose(disposing);
                if (disposing && FailDispose)
                {
                    throw new IOException("flush");
                }
            }

            public override ValueTask DisposeAsync()
            {
                AsyncDisposes++;
                return base.DisposeAsync();
            }
        }

        private DeltaMaintenanceBudget NewBudget(MaintenanceBuckets? buckets = null)
        {
            var settings = new MaintenanceBudgetSettings(2, Path.Combine(_directory, "scratch-" + ScratchOwners.ShortId()));
            return buckets == null
                ? new DeltaMaintenanceBudget(settings, new FakeTimeProvider(s_now), manualTick: true)
                : new DeltaMaintenanceBudget(settings, new FakeTimeProvider(s_now), manualTick: true) { Buckets = buckets };
        }

        private static DeltaMaintenanceWorker NewWorker(DeltaMaintenanceBudget budget, IFileStorage storage)
        {
            var worker = new DeltaMaintenanceWorker(budget, new MaintenanceWorkerSettings(2, null, new FakeTimeProvider(s_now)), storage, "t", "t", null);
            budget.Register(worker);
            worker.SetTableId("table-id");
            return worker;
        }

        [Fact]
        public async Task MeteredStorageCoversEveryCall()
        {
            var hooks = new HookFileStorage(Files.Of.InternalMemory("./every-" + Guid.NewGuid().ToString("N")));
            var clock = new FakeTimeProvider(s_now);
            var buckets = new MaintenanceBuckets(clock, requestsPerSecond: 1, requestBurst: 1, bytesPerSecond: 1 << 20, byteBurst: 1 << 20);
            var storage = MeteredFileStorage.Wrap(hooks, buckets, ListingPaging.S3);
            Assert.IsNotAssignableFrom<ILocalDiskFileStorage>(storage);
            // Local disk stays local disk through the meter, so native paths keep working
            var root = Path.Combine(_directory, "every");
            var local = MeteredFileStorage.Wrap(Files.Of.LocalDisk(root), buckets, ListingPaging.LocalDisk);
            Assert.Equal(Path.Combine(root, "t", "a.bin"), Path.GetFullPath(((ILocalDiskFileStorage)local).ToNativeLocalPath("/t/a.bin")));
            await hooks.WriteText("/t/a.bin", "abcdef");
            await hooks.WriteText("/t/r1.bin", "r");
            await hooks.WriteText("/t/gone.bin", "g");
            await hooks.WriteText("/t/json.json", "[1,2,3]");
            await buckets.Requests.AcquireAsync(1);

            var json = JsonSerializer.Serialize<object>(new[] { 4, 5 }, new JsonSerializerOptions() { WriteIndented = true });
            var calls = new (string Member, string Verb, long Bytes, Func<IFileStorage, Task> Call)[]
            {
                ("Ls", "Ls", 0, s => s.Ls("/t/")),
                ("OpenRead", "OpenRead", 0, async s => await (await s.OpenRead("/t/a.bin"))!.DisposeAsync()),
                ("OpenWrite", "OpenWrite", 0, async s => await (await s.OpenWrite("/t/w.bin")).DisposeAsync()),
                ("Ren", "Ren", 0, s => s.Ren("/t/r1.bin", "/t/r2.bin")),
                ("ReadText", "ReadText", 6, s => s.ReadText("/t/a.bin")),
                ("WriteText", "WriteText", 5, s => s.WriteText("/t/text.txt", "hello")),
                ("ReadAsJson", "ReadText", 7, s => s.ReadAsJson<int[]>("/t/json.json")),
                ("WriteAsJson", "WriteText", Encoding.UTF8.GetByteCount(json), s => s.WriteAsJson("/t/json2.json", new[] { 4, 5 }, true)),
                ("Rm", "Rm", 0, s => s.Rm("/t/gone.bin")),
                ("Exists", "Exists", 0, s => s.Exists("/t/a.bin")),
                ("Stat", "Stat", 0, s => s.Stat("/t/a.bin"))
            };
            // Every member of the interface is in the table, and the meter implements each itself rather than through a default body
            Assert.Equal(typeof(IFileStorage).GetMethods().Select(x => x.Name).Order(), calls.Select(x => x.Member).Order());
            Assert.All(typeof(MeteredFileStorage).GetInterfaceMap(typeof(IFileStorage)).TargetMethods, x => Assert.Equal(typeof(MeteredFileStorage), x.DeclaringType));

            foreach (var (member, verb, bytes, call) in calls)
            {
                hooks.ClearRequests();
                var requests = buckets.Requests.Charged;
                var charged = buckets.Bytes.Charged;
                var task = call(storage);
                // Nothing reaches the store before its request is charged
                Assert.False(task.IsCompleted, member);
                Assert.Equal(1, buckets.Requests.QueueLength);
                Assert.Empty(hooks.Requests);
                clock.Advance(s_second);
                await task.WaitAsync(s_timeout);
                Assert.Equal(verb, Assert.Single(hooks.Requests).Split(' ')[0]);
                Assert.Equal(requests + 1, buckets.Requests.Charged);
                Assert.Equal(charged + bytes, buckets.Bytes.Charged);
            }
            Assert.Equal(new[] { 4, 5 }, await hooks.ReadAsJson<int[]>("/t/json2.json"));
            Assert.Equal("r", await hooks.ReadText("/t/r2.bin"));
            Assert.False(await hooks.Exists("/t/gone.bin"));

            // Stream bytes, the synchronous members as well as the asynchronous ones
            clock.Advance(s_second);
            var write = await storage.OpenWrite("/t/stream.bin");
            var before = buckets.Bytes.Charged;
            await write.WriteAsync(new byte[] { 1, 2, 3 }, 0, 3);
            await write.WriteAsync(new ReadOnlyMemory<byte>(new byte[] { 4, 5, 6, 7 }));
            write.Write(new byte[] { 8, 9, 10, 11, 12 }, 0, 5);
            write.Write(new ReadOnlySpan<byte>(new byte[] { 13, 14, 15, 16, 17, 18 }));
            write.WriteByte(19);
            write.EndWrite(write.BeginWrite(new byte[] { 20, 21 }, 0, 2, null, null));
            Assert.Equal(21, buckets.Bytes.Charged - before);
            await write.DisposeAsync();
            Assert.Equal(Enumerable.Range(1, 21).Select(x => (byte)x).ToArray(), await DeltaTestKit.ReadBytes(hooks, "/t/stream.bin"));

            clock.Advance(s_second);
            var read = (await storage.OpenRead("/t/stream.bin"))!;
            before = buckets.Bytes.Charged;
            var buffer = new byte[4];
            Assert.Equal(3, await read.ReadAsync(buffer, 0, 3));
            Assert.Equal(4, await read.ReadAsync(buffer.AsMemory(0, 4)));
            Assert.Equal(2, read.Read(buffer, 0, 2));
            Assert.Equal(2, read.Read(buffer.AsSpan(0, 2)));
            Assert.Equal(12, read.ReadByte());
            Assert.Equal(1, read.EndRead(read.BeginRead(buffer, 0, 1, null, null)));
            var rest = new MemoryStream();
            read.CopyTo(rest);
            Assert.Equal(8, rest.Length);
            Assert.Equal(-1, read.ReadByte());
            Assert.Equal(21, buckets.Bytes.Charged - before);
            await read.DisposeAsync();

            // Bytes are charged before they reach the store
            hooks.RecordWritesOf = "late.bin";
            clock.Advance(s_second);
            var late = await storage.OpenWrite("/t/late.bin");
            if (buckets.Bytes.Available is > 0 and var available)
            {
                await buckets.Bytes.AcquireAsync(available);
            }
            var pending = late.WriteAsync(new byte[10]).AsTask();
            Assert.False(pending.IsCompleted);
            Assert.Empty(hooks.WriteEvents);
            clock.Advance(s_second);
            await pending.WaitAsync(s_timeout);
            Assert.Equal(new[] { "W10" }, hooks.WriteEvents);
            await late.DisposeAsync();

            // The storage below belongs to the sink, so disposing the meter leaves it open
            var stub = new MaintenanceBucketsTests.ListingStub(0);
            MeteredFileStorage.Wrap(stub, buckets, ListingPaging.S3).Dispose();
            Assert.False(stub.Disposed);
            // Metering a metered storage again charges once, not twice
            Assert.Same(hooks, MeteredFileStorage.Wrap(storage, buckets, ListingPaging.LocalDisk).Inner);
            Assert.Same(hooks, RawStorage.Unwrap(storage));
        }

        [Fact]
        public async Task NativeOperationsAreChargedToBuckets()
        {
            var root = Path.Combine(_directory, "native");
            Directory.CreateDirectory(root);
            var raw = Files.Of.LocalDisk(root);
            await DeltaMaintenanceWorkerTests.Commits(raw, 0, 2);
            var clock = new FakeTimeProvider(s_now);
            var buckets = new MaintenanceBuckets(clock, requestBurst: 1000);
            var storage = MeteredFileStorage.Wrap(raw, buckets, ListingPaging.LocalDisk);
            var scratch = _owners.Add(new ScratchDirectory(Path.Combine(_directory, ScratchOwners.ShortId())));
            var memory = new JobMemory();

            BuildResult result;
            using (var job = scratch.CreateJob())
            {
                result = await CheckpointBuilder.BuildAsync(new CheckpointBuildContext() { Storage = storage, Table = "t", Scratch = job, Memory = memory, Clock = clock }, new BuildRequest() { Version = 2 });
            }
            Assert.Equal(BuildStatus.Completed, result.Status);
            var log = Path.Combine(root, "t", "_delta_log");
            var hintPath = Path.Combine(log, "_last_checkpoint");
            var hintBytes = new FileInfo(hintPath).Length;
            // The checkpoint's move, rename and size check with its bytes, then the hint's temp write and rename with its bytes
            Assert.Equal(3 + 2, buckets.NativeRequests);
            Assert.Equal(result.Checkpoint!.SizeInBytes + hintBytes, buckets.NativeBytes);

            // Inspection reads the checkpoint in place: its probe and its bytes, as a download would be charged
            var transport = new StowageCheckpointTransport(storage, "t");
            var segment = await transport.ListAsync(CancellationToken.None);
            using (var job = scratch.CreateJob())
            {
                var inspected = await CheckpointInspector.InspectAsync(transport, job, memory, null, segment.Listing.Checkpoints.Single(x => x.Version == 2), CancellationToken.None);
                Assert.Equal(CheckpointClass.Valid, inspected.Class);
                Assert.False(inspected.Downloaded);
            }
            Assert.Equal(3 + 2 + 1, buckets.NativeRequests);
            Assert.Equal(2 * result.Checkpoint.SizeInBytes + hintBytes, buckets.NativeBytes);

            // Nothing is written natively before its requests are charged
            if (buckets.Requests.Available is > 0 and var available)
            {
                await buckets.Requests.AcquireAsync(available);
            }
            var previous = File.ReadAllText(hintPath);
            var writing = transport.WriteHintAsync(new LastCheckpointInfo() { Version = 9, Size = 1, SizeInBytes = 1 }, CancellationToken.None);
            Assert.False(writing.IsCompleted);
            Assert.Equal(previous, File.ReadAllText(hintPath));
            clock.Advance(s_second);
            await writing.WaitAsync(s_timeout);
            Assert.Equal(9, JsonSerializer.Deserialize<LastCheckpointInfo>(File.ReadAllText(hintPath))!.Version);
            Assert.Equal(3 + 2 + 1 + 2, buckets.NativeRequests);

            // Unmetered storage charges nothing and needs no buckets
            await MeteredFileStorage.ChargeNativeAsync(raw, 5, 5, CancellationToken.None);
            Assert.Equal(3 + 2 + 1 + 2, buckets.NativeRequests);

            // A worker's native publication is charged to its budget's buckets
            var workerRoot = Path.Combine(_directory, "native-worker");
            Directory.CreateDirectory(workerRoot);
            var workerRaw = Files.Of.LocalDisk(workerRoot);
            await DeltaMaintenanceWorkerTests.Commits(workerRaw, 0, 2);
            var workerBuckets = new MaintenanceBuckets(TimeProvider.System, requestBurst: 100_000, byteBurst: 1L << 30);
            var budget = NewBudget(workerBuckets);
            var worker = NewWorker(budget, workerRaw);
            worker.Published(2);
            budget.TickNow();
            await worker.CurrentJob.WaitAsync(s_timeout);
            Assert.Equal(2, worker.LastCompleted);
            Assert.Equal(3 + 2, workerBuckets.NativeRequests);
            await worker.StopAsync(s_timeout);
        }

        [Theory]
        [InlineData(StoreFlavor.Azure)]
        [InlineData(StoreFlavor.S3)]
        public async Task SuccessorAndAzureCandidateStatsAreMetered(StoreFlavor flavor)
        {
            await using var server = new LoopbackObjectStore(flavor);
            using var raw = server.CreateStorage();
            server.Put("t/_delta_log/00000000000000000000.json");
            server.Put("t/part-1.parquet", new byte[7]);
            var clock = new FakeTimeProvider(s_now);
            var buckets = new MaintenanceBuckets(clock, requestsPerSecond: 1, requestBurst: 1);
            await buckets.Requests.AcquireAsync(1);
            var storage = MeteredFileStorage.Wrap(raw, buckets, flavor == StoreFlavor.S3 ? ListingPaging.S3 : ListingPaging.Azure);

            // The successor probe waits for its token before anything is sent, and an absent successor reads as absent
            var probe = storage.Stat($"/c/t/_delta_log/{1:D20}.json");
            Assert.False(probe.IsCompleted);
            Assert.Empty(server.Requests);
            clock.Advance(s_second);
            Assert.Null(await probe.WaitAsync(s_timeout));
            var sent = server.Requests.Count;
            Assert.True(sent > 0);

            // An orphan candidate's size check is charged the same way
            var candidate = storage.Stat("/c/t/part-1.parquet");
            Assert.False(candidate.IsCompleted);
            Assert.Equal(sent, server.Requests.Count);
            clock.Advance(s_second);
            Assert.Equal(7, (await candidate.WaitAsync(s_timeout))!.Size);
            sent = server.Requests.Count;

            // A failed probe was charged as well, and fails rather than reading as absent
            server.Intercept = (method, target) => method == "HEAD" ? (500, "") : null;
            var failed = storage.Stat($"/c/t/_delta_log/{1:D20}.json");
            Assert.Equal(sent, server.Requests.Count);
            clock.Advance(s_second);
            await Assert.ThrowsAnyAsync<Exception>(() => failed.WaitAsync(s_timeout));
            Assert.Equal(1 + 3, buckets.Requests.Charged);
        }

        [Fact]
        public async Task FailedMeteredWriteDoesNotDisposeInner()
        {
            var open = new MaintenanceBuckets(TimeProvider.System, byteBurst: 1 << 20);

            // An inner write that fails: neither kind of dispose reaches the inner stream
            var failing = new TrackingStream() { FailWrites = true };
            var stream = new MeteredWriteStream(failing, open);
            await Assert.ThrowsAsync<IOException>(() => stream.WriteAsync(new byte[4]).AsTask());
            Assert.True(stream.Abandoned);
            stream.Dispose();
            await stream.DisposeAsync();
            Assert.Equal(0, failing.Disposes);
            Assert.Equal(0, failing.AsyncDisposes);

            var failingSync = new TrackingStream() { FailWrites = true };
            using (var syncStream = new MeteredWriteStream(failingSync, open))
            {
                Assert.Throws<IOException>(() => syncStream.Write(new byte[4], 0, 4));
            }
            Assert.Equal(0, failingSync.Disposes);

            // An object store that commits whatever was written when its stream is disposed
            var hooks = new HookFileStorage(new CommitOnDisposeStorage(Files.Of.InternalMemory("./metered-" + Guid.NewGuid().ToString("N"))));
            var clock = new FakeTimeProvider(s_now);
            var slow = new MaintenanceBuckets(clock, bytesPerSecond: 16, byteBurst: 16);
            var storage = MeteredFileStorage.Wrap(hooks, slow, ListingPaging.S3);

            // A write cancelled while it waits for its bytes publishes nothing, even when the caller disposes
            var cancelled = await storage.OpenWrite("/t/cancelled.bin");
            await cancelled.WriteAsync(new byte[16]);
            using (var cancellation = new CancellationTokenSource())
            {
                var waiting = cancelled.WriteAsync(new byte[8], cancellation.Token).AsTask();
                Assert.False(waiting.IsCompleted);
                cancellation.Cancel();
                await Assert.ThrowsAnyAsync<OperationCanceledException>(() => waiting);
            }
            await cancelled.DisposeAsync();
            Assert.False(await hooks.Exists("/t/cancelled.bin"));

            // The transport's chunked upload, cancelled between chunks, publishes nothing through the meter
            var local = Path.Combine(_directory, "upload.bin");
            File.WriteAllBytes(local, Enumerable.Range(0, 10_000).Select(x => (byte)x).ToArray());
            var uploads = MeteredFileStorage.Wrap(hooks, open, ListingPaging.S3);
            using (var cancellation = new CancellationTokenSource())
            {
                var transport = new StowageCheckpointTransport(uploads, "t") { ChunkSize = 4096, AfterChunkForTests = _ => cancellation.Cancel() };
                await Assert.ThrowsAnyAsync<OperationCanceledException>(() => transport.UploadAsync(local, "/t/upload.bin", new JobMemory(), cancellation.Token));
            }
            Assert.False(await hooks.Exists("/t/upload.bin"));

            // Success is disposed explicitly and awaited, which publishes
            var tracked = new TrackingStream();
            var good = new MeteredWriteStream(tracked, open);
            await good.WriteAsync(new byte[3]);
            await good.DisposeAsync();
            await good.DisposeAsync();
            Assert.Equal(1, tracked.AsyncDisposes);
            Assert.Equal(1, tracked.Disposes);
            await new StowageCheckpointTransport(uploads, "t") { ChunkSize = 4096 }.UploadAsync(local, "/t/uploaded.bin", new JobMemory(), CancellationToken.None);
            Assert.Equal(File.ReadAllBytes(local), await DeltaTestKit.ReadBytes(hooks, "/t/uploaded.bin"));
        }

        // Fails while any other handle is open, on Unix through the advisory lock .NET takes
        private static bool OpensExclusively(string path)
        {
            try
            {
                using (new FileStream(path, FileMode.Open, FileAccess.ReadWrite, FileShare.None))
                {
                }
                return true;
            }
            catch (IOException)
            {
                return false;
            }
        }

        [Fact]
        public async Task FailedLocalDiskWriteReleasesItsHandle()
        {
            var clock = new FakeTimeProvider(s_now);
            var slow = new MaintenanceBuckets(clock, bytesPerSecond: 16, byteBurst: 16);
            var storage = MeteredFileStorage.Wrap(Files.Of.LocalDisk(Path.Combine(_directory, "in-place")), slow, ListingPaging.LocalDisk);
            var native = ((ILocalDiskFileStorage)storage).ToNativeLocalPath("/t/torn.bin");

            // Local disk writes in place, so the name holds the file before the write fails
            var stream = await storage.OpenWrite("/t/torn.bin");
            await stream.WriteAsync(new byte[16]);
            using (var cancellation = new CancellationTokenSource())
            {
                var waiting = stream.WriteAsync(new byte[8], cancellation.Token).AsTask();
                Assert.False(waiting.IsCompleted);
                cancellation.Cancel();
                await Assert.ThrowsAnyAsync<OperationCanceledException>(() => waiting);
            }
            Assert.True(File.Exists(native));
            Assert.False(OpensExclusively(native));

            await stream.DisposeAsync();

            // Disposing closed the native handle rather than leaving it to a finalizer
            Assert.True(OpensExclusively(native));
            File.Delete(native);
            Assert.False(File.Exists(native));
            GC.KeepAlive(stream);

            // Either kind of dispose closes a failed write in place once, and a failing close is not reported again
            var open = new MaintenanceBuckets(TimeProvider.System, byteBurst: 1 << 20);
            var failingSync = new TrackingStream() { FailWrites = true, FailDispose = true };
            using (var syncStream = new MeteredWriteStream(failingSync, open, writesInPlace: true))
            {
                Assert.Throws<IOException>(() => syncStream.Write(new byte[4], 0, 4));
                Assert.True(syncStream.Abandoned);
            }
            Assert.Equal(1, failingSync.Disposes);
            var failingAsync = new TrackingStream() { FailWrites = true, FailDispose = true };
            var asyncStream = new MeteredWriteStream(failingAsync, open, writesInPlace: true);
            await Assert.ThrowsAsync<IOException>(() => asyncStream.WriteAsync(new byte[4]).AsTask());
            await asyncStream.DisposeAsync();
            asyncStream.Dispose();
            Assert.Equal(1, failingAsync.AsyncDisposes);
            Assert.Equal(1, failingAsync.Disposes);
        }

        [Fact]
        public async Task RootCacheHitsSurviveWorkerRestart()
        {
            // Stamped listings carry a modification time, so the cached root's identity is strong
            var hooks = new HookFileStorage(Files.Of.InternalMemory("./restart-" + Guid.NewGuid().ToString("N"))) { StampIdentity = true };
            var budget = NewBudget();
            var first = NewWorker(budget, hooks);
            await DeltaMaintenanceWorkerTests.Commits(hooks, 0, 2);
            first.Published(2);
            budget.TickNow();
            await first.CurrentJob.WaitAsync(s_timeout);
            Assert.Equal(new long[] { 2 }, await DeltaMaintenanceWorkerTests.Checkpoints(hooks));
            Assert.Equal(1, budget.Scratch.Cache.Count);
            await first.StopAsync(s_timeout);

            // The new worker meters through a new wrapper over the same storage, and still reads its root from the cache
            var second = NewWorker(budget, hooks);
            await DeltaMaintenanceWorkerTests.Commits(hooks, 3, 4);
            second.Published(4);
            hooks.ClearRequests();
            budget.TickNow();
            await second.CurrentJob.WaitAsync(s_timeout);
            Assert.Equal(new long[] { 2, 4 }, await DeltaMaintenanceWorkerTests.Checkpoints(hooks));
            Assert.Equal(2, second.LastBuild!.RootVersion);
            Assert.DoesNotContain(hooks.Requests, x => x.StartsWith("OpenRead") && x.EndsWith($"{2:D20}.checkpoint.parquet"));
            await second.StopAsync(s_timeout);
        }

        [Fact]
        public async Task StopCancelsAJobWaitingForItsListingTurn()
        {
            var clock = new FakeTimeProvider(s_now);
            var buckets = new MaintenanceBuckets(clock, requestsPerSecond: 1, requestBurst: 1);
            var hooks = new HookFileStorage(Files.Of.InternalMemory("./stop-listing-" + Guid.NewGuid().ToString("N")));
            await DeltaMaintenanceWorkerTests.Commits(hooks, 0, 2);
            var budget = NewBudget(buckets);
            var worker = NewWorker(budget, hooks);
            await buckets.Requests.AcquireAsync(1);
            worker.Published(2);
            hooks.ClearRequests();

            budget.TickNow();
            // The job's log listing waits for a token the fake clock never grants
            await MaintenanceBucketsTests.Until(() => buckets.Requests.QueueLength == 1);
            Assert.Empty(hooks.Requests);

            await worker.StopAsync(s_timeout);

            // The listing stopped waiting, so the job settled within the join and its table lease is free
            Assert.Equal(0, worker.DetachedJobs);
            Assert.True(worker.CurrentJob.IsCompleted);
            var lease = budget.Leases.AcquireAsync(TableLeases.KeyOf(hooks, "t"));
            Assert.True(lease.IsCompletedSuccessfully);
            (await lease).Dispose();
            Assert.Equal(0, buckets.Requests.QueueLength);
            Assert.Empty(hooks.Requests);
        }

        [Theory]
        [InlineData(false, "LocalDisk")]
        [InlineData(true, "S3")]
        public async Task ReplacedStorageIsMeteredWithTheConservativePaging(bool replaced, string paging)
        {
            var testName = nameof(ReplacedStorageIsMeteredWithTheConservativePaging) + replaced;
            var root = Path.Combine(_directory, "sink-" + replaced);
            Directory.CreateDirectory(root);
            var storage = Files.Of.LocalDisk(root);
            await using var stream = new DeltaLakeSinkStream(testName, storage, options =>
            {
                if (replaced)
                {
                    // Another instance after the classification at construction, whose paging the sink cannot know
                    options.SinkCreatedForTests = _ => options.StorageLocation = new ForwardingStorage(storage);
                }
            });
            stream.Generate(3);
            await stream.StartStream(DeltaTestKit.UserInsert);
            await DeltaTestKit.WaitForVersion(storage, "test", stream, 0);

            var worker = Assert.Single(stream.Maintenance.Workers);
            Assert.Equal(Enum.Parse<ListingPaging>(paging), worker.Paging);
            Assert.Equal(!replaced, worker.CleanupEligible);
        }

        [Fact]
        public async Task WorkerTrafficIsMetered()
        {
            var clock = new FakeTimeProvider(s_now);
            var buckets = new MaintenanceBuckets(clock, requestsPerSecond: 1000, requestBurst: 1000);
            var hooks = new HookFileStorage(Files.Of.InternalMemory("./worker-metered-" + Guid.NewGuid().ToString("N")));
            await DeltaMaintenanceWorkerTests.Commits(hooks, 0, 2);
            var budget = NewBudget(buckets);
            var worker = NewWorker(budget, hooks);
            await buckets.Requests.AcquireAsync(1000);
            worker.Published(2);
            hooks.ClearRequests();

            budget.TickNow();
            // The job's first storage call waits for a token, so nothing has reached the store
            await MaintenanceBucketsTests.Until(() => buckets.Requests.QueueLength == 1);
            Assert.Empty(hooks.Requests);
            clock.Advance(s_second);
            await worker.CurrentJob.WaitAsync(s_timeout);
            Assert.Equal(2, worker.LastCompleted);

            // Every request the store saw was charged once, and the bytes moved were charged too
            Assert.NotEmpty(hooks.Requests);
            Assert.Equal(hooks.Requests.Count, buckets.Requests.Charged - 1000);
            Assert.True(buckets.Bytes.Charged > 0);
            await worker.StopAsync(s_timeout);
        }
    }
}
