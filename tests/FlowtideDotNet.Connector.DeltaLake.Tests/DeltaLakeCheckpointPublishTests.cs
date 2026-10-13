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

using FlowtideDotNet.Connector.DeltaLake.Internal.Delta;
using FlowtideDotNet.Connector.DeltaLake.Internal.Delta.Actions;
using FlowtideDotNet.Connector.DeltaLake.Internal.Delta.Checkpoints;
using FlowtideDotNet.Connector.DeltaLake.Internal.Delta.ParquetFormat.Bounded;
using FlowtideDotNet.Connector.DeltaLake.Internal.Maintenance;
using Microsoft.Extensions.Time.Testing;
using Stowage;
using System.Text.Json;

namespace FlowtideDotNet.Connector.DeltaLake.Tests
{
    public class DeltaLakeCheckpointPublishTests : IDisposable
    {
        private static readonly DateTimeOffset s_now = new DateTimeOffset(2026, 10, 1, 12, 0, 0, TimeSpan.Zero);
        private readonly string _directory;

        public DeltaLakeCheckpointPublishTests()
        {
            _directory = Path.Combine(Path.GetTempPath(), "pt-" + ScratchOwners.ShortId());
            Directory.CreateDirectory(_directory);
        }

        private readonly ScratchOwners _owners = new ScratchOwners();

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
        }

        [Fact]
        public void DisposeReleasesScratchOwnersRetainedUntilTeardown()
        {
            var owner = _owners.Add(new ScratchDirectory(Path.Combine(_directory, ScratchOwners.ShortId())));
            Assert.True(owner.LockHeldForTests);
            try
            {
                Dispose();
                Assert.False(owner.LockHeldForTests);
                Assert.False(Directory.Exists(_directory));
            }
            finally
            {
                owner.ReleaseLockForTests();
            }
        }

        private sealed class Setup
        {
            public required IFileStorage Storage { get; init; }

            // The hooks, absent on unwrapped local disk
            public HookFileStorage? Hooks { get; init; }

            public string? Root { get; init; }
        }

        private Setup Storage(string kind)
        {
            switch (kind)
            {
                case "memory":
                    {
                        var hooks = new HookFileStorage(Files.Of.InternalMemory($"./publish-{Guid.NewGuid():N}"));
                        return new Setup() { Storage = hooks, Hooks = hooks };
                    }
                case "commit-on-dispose":
                    {
                        var hooks = new HookFileStorage(new CommitOnDisposeStorage(Files.Of.InternalMemory($"./publish-{Guid.NewGuid():N}")));
                        return new Setup() { Storage = hooks, Hooks = hooks };
                    }
                default:
                    {
                        var root = Path.Combine(_directory, "table-" + Guid.NewGuid().ToString("N"));
                        Directory.CreateDirectory(root);
                        return new Setup() { Storage = Files.Of.LocalDisk(root), Root = root };
                    }
            }
        }

        // A table with a root at 3 and adds that make the checkpoint span several 4 KiB upload chunks
        private static async Task Table(IFileStorage storage)
        {
            await DeltaTestKit.WriteCommit(storage, "t", 0, DeltaTestKit.Protocol(), DeltaTestKit.Metadata(DeltaTestKit.UserSchema));
            for (int version = 1; version <= 6; version++)
            {
                var adds = Enumerable.Range(0, 100).Select(i => DeltaLakeCheckpointBuildTests.Add($"v{version}-{i}-{Guid.NewGuid():N}.parquet")).ToArray();
                await DeltaTestKit.WriteCommit(storage, "t", version, adds);
            }
            await DeltaLakeCheckpointBuildTests.LegacyRoot(storage, "t", 3);
        }

        private static string CheckpointPath(long version) => $"/t/_delta_log/{version:D20}.checkpoint.parquet";

        private sealed record Run(BuildResult? Result, Exception? Error, ScratchDirectory Scratch);

        private async Task<Run> Build(Setup setup, long version, long diskCap = DiskLedger.DefaultCap, long arenaCap = TailReducer.DefaultArenaCap,
            Action<string>? publishFault = null, Action<string>? damage = null, Func<string, long>? freeSpace = null, int chunkSize = StowageCheckpointTransport.DefaultChunkSize,
            Action<int>? afterChunk = null, CancellationToken cancellationToken = default, RootPolicy root = RootPolicy.Auto, ScratchDirectory? scratch = null, JobMemory? memory = null,
            Action<string>? buildFault = null)
        {
            scratch ??= _owners.Add(new ScratchDirectory(Path.Combine(_directory, ScratchOwners.ShortId()), diskCap));
            memory ??= new JobMemory();
            BuildResult? result = null;
            Exception? error = null;
            using (var job = scratch.CreateJob())
            {
                var context = new CheckpointBuildContext()
                {
                    Storage = setup.Storage,
                    Table = "t",
                    Scratch = job,
                    Memory = memory,
                    Clock = new FakeTimeProvider(s_now),
                    ArenaCap = arenaCap,
                    Transport = new StowageCheckpointTransport(setup.Storage, "t") { ChunkSize = chunkSize, AfterChunkForTests = afterChunk },
                    Transfer = new TransferAllowance(),
                    PublishFaultForTests = publishFault,
                    BuildFaultForTests = buildFault,
                    BeforeValidationForTests = damage,
                    FreeSpaceForTests = freeSpace
                };
                try
                {
                    result = await CheckpointBuilder.BuildAsync(context, new BuildRequest() { Version = version, Root = root }, cancellationToken);
                }
                catch (Exception e)
                {
                    error = e;
                }
            }
            Assert.Equal(0, memory.Managed);
            Assert.Equal(0, memory.Native);
            return new Run(result, error, scratch);
        }

        private static async Task<LastCheckpointInfo?> Hint(IFileStorage storage)
        {
            var text = await storage.ReadText("/t/_delta_log/_last_checkpoint");
            return text == null ? null : JsonSerializer.Deserialize<LastCheckpointInfo>(text);
        }

        // A published checkpoint is complete: its listed size and the scan agree
        private async Task AssertCompleteOrAbsent(IFileStorage storage, long version)
        {
            var entry = (await storage.Ls("/t/_delta_log/")).FirstOrDefault(x => x.Name == $"{version:D20}.checkpoint.parquet");
            if (entry == null)
            {
                return;
            }
            var path = Path.Combine(_directory, Guid.NewGuid().ToString("N") + ".parquet");
            await File.WriteAllBytesAsync(path, await DeltaTestKit.ReadBytes(storage, entry.Path.Full));
            // The in-memory store lists no sizes
            Assert.True(entry.Size == null || entry.Size == new FileInfo(path).Length);
            var memory = new JobMemory();
            var scan = CheckpointPageScan.Scan(path, memory);
            memory.Release(scan.ModelBytes);
        }

        public static TheoryData<string, string> Cells()
        {
            var cells = new TheoryData<string, string>();
            foreach (var kind in new[] { "memory", "commit-on-dispose", "local" })
            {
                foreach (var fault in new[] { "list", "hint read", "root read", "tail read", "run write", "merge", "local write", "writer close", "validation", "move", "rename", "upload torn", "upload cancelled", "size check", "hint write", "scratch deletion" })
                {
                    var hooked = fault is "list" or "hint read" or "root read" or "tail read" or "upload torn" or "size check";
                    if (kind == "local" && (hooked || fault == "upload cancelled"))
                    {
                        continue;
                    }
                    if (kind != "local" && fault is "move" or "rename")
                    {
                        continue;
                    }
                    cells.Add(kind, fault);
                }
            }
            return cells;
        }

        [Theory]
        [MemberData(nameof(Cells))]
        public async Task FaultMatrix(string kind, string fault)
        {
            var setup = Storage(kind);
            await Table(setup.Storage);
            using var cancellation = new CancellationTokenSource();
            var hooks = setup.Hooks;
            switch (fault)
            {
                case "list":
                    hooks!.Before = (verb, path) => verb == "Ls" ? throw new IOException("listing fault") : Task.CompletedTask;
                    break;
                case "hint read":
                    hooks!.Before = (verb, path) => verb == "OpenRead" && path.Full.EndsWith("_last_checkpoint") ? throw new IOException("hint fault") : Task.CompletedTask;
                    break;
                case "root read":
                    hooks!.Before = (verb, path) => verb == "OpenRead" && path.Full.EndsWith(".checkpoint.parquet") ? throw new IOException("root fault") : Task.CompletedTask;
                    break;
                case "tail read":
                    hooks!.Before = (verb, path) => verb == "OpenRead" && path.Full.EndsWith($"{5:D20}.json") ? throw new IOException("tail fault") : Task.CompletedTask;
                    break;
                case "upload torn":
                    hooks!.TearWriteOf = $"{6:D20}.checkpoint.parquet";
                    hooks!.TearAfterBytes = 100;
                    break;
                case "size check":
                    hooks!.Before = (verb, path) => verb == "Stat" ? throw new IOException("stat fault") : Task.CompletedTask;
                    break;
            }
            var run = await Build(setup, 6,
                diskCap: fault == "local write" ? 70_000 : DiskLedger.DefaultCap,
                arenaCap: fault is "run write" or "merge" or "scratch deletion" ? 4096 : TailReducer.DefaultArenaCap,
                publishFault: point => { if ((fault == "move" && point == "move") || (fault == "rename" && point == "rename") || (fault == "hint write" && point == "hint")) throw new IOException($"{point} fault"); },
                damage: fault == "validation" ? path => File.WriteAllBytes(path, File.ReadAllBytes(path).Take(100).ToArray()) : null,
                chunkSize: 4096,
                afterChunk: fault == "upload cancelled" ? chunk => cancellation.Cancel() : null,
                cancellationToken: cancellation.Token,
                scratch: fault == "scratch deletion" ? DeletesFail() : null,
                buildFault: point => { if (point == fault) throw new IOException($"{point} fault"); });

            if (fault == "scratch deletion")
            {
                // A failed deletion never fails the job; its charges stay on the pending list until a retry deletes the files
                Assert.Equal(BuildStatus.Completed, run.Result!.Status);
                Assert.True(run.Scratch.PendingCount > 0);
                Assert.Equal(run.Scratch.PendingBytes, run.Scratch.Ledger.Reserved);
                run.Scratch.DeleteFailsForTests = null;
                run.Scratch.RetryPending();
                Assert.Equal(0, run.Scratch.Ledger.Reserved);
            }
            else
            {
                Assert.True(run.Error != null || run.Result!.Status is BuildStatus.Failed or BuildStatus.Declined,
                    $"{kind} {fault}: {run.Result?.Status} {run.Result?.Reason} {run.Error?.Message}");
                if (fault == "local write")
                {
                    // Once the root fitted, the tail's spill or the output's growth reaches the cap; the writer's opening charge has tests of its own
                    Assert.Equal(MaintenanceReason.DiskCap, run.Result!.Reason);
                }
                if (fault is "run write" or "merge" or "writer close")
                {
                    Assert.Equal($"{fault} fault", run.Error?.Message);
                }
                // Scratch is deleted and every charge released
                Assert.Equal(0, run.Scratch.Ledger.Reserved);
                Assert.Equal(0, run.Scratch.PendingCount);
            }

            if (hooks != null)
            {
                hooks.Before = null;
                Assert.DoesNotContain(hooks.Requests, x => x.StartsWith("Rm "));
            }
            // Local disk never shows a partial checkpoint; elsewhere a store may, but no hint names one
            if (setup.Root != null)
            {
                await AssertCompleteOrAbsent(setup.Storage, 6);
            }
            if (await Hint(setup.Storage) is { Version: 6 })
            {
                Assert.Contains(await setup.Storage.Ls("/t/_delta_log/"), x => x.Name == $"{6:D20}.checkpoint.parquet");
                await AssertCompleteOrAbsent(setup.Storage, 6);
            }
            if (setup.Root != null)
            {
                // No temp file is left beside the log
                Assert.Empty(Directory.GetFiles(Path.Combine(setup.Root, "t", "_delta_log"), ".flowtide-*"));
            }
        }

        private ScratchDirectory DeletesFail()
        {
            return _owners.Add(new ScratchDirectory(Path.Combine(_directory, ScratchOwners.ShortId())) { DeleteFailsForTests = _ => true });
        }

        [Theory]
        [InlineData("memory")]
        [InlineData("commit-on-dispose")]
        [InlineData("local")]
        public async Task PublishedCheckpointEqualsTheOracleAndTheHintNamesIt(string kind)
        {
            var setup = Storage(kind);
            await Table(setup.Storage);
            var run = await Build(setup, 6);

            Assert.Equal(BuildStatus.Completed, run.Result!.Status);
            var hint = (await Hint(setup.Storage))!;
            Assert.Equal(6, hint.Version);
            Assert.Equal(run.Result.Checkpoint!.SizeInBytes, hint.SizeInBytes);
            Assert.Equal(run.Result.Checkpoint.Actions, hint.Size);
            Assert.Equal(run.Result.Checkpoint.AddFiles, hint.NumOfAddFiles);
            var table = (await DeltaTransactionReader.ReadTable(setup.Storage, "t"))!;
            Assert.Equal(6, table.StartCheckpointVersion);
            Assert.Equal(600, table.AddFiles.Count);
            Assert.Equal(0, run.Scratch.Ledger.Reserved);
        }

        [Fact]
        public async Task FailedUploadCommitsNothing()
        {
            // An object store commits on dispose, so an upload failing between chunks must be abandoned, never disposed
            var setup = Storage("commit-on-dispose");
            await Table(setup.Storage);
            var run = await Build(setup, 6, chunkSize: 4096, afterChunk: chunk => throw new IOException("network"));

            Assert.NotNull(run.Error);
            Assert.DoesNotContain(await setup.Storage.Ls("/t/_delta_log/"), x => x.Name == $"{6:D20}.checkpoint.parquet");
            Assert.Equal(3, (await Hint(setup.Storage))!.Version);
        }

        [Fact]
        public async Task HintIsWrittenOnlyAfterTheSizeMatches()
        {
            var inner = Files.Of.InternalMemory($"./publish-{Guid.NewGuid():N}");
            var hooks = new HookFileStorage(inner);
            await Table(hooks);
            // The store holds one byte more than was written when the size is checked
            var grown = 0;
            hooks.Before = async (verb, path) =>
            {
                if (verb == "Stat" && path.Full.EndsWith($"{6:D20}.checkpoint.parquet") && Interlocked.Exchange(ref grown, 1) == 0)
                {
                    var bytes = await DeltaTestKit.ReadBytes(inner, path.Full);
                    await using var stream = await inner.OpenWrite(path);
                    await stream.WriteAsync(bytes.Append((byte)0).ToArray());
                }
            };
            var run = await Build(new Setup() { Storage = hooks, Hooks = hooks }, 6);

            Assert.Equal(1, grown);
            Assert.Equal(BuildStatus.Failed, run.Result!.Status);
            Assert.Equal(MaintenanceReason.PublishFailed, run.Result.Reason);
            Assert.Equal(3, (await Hint(hooks))!.Version);
        }

        [Fact]
        public async Task HintNeverMovesBackward()
        {
            var setup = Storage("memory");
            await Table(setup.Storage);
            await setup.Storage.WriteText("/t/_delta_log/_last_checkpoint", JsonSerializer.Serialize(new LastCheckpointInfo() { Version = 3, Size = 302 }));
            Assert.Equal(BuildStatus.Completed, (await Build(setup, 6)).Result!.Status);
            Assert.Equal(6, (await Hint(setup.Storage))!.Version);

            // A later build at a lower version leaves the newer hint alone
            Assert.Equal(BuildStatus.Completed, (await Build(setup, 5)).Result!.Status);
            Assert.Equal(6, (await Hint(setup.Storage))!.Version);
        }

        [Fact]
        public async Task HintAboveTargetIsNeverLowered()
        {
            var setup = Storage("memory");
            await Table(setup.Storage);
            await setup.Storage.WriteText("/t/_delta_log/_last_checkpoint", JsonSerializer.Serialize(new LastCheckpointInfo() { Version = 9, Size = 1 }));
            Assert.Equal(BuildStatus.Completed, (await Build(setup, 6)).Result!.Status);
            Assert.Equal(9, (await Hint(setup.Storage))!.Version);
        }

        [Fact]
        public async Task MismatchedSameVersionHintIsRepaired()
        {
            var setup = Storage("memory");
            await Table(setup.Storage);
            Assert.Equal(BuildStatus.Completed, (await Build(setup, 6)).Result!.Status);
            var correct = (await Hint(setup.Storage))!;
            await setup.Storage.WriteText("/t/_delta_log/_last_checkpoint", JsonSerializer.Serialize(new LastCheckpointInfo() { Version = 6, Size = 1, SizeInBytes = 2 }));

            // The checkpoint is already there, so the build only repairs the hint from the listing and the footer
            var run = await Build(setup, 6);
            Assert.Equal(BuildStatus.AlreadyValid, run.Result!.Status);
            var repaired = (await Hint(setup.Storage))!;
            Assert.Equal((6L, correct.Size, correct.SizeInBytes), (repaired.Version, repaired.Size, repaired.SizeInBytes));
        }

        [Fact]
        public async Task TornCheckpointIsRebuiltInPlace()
        {
            var setup = Storage("memory");
            await Table(setup.Storage);
            await setup.Storage.WriteText(CheckpointPath(6), "torn");
            var run = await Build(setup, 6);

            Assert.Equal(BuildStatus.Completed, run.Result!.Status);
            await AssertCompleteOrAbsent(setup.Storage, 6);
            Assert.Equal(6, (await Hint(setup.Storage))!.Version);
        }

        [Fact]
        public async Task UnsupportedCheckpointAtTargetIsNotRebuilt()
        {
            var setup = Storage("memory");
            await Table(setup.Storage);
            // A checkpoint with a sidecar row: unsupported, so it is neither used nor replaced
            await DeltaLogReplayTests.WriteClassicCheckpoint(setup.Storage, "t", 6, new Dictionary<string, (string Name, Apache.Arrow.Types.IArrowType Type)[]>(DeltaLogReplayTests.CheckpointColumns)
            {
                ["sidecar"] = new (string, Apache.Arrow.Types.IArrowType)[] { ("path", Apache.Arrow.Types.StringType.Default) }
            },
                ("protocol", new object?[] { 1, 2 }),
                ("metaData", new object?[] { "table-id", DeltaTestKit.UserSchema, 0L }),
                ("sidecar", new object?[] { "part.parquet" }));
            var before = await DeltaTestKit.ReadBytes(setup.Storage, CheckpointPath(6));
            var run = await Build(setup, 6);

            Assert.Equal(BuildStatus.Completed, run.Result!.Status);
            Assert.Null(run.Result.Checkpoint);
            Assert.Equal(before, await DeltaTestKit.ReadBytes(setup.Storage, CheckpointPath(6)));
            Assert.Equal(3, (await Hint(setup.Storage))!.Version);
        }

        [Fact]
        public async Task RootNoneNeverReusesOrOverwritesAValidCheckpoint()
        {
            var setup = Storage("memory");
            await Table(setup.Storage);
            Assert.Equal(BuildStatus.Completed, (await Build(setup, 6)).Result!.Status);
            var before = await DeltaTestKit.ReadBytes(setup.Storage, CheckpointPath(6));
            var run = await Build(setup, 6, root: RootPolicy.None);

            Assert.Equal(BuildStatus.Declined, run.Result!.Status);
            Assert.Equal(MaintenanceReason.CheckpointExists, run.Result.Reason);
            Assert.Equal(before, await DeltaTestKit.ReadBytes(setup.Storage, CheckpointPath(6)));
        }

        [Fact]
        public async Task ScratchCapDeclinesBeforeWriting()
        {
            var setup = Storage("memory");
            await Table(setup.Storage);
            var run = await Build(setup, 6, diskCap: 1024);

            Assert.Equal(BuildStatus.Declined, run.Result!.Status);
            Assert.Equal(MaintenanceReason.DiskCap, run.Result.Reason);
            Assert.True(run.Scratch.Ledger.Peak <= 1024);
            Assert.Equal(3, (await Hint(setup.Storage))!.Version);
        }

        [Fact]
        public async Task DestinationWithoutFreeSpaceDeclines()
        {
            var setup = Storage("local");
            await Table(setup.Storage);
            var run = await Build(setup, 6, freeSpace: _ => 10L << 20);

            Assert.Equal(BuildStatus.Declined, run.Result!.Status);
            Assert.Equal(MaintenanceReason.DiskCap, run.Result.Reason);
            Assert.False(File.Exists(Path.Combine(setup.Root!, "t", "_delta_log", $"{6:D20}.checkpoint.parquet")));
            Assert.Equal(0, run.Scratch.Ledger.Reserved);
        }

        [Fact]
        public async Task FailedFreeSpaceQueryReleasesTheAdjacentCharge()
        {
            // DriveInfo refuses a UNC root on Windows, and a drive that is not ready fails the query
            var setup = Storage("local");
            await Table(setup.Storage);
            var run = await Build(setup, 6, freeSpace: _ => throw new IOException("The device is not ready"));

            Assert.NotNull(run.Error);
            Assert.Equal(0, run.Scratch.Ledger.Reserved);
        }

        [Theory]
        [InlineData("move")]
        [InlineData("rename")]
        public async Task FailedLocalPublishDeletesItsTemp(string point)
        {
            var setup = Storage("local");
            await Table(setup.Storage);
            var run = await Build(setup, 6, publishFault: p => { if (p == point) throw new IOException($"{p} fault"); });

            Assert.IsType<IOException>(run.Error);
            Assert.Empty(Directory.GetFiles(Path.Combine(setup.Root!, "t", "_delta_log"), ".flowtide-*"));
            Assert.False(File.Exists(Path.Combine(setup.Root!, "t", "_delta_log", $"{6:D20}.checkpoint.parquet")));
            Assert.Equal(0, run.Scratch.Ledger.Reserved);
        }

        [Fact]
        public async Task UndeletableTempKeepsItsChargeUntilARetryDeletesIt()
        {
            var setup = Storage("local");
            await Table(setup.Storage);
            var scratch = _owners.Add(new ScratchDirectory(Path.Combine(_directory, ScratchOwners.ShortId())) { DeleteFailsForTests = path => path.EndsWith(".checkpoint.tmp") });
            var run = await Build(setup, 6, publishFault: p => { if (p == "rename") throw new IOException("rename fault"); }, scratch: scratch);

            Assert.IsType<IOException>(run.Error);
            var log = Path.Combine(setup.Root!, "t", "_delta_log");
            var temp = Assert.Single(Directory.GetFiles(log, ".flowtide-*"));
            Assert.Equal(new FileInfo(temp).Length, scratch.PendingBytes);
            Assert.Equal(scratch.PendingBytes, scratch.Ledger.Reserved);

            scratch.DeleteFailsForTests = null;
            scratch.RetryPending();
            Assert.Equal(0, scratch.Ledger.Reserved);
            Assert.Empty(Directory.GetFiles(log, ".flowtide-*"));
        }

        [Fact]
        public async Task AdjacentCopyIsReservedBeforeTheMove()
        {
            var setup = Storage("local");
            await Table(setup.Storage);
            var scratch = _owners.Add(new ScratchDirectory(Path.Combine(_directory, ScratchOwners.ShortId())));
            var memory = new JobMemory();
            using var job = scratch.CreateJob();
            var built = await CheckpointBuilder.BuildAsync(new CheckpointBuildContext()
            {
                Storage = setup.Storage,
                Table = "t",
                Scratch = job,
                Memory = memory,
                Clock = new FakeTimeProvider(s_now),
                Publish = false
            }, new BuildRequest() { Version = 6 });
            var checkpoint = built.Checkpoint!;
            // Another job holds all but too little of the ledger for this output's adjacent copy
            scratch.Ledger.Reserve(scratch.Ledger.Cap - scratch.Ledger.Reserved - checkpoint.SizeInBytes + 1);

            var declined = await Assert.ThrowsAsync<CheckpointDeclinedException>(() => CheckpointPublisher.PublishAsync(new StowageCheckpointTransport(setup.Storage, "t"), job, memory, checkpoint, null, default));
            Assert.Equal(MaintenanceReason.DiskCap, declined.Reason);
            // Nothing moved: the output is still in scratch, and the log holds neither a temp nor the checkpoint
            Assert.True(File.Exists(checkpoint.LocalPath));
            Assert.Empty(Directory.GetFiles(Path.Combine(setup.Root!, "t", "_delta_log"), ".flowtide-*"));
            Assert.False(File.Exists(Path.Combine(setup.Root!, "t", "_delta_log", $"{6:D20}.checkpoint.parquet")));
        }

        [Fact]
        public async Task OutputFailingTheScanIsNotPublished()
        {
            var setup = Storage("memory");
            await Table(setup.Storage);
            var run = await Build(setup, 6, damage: path => File.WriteAllBytes(path, File.ReadAllBytes(path).Take(100).ToArray()));

            Assert.Equal(BuildStatus.Failed, run.Result!.Status);
            Assert.Equal(MaintenanceReason.ValidationFailed, run.Result.Reason);
            Assert.DoesNotContain(await setup.Storage.Ls("/t/_delta_log/"), x => x.Name == $"{6:D20}.checkpoint.parquet");
        }

        [Fact]
        public void DiskChargeIsReleasedOnlyAfterDeletion()
        {
            var scratch = _owners.Add(new ScratchDirectory(Path.Combine(_directory, ScratchOwners.ShortId()), 100_000));
            var job = scratch.CreateJob();
            var path = job.NewPath(".bin");
            using (var stream = job.Create(path))
            {
                stream.Write(new byte[80_000]);
            }
            // While the deletion is held the file still exists, so a request that fits only after the release declines
            var declined = false;
            scratch.DeleteFailsForTests = _ =>
            {
                Assert.True(File.Exists(path));
                declined = Assert.Throws<CheckpointDeclinedException>(() => scratch.Ledger.Reserve(50_000)) != null;
                return false;
            };
            Assert.True(job.Delete(path));
            Assert.True(declined);
            Assert.Equal(0, scratch.Ledger.Reserved);
            scratch.Ledger.Reserve(50_000);
        }

        [Fact]
        public void FailedScratchDeletionKeepsItsCharge()
        {
            var scratch = _owners.Add(new ScratchDirectory(Path.Combine(_directory, ScratchOwners.ShortId()), 100_000) { DeleteFailsForTests = _ => true });
            var job = scratch.CreateJob();
            var path = job.NewPath(".bin");
            using (var stream = job.Create(path))
            {
                stream.Write(new byte[80_000]);
            }
            Assert.False(job.Delete(path));
            Assert.Equal(80_000, scratch.Ledger.Reserved);
            Assert.Equal(1, scratch.PendingCount);
            Assert.Throws<CheckpointDeclinedException>(() => scratch.Ledger.Reserve(50_000));

            // The next admitted job retries the list before it reserves anything
            scratch.DeleteFailsForTests = null;
            scratch.CreateJob().Dispose();
            Assert.False(File.Exists(path));
            Assert.Equal(0, scratch.PendingCount);
            Assert.Equal(0, scratch.Ledger.Reserved);
        }

        [Fact]
        public async Task PendingDeletionIsClaimedOnce()
        {
            var scratch = _owners.Add(new ScratchDirectory(Path.Combine(_directory, ScratchOwners.ShortId())));
            var job = scratch.CreateJob();
            var paths = new List<string>();
            for (int i = 0; i < 64; i++)
            {
                var path = job.NewPath(".bin");
                using (var stream = job.Create(path))
                {
                    stream.Write(new byte[1000]);
                }
                paths.Add(path);
            }
            scratch.DeleteFailsForTests = _ => true;
            foreach (var path in paths)
            {
                job.Delete(path);
            }
            // The first retry claims the list and is held in its first deletion while a second retry runs
            using var held = new ManualResetEventSlim();
            using var release = new ManualResetEventSlim();
            var calls = 0;
            scratch.DeleteFailsForTests = _ =>
            {
                if (Interlocked.Increment(ref calls) == 1)
                {
                    held.Set();
                    release.Wait();
                }
                return false;
            };
            var first = Task.Run(scratch.RetryPending);
            held.Wait();
            Assert.Equal(0, scratch.RetryPending());
            release.Set();
            Assert.Equal(64, await first);
            // Each charge was released exactly once
            Assert.Equal(0, scratch.Ledger.Reserved);
            Assert.Equal(0, scratch.PendingCount);
        }

        [Fact]
        public async Task TransferAllowanceRunsALargeObjectAlone()
        {
            var allowance = new TransferAllowance(100);
            var small = await allowance.AdmitAsync(60);
            var large = allowance.AdmitAsync(500);
            var queuedSmall = allowance.AdmitAsync(10);
            Assert.False(large.IsCompleted);
            // Arrival order: the small request waits behind the large one
            Assert.False(queuedSmall.IsCompleted);
            small.Dispose();
            var largeCharge = await large;
            Assert.Equal(500, allowance.Charged);
            Assert.False(queuedSmall.IsCompleted);
            largeCharge.Dispose();
            (await queuedSmall).Dispose();
            Assert.Equal(0, allowance.Charged);
        }
    }
}
