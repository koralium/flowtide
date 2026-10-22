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

using FlowtideDotNet.Connector.DeltaLake.Internal.Delta.Actions;
using FlowtideDotNet.Connector.DeltaLake.Internal.Maintenance;
using FlowtideDotNet.Connector.DeltaLake.Internal.Maintenance.Deletion;
using FlowtideDotNet.Connector.DeltaLake.Internal.Maintenance.References;
using Microsoft.Extensions.Time.Testing;
using Stowage;
using Stowage.Impl;
using System.Collections.Concurrent;
using System.Diagnostics;
using System.Runtime.InteropServices;
using System.Text;
using Xunit.Abstractions;

namespace FlowtideDotNet.Connector.DeltaLake.Tests
{
    [Collection(nameof(PinnedClientCollection))]
    public partial class DeletionExecutorTests : IDisposable
    {
        private static readonly DateTimeOffset s_now = new DateTimeOffset(2026, 10, 15, 12, 0, 0, TimeSpan.Zero);
        private static readonly TimeSpan s_wait = TimeSpan.FromSeconds(30);

        private readonly string _directory = Path.Combine(Path.GetTempPath(), "dx-" + ScratchOwners.ShortId());
        private readonly ScratchOwners _owners = new ScratchOwners();
        private readonly List<IDisposable> _disposables = new List<IDisposable>();
        // Links are removed before the folders, so cleanup never follows one
        private readonly List<string> _links = new List<string>();
        private readonly ITestOutputHelper _output;

        public DeletionExecutorTests(ITestOutputHelper output)
        {
            _output = output;
            Directory.CreateDirectory(_directory);
        }

        public void Dispose()
        {
            foreach (var disposable in _disposables)
            {
                disposable.Dispose();
            }
            _owners.Release();
            foreach (var link in _links)
            {
                try
                {
                    if (Directory.Exists(link))
                    {
                        Directory.Delete(link);
                    }
                    else
                    {
                        File.Delete(link);
                    }
                }
                catch (IOException)
                {
                }
                catch (UnauthorizedAccessException)
                {
                }
            }
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

        // An eligible local disk table seen through a connector-owned test view and the maintenance meter
        private sealed class LocalTable
        {
            public required string Root { get; init; }

            public required string TableDir { get; init; }

            public required IFileStorage Local { get; init; }

            public required TestStorage Owned { get; init; }

            public required MaintenanceBuckets Buckets { get; init; }

            public required MeteredFileStorage Metered { get; init; }

            public required CleanupConfiguration Configuration { get; init; }

            public required DeletionExecutor Executor { get; init; }

            public string Native(string relative) => Path.Combine(TableDir, relative.Replace('/', Path.DirectorySeparatorChar));

            public DeletionTarget Target(string relative, TargetKind kind = TargetKind.Payload) => new DeletionTarget(Native(relative), kind);

            public string Put(string relative, string content = "x")
            {
                var path = Native(relative);
                Directory.CreateDirectory(Path.GetDirectoryName(path)!);
                File.WriteAllText(path, content);
                return path;
            }
        }

        private LocalTable Local(string table = "t", string? root = null)
        {
            root ??= Path.Combine(_directory, ScratchOwners.ShortId());
            Directory.CreateDirectory(root);
            var local = Files.Of.LocalDisk(root);
            _disposables.Add(local);
            var configuration = CleanupConfiguration.Validate(local, table, false, CleanupOptIns.Off);
            Assert.True(configuration.Store.Eligible);
            var owned = new LocalTestStorage(local);
            var buckets = new MaintenanceBuckets(TimeProvider.System);
            var metered = MeteredFileStorage.Wrap(owned, buckets, ListingPaging.LocalDisk);
            var tableDir = Path.Combine(root, table.Replace('/', Path.DirectorySeparatorChar));
            Directory.CreateDirectory(tableDir);
            return new LocalTable()
            {
                Root = root,
                TableDir = tableDir,
                Local = local,
                Owned = owned,
                Buckets = buckets,
                Metered = metered,
                Configuration = configuration,
                Executor = new DeletionExecutor(configuration, metered)
            };
        }

        // An eligible table at c/t on a loopback store, through the real pinned client and the maintenance meter
        private sealed class CloudTable
        {
            public required LoopbackObjectStore Server { get; init; }

            public required IFileStorage Client { get; init; }

            public required TestStorage Owned { get; init; }

            public required MaintenanceBuckets Buckets { get; init; }

            public required MeteredFileStorage Metered { get; init; }

            public required CleanupConfiguration Configuration { get; init; }

            public required DeletionExecutor Executor { get; init; }

            public DeletionTarget Target(string relative, TargetKind kind = TargetKind.Payload) => new DeletionTarget("/c/t/" + relative, kind);

            public void ClearRequests()
            {
                while (Server.Requests.TryDequeue(out _))
                {
                }
            }
        }

        private CloudTable Cloud(LoopbackObjectStore server)
        {
            var client = server.CreateStorage();
            _disposables.Add(client);
            var configuration = CleanupConfiguration.Validate(client, "c/t", false, CleanupOptIns.Off);
            Assert.True(configuration.Store.Eligible);
            var owned = new TestStorage(client);
            var buckets = new MaintenanceBuckets(TimeProvider.System);
            var metered = MeteredFileStorage.Wrap(owned, buckets, configuration.Store.Store.Paging);
            return new CloudTable()
            {
                Server = server,
                Client = client,
                Owned = owned,
                Buckets = buckets,
                Metered = metered,
                Configuration = configuration,
                Executor = new DeletionExecutor(configuration, metered)
            };
        }

        // A table at c/t classified as pinned S3 over scripted objects, so runs need no service
        private sealed class ScriptedTable
        {
            public required ScriptedStorage Objects { get; init; }

            public required TestStorage Owned { get; init; }

            public required MaintenanceBuckets Buckets { get; init; }

            public required MeteredFileStorage Metered { get; init; }

            public required CleanupConfiguration Configuration { get; init; }

            public required DeletionExecutor Executor { get; init; }

            public DeletionTarget Target(string relative, TargetKind kind = TargetKind.Payload) => new DeletionTarget("/c/t/" + relative, kind);

            public void Put(params string[] relatives)
            {
                foreach (var relative in relatives)
                {
                    Objects.Put("/c/t/" + relative);
                }
            }

            public async Task<bool> Contains(string relative) => await Objects.Stat("/c/t/" + relative) != null;
        }

        private ScriptedTable Scripted(bool azure = false)
        {
            var objects = new ScriptedStorage();
            using var pinned = azure ? StoreFixtures.Azure() : StoreFixtures.S3();
            var configuration = CleanupConfiguration.Validate(CleanupStoreContract.Create(objects, StoreDescriptor.Of(pinned), "c/t", false), CleanupOptIns.Off);
            Assert.True(configuration.Store.Eligible);
            var owned = new TestStorage(objects);
            var buckets = new MaintenanceBuckets(TimeProvider.System);
            var metered = MeteredFileStorage.Wrap(owned, buckets, ListingPaging.S3);
            return new ScriptedTable()
            {
                Objects = objects,
                Owned = owned,
                Buckets = buckets,
                Metered = metered,
                Configuration = configuration,
                Executor = new DeletionExecutor(configuration, metered)
            };
        }

        private static JobRun Run(JobKind kind = JobKind.Deletion) => new JobRun(0, kind) { Cancellation = new CancellationTokenSource() };

        private CleanupContext Context(JobKind kind = JobKind.Deletion, JobRun? run = null, TimeProvider? clock = null, CancellationToken stop = default, JobMemory? memory = null, long head = -1)
        {
            clock ??= new FakeTimeProvider(s_now);
            var owner = _owners.Add(new ScratchDirectory(Path.Combine(_directory, ScratchOwners.ShortId())));
            var scratch = owner.CreateJob();
            _disposables.Add(scratch);
            return new CleanupContext(run ?? Run(kind), clock)
            {
                Kind = kind,
                Long = false,
                ReadLimit = MaintenanceWorkerSettings.DefaultShortReads,
                Deadline = clock.GetUtcNow() + TimeSpan.FromMinutes(15),
                Head = head,
                TableId = null,
                Scratch = scratch,
                Memory = memory ?? new JobMemory(),
                Transfer = new TransferAllowance(),
                Stop = stop
            };
        }

        private static DeltaAction Own(long version) => new DeltaAction()
        {
            CommitInfo = new DeltaCommitInfoAction() { StageId = $"stage-{version}", AdoptedAt = 0, Timestamp = 1000 + version, Operation = "WRITE" }
        };

        private static DeltaAction Foreign(long version) => new DeltaAction()
        {
            CommitInfo = new DeltaCommitInfoAction() { Timestamp = 1000 + version, Operation = "WRITE" }
        };

        // Own commits 0 to head of one epoch: the table at 0, then one added file per commit
        private static async Task Create(IFileStorage storage, string table, long head)
        {
            var metaData = DeltaTestKit.Metadata(DeltaTestKit.UserSchema);
            metaData.MetaData!.Id = "table-a";
            await DeltaTestKit.WriteCommit(storage, table, 0, Own(0), DeltaTestKit.Protocol(), metaData);
            for (long version = 1; version <= head; version++)
            {
                await DeltaTestKit.WriteCommit(storage, table, version, Own(version), DeltaLakeCheckpointBuildTests.Add($"part-{version:D4}.parquet"));
            }
        }

        // Evidence anchored at an own commit, established through the given storage
        private static async Task<CleanupEvidence> Establish(CleanupConfiguration configuration, IFileStorage storage, long anchor, bool durable, CleanupContext context)
        {
            var (check, log) = await ListingEvidence.ReadLogAsync(configuration, storage, -1, default);
            Assert.Equal(ListingCheck.Passed, check);
            var result = await CleanupEvidence.EstablishAsync(configuration, storage, log!, anchor, null, durable, context);
            Assert.True(result.Passed, $"{result.Verdict}: {result.Detail}");
            return result.Evidence!;
        }

        // A gate that passes on any checked stage listing, so a run's own behavior is visible
        private static ChunkGate OpenGate(CleanupConfiguration configuration, IFileStorage storage)
        {
            return ChunkGate.ForStages(configuration, storage, (_, _) => Task.FromResult(new GateResult(GateVerdict.Passed)));
        }

        // A directory link: a symbolic link where the process may create one, otherwise a Windows junction; false when neither can be made
        private bool TryLinkDirectory(string link, string target)
        {
            try
            {
                Directory.CreateSymbolicLink(link, target);
                _links.Add(link);
                return true;
            }
            catch (IOException)
            {
            }
            catch (UnauthorizedAccessException)
            {
            }
            if (!OperatingSystem.IsWindows())
            {
                return false;
            }
            var start = new ProcessStartInfo("cmd.exe") { UseShellExecute = false, RedirectStandardOutput = true, RedirectStandardError = true, CreateNoWindow = true };
            start.ArgumentList.Add("/c");
            start.ArgumentList.Add("mklink");
            start.ArgumentList.Add("/J");
            start.ArgumentList.Add(link);
            start.ArgumentList.Add(target);
            using var process = Process.Start(start)!;
            process.StandardOutput.ReadToEnd();
            process.StandardError.ReadToEnd();
            process.WaitForExit();
            if (process.ExitCode != 0 || !Directory.Exists(link))
            {
                return false;
            }
            _links.Add(link);
            return true;
        }

        private bool TryLinkFile(string link, string target)
        {
            try
            {
                File.CreateSymbolicLink(link, target);
                _links.Add(link);
                return true;
            }
            catch (IOException)
            {
                return false;
            }
            catch (UnauthorizedAccessException)
            {
                return false;
            }
        }

        [Fact]
        public async Task ExecutorNeverDeletesDirectories()
        {
            var table = Local();
            // A directory where a payload file is expected, a folder-shaped key and a folder placeholder
            table.Put("part-1.parquet/inner.bin");
            table.Put("dir/inner.bin");
            table.Put("x_$folder$", "");

            Assert.Equal(DeleteOutcome.Keep(KeepReason.Directory), await table.Executor.DeleteAsync(table.Target("part-1.parquet"), false, default));
            var folderKey = table.Native("dir") + Path.DirectorySeparatorChar;
            Assert.Equal(DeleteOutcome.Keep(KeepReason.Folder), await table.Executor.DeleteAsync(new DeletionTarget(folderKey, TargetKind.Payload), false, default));
            Assert.Equal(KeepReason.Folder, table.Executor.TryTarget("x_$folder$", TargetKind.Payload, out _));
            Assert.Equal(DeleteOutcome.Keep(KeepReason.Folder), await table.Executor.DeleteAsync(table.Target("x_$folder$"), false, default));

            Assert.True(File.Exists(table.Native("part-1.parquet/inner.bin")));
            Assert.True(File.Exists(table.Native("dir/inner.bin")));
            Assert.True(File.Exists(table.Native("x_$folder$")));
            Assert.Equal(3, table.Executor.Tally.Kept);
            Assert.Equal(0, table.Executor.Tally.Deleted);

            // On the object stores a folder key never reaches the client, whose Rm deletes a whole prefix
            foreach (var flavor in new[] { StoreFlavor.S3, StoreFlavor.Azure })
            {
                await using var server = new LoopbackObjectStore(flavor);
                var cloud = Cloud(server);
                server.Put("t/sacrificial/one.bin");
                await cloud.Client.Rm("/c/t/sacrificial/");
                Assert.False(server.Contains("t/sacrificial/one.bin"));
                server.Put("t/dir/one.bin");
                server.Put("t/dir/two.bin");
                cloud.ClearRequests();

                Assert.Equal(KeepReason.Unqualified, cloud.Executor.TryTarget("dir/", TargetKind.Payload, out _));
                Assert.Equal(DeleteOutcome.Keep(KeepReason.Unqualified), await cloud.Executor.DeleteAsync(cloud.Target("dir/"), false, default));

                Assert.Empty(server.Requests);
                Assert.True(server.Contains("t/dir/one.bin"));
                Assert.True(server.Contains("t/dir/two.bin"));
            }
        }

        [Fact]
        public async Task ExecutorRejectsReparseRootAncestorsAndLeaf()
        {
            var outside = Path.Combine(_directory, "outside-" + ScratchOwners.ShortId());
            Directory.CreateDirectory(Path.Combine(outside, "root"));
            Directory.CreateDirectory(Path.Combine(outside, "sub"));
            Directory.CreateDirectory(Path.Combine(outside, "leaf"));
            File.WriteAllText(Path.Combine(outside, "root", "part-1.parquet"), "r");
            File.WriteAllText(Path.Combine(outside, "sub", "part-2.parquet"), "s");
            File.WriteAllText(Path.Combine(outside, "leaf.parquet"), "l");

            // Windows can always make a junction; elsewhere a file system that takes no symbolic link is the only reason to skip
            var probe = Path.Combine(_directory, "probe-" + ScratchOwners.ShortId());
            if (!OperatingSystem.IsWindows() && !TryLinkDirectory(probe, outside))
            {
                _output.WriteLine("SKIPPED: this file system creates no directory link");
                return;
            }

            // The table root itself is a link to another directory
            var linkedRoot = Path.Combine(_directory, ScratchOwners.ShortId());
            Directory.CreateDirectory(linkedRoot);
            Assert.True(TryLinkDirectory(Path.Combine(linkedRoot, "t"), Path.Combine(outside, "root")), "No link at the table root");
            var table = Local(root: linkedRoot);
            Assert.Equal(DeleteOutcome.Keep(KeepReason.ReparsePoint), await table.Executor.DeleteAsync(table.Target("part-1.parquet"), false, default));
            Assert.True(File.Exists(Path.Combine(outside, "root", "part-1.parquet")));

            var plain = Local();
            plain.Put("part-4.parquet");
            // A directory between the root and the target is a link
            Assert.True(TryLinkDirectory(plain.Native("sub"), Path.Combine(outside, "sub")), "No link between the root and the target");
            Assert.Equal(DeleteOutcome.Keep(KeepReason.ReparsePoint), await plain.Executor.DeleteAsync(plain.Target("sub/part-2.parquet"), false, default));
            Assert.True(File.Exists(Path.Combine(outside, "sub", "part-2.parquet")));

            // The target itself is a link: a file link where the process may create one, otherwise a directory link
            Assert.True(TryLinkFile(plain.Native("part-3.parquet"), Path.Combine(outside, "leaf.parquet")) || TryLinkDirectory(plain.Native("part-3.parquet"), Path.Combine(outside, "leaf")), "No link at the target");
            Assert.Equal(DeleteOutcome.Keep(KeepReason.ReparsePoint), await plain.Executor.DeleteAsync(plain.Target("part-3.parquet"), false, default));
            Assert.True(File.Exists(plain.Native("part-3.parquet")) || Directory.Exists(plain.Native("part-3.parquet")));
            Assert.True(File.Exists(Path.Combine(outside, "leaf.parquet")));
            Assert.Equal(3, table.Executor.Tally.KeptFor(KeepReason.ReparsePoint) + plain.Executor.Tally.KeptFor(KeepReason.ReparsePoint));

            // An ordinary file in the same table is deleted
            Assert.Equal(DeleteStatus.Deleted, (await plain.Executor.DeleteAsync(plain.Target("part-4.parquet"), false, default)).Status);
            Assert.False(File.Exists(plain.Native("part-4.parquet")));
        }

        [Fact]
        public async Task PathKeyBridgesToATargetOnLocalDiskAndObjectStores()
        {
            var table = Local();
            table.Put("part-1.parquet");
            table.Put("dir/part-2.parquet");
            var identity = PathCanonicalizer.Create(table.Configuration.Store, table.Metered);
            var key = identity.Name("part-1.parquet", PathRole.Candidate);
            Assert.Equal("/t/part-1.parquet", key.DeletionPath);
            // The deletion path is a store path, not the native key local disk deletes by
            Assert.Equal(KeepReason.Unqualified, table.Executor.Check(new DeletionTarget(key.DeletionPath!, TargetKind.Payload)));
            var target = table.Executor.TargetOf(key);
            Assert.Equal(new DeletionTarget(table.Native("part-1.parquet"), TargetKind.Payload), target);
            Assert.Equal(DeleteStatus.Deleted, (await table.Executor.DeleteAsync(target, false, default)).Status);
            Assert.False(File.Exists(table.Native("part-1.parquet")));

            // A candidate read back from a path set difference, as a run's source reads it
            var context = Context();
            using var candidates = new PathSet(context.Scratch, context.Memory, identity.Domain, PathRole.Candidate);
            using var protection = new PathSet(context.Scratch, context.Memory, identity.Domain, PathRole.Protection);
            Assert.True(new CommitReferenceExtractor(identity, candidates, PathRole.Candidate).AddName("dir/part-2.parquet"));
            candidates.Seal();
            protection.Seal();
            using (var difference = PathSet.Difference(candidates, protection))
            {
                Assert.True(difference.MoveNext());
                Assert.Equal(DeleteStatus.Deleted, (await table.Executor.DeleteAsync(table.Executor.TargetOf(difference.DeletionPathText), false, default)).Status);
            }
            Assert.False(File.Exists(table.Native("dir/part-2.parquet")));

            // A protection key or a path outside the table names no target
            Assert.Equal(KeepReason.Unqualified, table.Executor.Check(table.Executor.TargetOf(identity.Name("part-3.parquet", PathRole.Protection))));
            Assert.Equal(KeepReason.Unqualified, table.Executor.Check(table.Executor.TargetOf("/u/part-1.parquet")));

            // On an object store the deletion path is the key itself
            var cloud = Scripted(azure: true);
            var cloudKey = PathCanonicalizer.Create(cloud.Configuration.Store).Name("part-1.parquet", PathRole.Candidate);
            Assert.Equal(cloud.Target("part-1.parquet"), cloud.Executor.TargetOf(cloudKey));
            Assert.Equal(cloud.Target("_delta_log/x.json", TargetKind.Control), cloud.Executor.TargetOf("/c/t/_delta_log/x.json", TargetKind.Control));
        }

        [Fact]
        public async Task DirectorySubstitutionCannotRecurse()
        {
            var table = Local();
            table.Put("part-1.parquet");
            // The client's own Rm deletes a directory with everything in it
            table.Put("sacrificial/a.bin");
            await table.Local.Rm("/t/sacrificial");
            Assert.False(Directory.Exists(table.Native("sacrificial")));

            // Between the checks and the delete the file becomes a directory holding a protected file
            table.Executor.BeforeDeleteForTests = target =>
            {
                File.Delete(target.Key);
                Directory.CreateDirectory(target.Key);
                File.WriteAllText(Path.Combine(target.Key, "protected.bin"), "p");
                return Task.CompletedTask;
            };
            var outcome = await table.Executor.DeleteAsync(table.Target("part-1.parquet"), false, default);

            Assert.Equal(DeleteStatus.Failed, outcome.Status);
            Assert.NotNull(outcome.Error);
            Assert.True(File.Exists(table.Native("part-1.parquet/protected.bin")));
            Assert.Equal(1, table.Executor.Tally.Failed);
        }

        [Fact]
        public async Task AssertedUnknownStackNeverDeletesAFolder()
        {
            var root = Path.Combine(_directory, ScratchOwners.ShortId());
            var tableDir = Path.Combine(root, "t");
            Directory.CreateDirectory(tableDir);
            var local = Files.Of.LocalDisk(root);
            _disposables.Add(local);
            // A user decorator over local disk, vouched for; the connector cannot see that its Rm recurses
            var wrapper = new ForwardingStorage(local);
            var configuration = CleanupConfiguration.Validate(wrapper, "t", true, CleanupOptIns.Off);
            Assert.Equal(StoreKind.Unknown, configuration.Store.Store.Kind);
            Assert.True(configuration.Store.Eligible);
            var owned = new TestStorage(wrapper);
            var buckets = new MaintenanceBuckets(TimeProvider.System);
            var executor = new DeletionExecutor(configuration, MeteredFileStorage.Wrap(owned, buckets, ListingPaging.S3));
            Directory.CreateDirectory(Path.Combine(tableDir, "dir.parquet"));
            File.WriteAllText(Path.Combine(tableDir, "dir.parquet", "protected.bin"), "p");
            File.WriteAllText(Path.Combine(tableDir, "part.parquet"), "x");
            File.WriteAllText(Path.Combine(tableDir, "empty.parquet"), "");

            // A name the store holds as a directory is never removed
            Assert.Equal(KeepReason.None, executor.TryTarget("dir.parquet", TargetKind.Payload, out var folder));
            // Stowage's local disk Stat throws for a directory, which fails the delete before any Rm
            var kept = await executor.DeleteAsync(folder, false, default);
            Assert.NotEqual(DeleteStatus.Deleted, kept.Status);
            Assert.True(File.Exists(Path.Combine(tableDir, "dir.parquet", "protected.bin")));
            // Nor is a file without a positive size
            Assert.Equal(KeepReason.None, executor.TryTarget("empty.parquet", TargetKind.Payload, out var empty));
            Assert.Equal(DeleteOutcome.Keep(KeepReason.UnknownSize), await executor.DeleteAsync(empty, false, default));
            Assert.True(File.Exists(Path.Combine(tableDir, "empty.parquet")));
            // A missing object is already absent
            Assert.Equal(KeepReason.None, executor.TryTarget("gone.parquet", TargetKind.Payload, out var gone));
            Assert.Equal(DeleteStatus.Absent, (await executor.DeleteAsync(gone, false, default)).Status);
            Assert.DoesNotContain(owned.Calls, x => x.StartsWith("Rm ", StringComparison.Ordinal));

            // An existing file of positive size is deleted after its charged Stat
            Assert.Equal(KeepReason.None, executor.TryTarget("part.parquet", TargetKind.Payload, out var file));
            Assert.Equal(DeleteStatus.Deleted, (await executor.DeleteAsync(file, false, default)).Status);
            Assert.False(File.Exists(Path.Combine(tableDir, "part.parquet")));
            Assert.Equal(new[] { "Stat /t/dir.parquet", "Stat /t/empty.parquet", "Stat /t/gone.parquet", "Stat /t/part.parquet", "Rm /t/part.parquet" }, owned.Calls);
            Assert.Equal(5, buckets.Requests.Charged);

            // A stack whose Stat reports a folder, or a file without a known size
            var objects = new ScriptedStorage();
            objects.Put("/t/folder.parquet");
            objects.Put("/t/unsized.parquet");
            objects.StatAnswer = (path, entry) => path.Name switch
            {
                "folder.parquet" => new IOEntry(path.Full + "/"),
                "unsized.parquet" => new IOEntry(path.Full),
                _ => entry
            };
            var scripted = new ForwardingStorage(objects);
            var scriptedExecutor = new DeletionExecutor(CleanupConfiguration.Validate(scripted, "t", true, CleanupOptIns.Off), scripted);
            Assert.Equal(DeleteOutcome.Keep(KeepReason.Directory), await scriptedExecutor.DeleteAsync(new DeletionTarget("/t/folder.parquet", TargetKind.Payload), false, default));
            Assert.Equal(DeleteOutcome.Keep(KeepReason.UnknownSize), await scriptedExecutor.DeleteAsync(new DeletionTarget("/t/unsized.parquet", TargetKind.Payload), false, default));
            Assert.DoesNotContain(objects.Requests, x => x.StartsWith("Rm ", StringComparison.Ordinal));
        }

        // Protected objects whose names begin retired keys: a data file before a '#', and a deletion vector folder named like a retired file
        private async Task RetiredHashKeyCannotDeleteProtectedPrefix(StoreFlavor flavor)
        {
            await using var server = new LoopbackObjectStore(flavor);
            var cloud = Cloud(server);
            server.Put("t/part-1.parquet");
            server.Put("t/part-1.parquet#x");
            server.Put("t/ab/deletion_vector_1.bin");
            server.Put("t/ab");
            // The client sends '#' unencoded, so the raw delete of a retired key removes the protected name before it
            server.Put("t/hazard");
            server.Put("t/hazard#x");
            await cloud.Client.Rm("/c/t/hazard#x");
            Assert.False(server.Contains("t/hazard"));
            Assert.True(server.Contains("t/hazard#x"));
            cloud.ClearRequests();

            Assert.Equal(KeepReason.Unqualified, cloud.Executor.TryTarget("part-1.parquet#x", TargetKind.Payload, out _));
            Assert.Equal(KeepReason.Unqualified, cloud.Executor.TryTarget("ab/", TargetKind.Payload, out _));
            foreach (var retired in new[] { "part-1.parquet#x", "ab/", "ab#/deletion_vector_1.bin" })
            {
                Assert.Equal(DeleteOutcome.Keep(KeepReason.Unqualified), await cloud.Executor.DeleteAsync(cloud.Target(retired), false, default));
            }
            Assert.Empty(server.Requests);
            Assert.True(server.Contains("t/part-1.parquet"));
            Assert.True(server.Contains("t/part-1.parquet#x"));
            Assert.True(server.Contains("t/ab/deletion_vector_1.bin"));

            // A retired key equal to the folder's name is one exact delete of that object only
            Assert.Equal(KeepReason.None, cloud.Executor.TryTarget("ab", TargetKind.Payload, out var exact));
            Assert.Equal(DeleteStatus.Deleted, (await cloud.Executor.DeleteAsync(exact, false, default)).Status);
            Assert.Equal(new[] { "DELETE /c/t/ab HTTP/1.1" }, server.Requests);
            Assert.False(server.Contains("t/ab"));
            Assert.True(server.Contains("t/ab/deletion_vector_1.bin"));
            Assert.True(server.Contains("t/part-1.parquet"));
            Assert.Equal(3, cloud.Executor.Tally.KeptFor(KeepReason.Unqualified));
        }

        [Fact]
        public Task S3RetiredHashKeyCannotDeleteProtectedPrefix() => RetiredHashKeyCannotDeleteProtectedPrefix(StoreFlavor.S3);

        [Theory]
        [InlineData(StoreFlavor.Azure)]
        [InlineData(StoreFlavor.AzureHns)]
        public Task AzureRetiredHashKeyCannotDeleteProtectedPrefix(StoreFlavor flavor) => RetiredHashKeyCannotDeleteProtectedPrefix(flavor);

        [Theory]
        [InlineData(StoreFlavor.S3)]
        [InlineData(StoreFlavor.Azure)]
        public async Task UnqualifiedForeignCandidatesAreKeptAndCounted(StoreFlavor flavor)
        {
            await using var server = new LoopbackObjectStore(flavor);
            var cloud = Cloud(server);
            var foreign = new[]
            {
                "/c/t/part 1.parquet", "/c/t/part+1.parquet", "/c/t/part%201.parquet", "/c/t/part?x=1.parquet", "/c/t/a=1/part.parquet", "/c/t/part&x.parquet",
                "/c/t/part~1.parquet", "/c/t/part:1.parquet", "/c/t/pärt.parquet", "/c/t/x//part.parquet", "/c/t/./part.parquet", "/c/t/../u/part.parquet",
                "/c/t/part.parquet/", "/c/t/", "/c/t", "/c/tx/part.parquet", "/c/t2part.parquet", "/c/u/part.parquet", "/d/t/part.parquet", "c/t/part.parquet", "/c/T/part.parquet"
            };
            foreach (var key in foreign)
            {
                if (key.StartsWith("/c/", StringComparison.Ordinal) && !key.EndsWith('/') && key != "/c/t")
                {
                    server.Put(key.Substring(3));
                }
            }
            server.Put("t/part.parquet");
            server.Put("u/part.parquet");
            var before = server.Names;
            cloud.ClearRequests();

            foreach (var key in foreign.Append("").Append(null))
            {
                Assert.Equal(DeleteOutcome.Keep(KeepReason.Unqualified), await cloud.Executor.DeleteAsync(new DeletionTarget(key!, TargetKind.Payload), false, default));
            }
            foreach (var relative in new[] { "part 1.parquet", "part#1.parquet", "pärt.parquet", "a=1/part.parquet", "../u/part.parquet", "" })
            {
                Assert.Equal(KeepReason.Unqualified, cloud.Executor.TryTarget(relative, TargetKind.Payload, out _));
            }

            Assert.Equal(foreign.Length + 2, cloud.Executor.Tally.KeptFor(KeepReason.Unqualified));
            Assert.Equal(foreign.Length + 2, cloud.Executor.Tally.Kept);
            Assert.Empty(server.Requests);
            Assert.Equal(before, server.Names);
        }

        [Fact]
        public async Task GcsInlineTargetUsesExactQualifiedName()
        {
            using var gcs = StoreFixtures.Gcs();
            var descriptor = StoreDescriptor.Of(gcs);
            Assert.Equal(StoreKind.Gcs, descriptor.Kind);
            Assert.True(descriptor.KnownFailed);
            const string Own = "part-00000-0f8fad5b-d9cb-469f-a165-70867728950e.parquet";
            var objects = new ScriptedStorage();
            objects.Put("/sales/t/" + Own);
            objects.Put("/sales/t/part");
            objects.Put("/sales/t/part#x.parquet");
            // The sink's own storage, as the inline fallback has it; the opt-ins are off so nothing throws
            var configuration = CleanupConfiguration.Validate(CleanupStoreContract.Create(objects, descriptor, "sales/t", false), CleanupOptIns.Off);
            Assert.False(configuration.Store.Eligible);
            var executor = new DeletionExecutor(configuration, objects);

            Assert.Equal(KeepReason.None, executor.TryTarget(Own, TargetKind.Payload, out var target));
            Assert.Equal("/sales/t/" + Own, target.Key);
            Assert.Equal(DeleteStatus.Deleted, (await executor.DeleteAsync(target, false, default)).Status);
            foreach (var foreign in new[] { "part#x.parquet", "part?x.parquet", "part 1.parquet", "_change_data/cdc#1.parquet" })
            {
                Assert.Equal(KeepReason.Unqualified, executor.TryTarget(foreign, TargetKind.Payload, out _));
                Assert.Equal(DeleteOutcome.Keep(KeepReason.Unqualified), await executor.DeleteAsync(new DeletionTarget("/sales/t/" + foreign, TargetKind.Payload), false, default));
            }
            Assert.Equal(new[] { "Rm /sales/t/" + Own }, objects.Requests);
            Assert.NotNull(await objects.Stat("/sales/t/part"));
            Assert.NotNull(await objects.Stat("/sales/t/part#x.parquet"));

            // An unsafe table prefix qualifies no target
            var unsafeConfiguration = CleanupConfiguration.Validate(CleanupStoreContract.Create(objects, descriptor, "sales#2024/t", false), CleanupOptIns.Off);
            var unsafeExecutor = new DeletionExecutor(unsafeConfiguration, objects);
            Assert.Equal(KeepReason.StoreUnsupported, unsafeExecutor.TryTarget(Own, TargetKind.Payload, out _));
            Assert.Equal(DeleteOutcome.Keep(KeepReason.StoreUnsupported), await unsafeExecutor.DeleteAsync(new DeletionTarget("/sales#2024/t/" + Own, TargetKind.Payload), false, default));
            Assert.Single(objects.Requests, x => x.StartsWith("Rm", StringComparison.Ordinal));
        }

        [Theory]
        [InlineData("part.parquet", "Payload", true)]
        [InlineData("_change_data/cdc-1.parquet", "Payload", true)]
        [InlineData("ab/deletion_vector_1.bin", "Payload", true)]
        [InlineData("_delta_log/part.parquet", "Payload", false)]
        [InlineData("_DELTA_LOG/part.parquet", "Payload", false)]
        [InlineData("nested/_delta_log/00000000000000000001.json", "Payload", false)]
        [InlineData("_delta_log/.00000000000000000003.json.stage-3.tmp", "Stage", true)]
        [InlineData(".00000000000000000003.json.stage-3.tmp", "Stage", false)]
        [InlineData("_delta_log/sub/.00000000000000000003.json.stage-3.tmp", "Stage", false)]
        [InlineData("_delta_log/00000000000000000003.json", "Stage", false)]
        [InlineData("_delta_log/.flowtide-cleanup.00000000000000000005.adv.started", "Control", true)]
        [InlineData("_delta_log/.flowtide-cleanup.", "Control", false)]
        [InlineData("_delta_log/.flowtide-cleanupx", "Control", false)]
        [InlineData("x/.flowtide-cleanup.00000000000000000005.adv.started", "Control", false)]
        [InlineData("_delta_log/.flowtide-0f8fad5bd9cb469fa16570867728950e.checkpoint.tmp", "Temporary", true)]
        [InlineData("_delta_log/.flowtide-0f8fad5bd9cb469fa16570867728950e.last_checkpoint.tmp", "Temporary", true)]
        [InlineData("_delta_log/.flowtide-0f8fad5bd9cb469fa16570867728950x.checkpoint.tmp", "Temporary", false)]
        [InlineData("_delta_log/.flowtide-0f8fad5bd9cb469fa16570867728950e.other.tmp", "Temporary", false)]
        [InlineData(".flowtide-0f8fad5bd9cb469fa16570867728950e.checkpoint.tmp", "Temporary", false)]
        [InlineData("_delta_log/00000000000000000003.json", "Log", true)]
        [InlineData("_delta_log/00000000000000000003.crc", "Log", true)]
        [InlineData("_delta_log/00000000000000000003.checkpoint.parquet", "Log", true)]
        [InlineData("_delta_log/_last_checkpoint", "Log", false)]
        [InlineData("_delta_log/0000000000000000003.json", "Log", false)]
        [InlineData("_delta_log/0000000000000000000x.json", "Log", false)]
        [InlineData("_delta_log/00000000000000000003", "Log", false)]
        [InlineData("_delta_log/00000000000000000003x.json", "Log", false)]
        [InlineData("_delta_logx/00000000000000000003.json", "Log", false)]
        [InlineData("nested/_delta_log/00000000000000000003.json", "Log", false)]
        [InlineData("part.parquet", "Log", false)]
        public async Task ExecutorKeepsKindsInTheirPlace(string relative, string kindName, bool allowed)
        {
            var kind = Enum.Parse<TargetKind>(kindName);
            var table = Scripted();
            table.Objects.Put("/c/t/" + relative);

            Assert.Equal(allowed ? KeepReason.None : KeepReason.Misplaced, table.Executor.TryTarget(relative, kind, out _));
            var outcome = await table.Executor.DeleteAsync(table.Target(relative, kind), false, default);

            Assert.Equal(allowed ? DeleteStatus.Deleted : DeleteStatus.Kept, outcome.Status);
            Assert.Equal(allowed ? KeepReason.None : KeepReason.Misplaced, outcome.Reason);
            Assert.Equal(allowed ? new[] { "Rm /c/t/" + relative } : Array.Empty<string>(), table.Objects.Requests.Where(x => x.StartsWith("Rm", StringComparison.Ordinal)));
        }

        [Fact]
        public async Task ExecutorRejectsUnsafeNativeNames()
        {
            var table = Local();
            var sibling = Local("t2", table.Root);
            sibling.Put("part.parquet");
            table.Put("part.parquet");
            table.Put("sub/part.parquet");
            table.Put("a");
            var outside = Path.Combine(_directory, "outside.parquet");
            File.WriteAllText(outside, "o");
            // Another root of the same shape, so only the prefix tells it apart
            var lookalike = Path.Combine(_directory, new string('z', Path.GetFileName(table.Root).Length), "t", "part.parquet");
            Directory.CreateDirectory(Path.GetDirectoryName(lookalike)!);
            File.WriteAllText(lookalike, "l");
            var separator = Path.DirectorySeparatorChar;
            var unsafeKeys = new List<string>
            {
                table.TableDir,
                table.TableDir + separator,
                // The sibling shares the root's name as a prefix
                sibling.Native("part.parquet"),
                table.TableDir + separator + ".." + separator + "t2" + separator + "part.parquet",
                Path.Combine("t", "part.parquet"),
                outside,
                lookalike
            };
            var stream = table.TableDir + separator + "a:hidden";
            if (OperatingSystem.IsWindows())
            {
                // Stream syntax, a segment Windows would trim, a device name and a trailing dot
                File.WriteAllText(stream, "h");
                unsafeKeys.Add(stream);
                unsafeKeys.Add(table.TableDir + separator + "sub " + separator + "part.parquet");
                unsafeKeys.Add(table.Native("nul.parquet"));
                unsafeKeys.Add(table.Native("COM1.parquet"));
                unsafeKeys.Add(table.Native("LPT².parquet"));
                unsafeKeys.Add(table.Native("aux .parquet"));
                unsafeKeys.Add(table.Native("part.parquet."));
            }

            foreach (var key in unsafeKeys)
            {
                Assert.Equal(DeleteOutcome.Keep(KeepReason.Unqualified), await table.Executor.DeleteAsync(new DeletionTarget(key, TargetKind.Payload), false, default));
            }

            Assert.Equal(unsafeKeys.Count, table.Executor.Tally.KeptFor(KeepReason.Unqualified));
            Assert.True(File.Exists(sibling.Native("part.parquet")));
            Assert.True(File.Exists(table.Native("part.parquet")));
            Assert.True(File.Exists(table.Native("sub/part.parquet")));
            Assert.True(File.Exists(table.Native("a")));
            Assert.True(File.Exists(outside));
            Assert.True(File.Exists(lookalike));
            if (OperatingSystem.IsWindows())
            {
                Assert.Equal("h", File.ReadAllText(stream));
            }
            Assert.Equal(0, table.Buckets.NativeRequests);
        }

        [DllImport("kernel32.dll", CharSet = CharSet.Unicode, SetLastError = true, EntryPoint = "GetShortPathNameW")]
        private static extern uint GetShortPathName(string longPath, StringBuilder shortPath, uint length);

        [WindowsFact]
        public async Task ExecutorKeepsWindowsShortAliases()
        {
            var table = Local();
            var protectedPath = table.Put("part-00001-protected.parquet");
            var buffer = new StringBuilder(1024);
            var length = GetShortPathName(protectedPath, buffer, (uint)buffer.Capacity);
            var shortName = length == 0 ? null : Path.GetFileName(buffer.ToString());
            if (shortName == null || shortName == "part-00001-protected.parquet")
            {
                _output.WriteLine("The volume creates no short names; the generated alias case was skipped");
            }
            else
            {
                // A generated alias holds '~', which the canonical form expands
                var alias = table.Native(shortName);
                Assert.True(File.Exists(alias));
                Assert.Equal(DeleteOutcome.Keep(KeepReason.Unqualified), await table.Executor.DeleteAsync(new DeletionTarget(alias, TargetKind.Payload), false, default));
                Assert.True(File.Exists(protectedPath));
            }

            // An alias without '~' needs a privilege to set, so the long path the system reports for it is substituted
            var aliasPath = table.Put("PART0001.PAR");
            table.Executor.LongPathForTests = path => path == aliasPath ? protectedPath : path;
            Assert.Equal(DeleteOutcome.Keep(KeepReason.Alias), await table.Executor.DeleteAsync(new DeletionTarget(aliasPath, TargetKind.Payload), false, default));
            Assert.True(File.Exists(aliasPath));
            Assert.True(File.Exists(protectedPath));
            table.Executor.LongPathForTests = null;
            Assert.Equal(DeleteStatus.Deleted, (await table.Executor.DeleteAsync(new DeletionTarget(protectedPath, TargetKind.Payload), false, default)).Status);
            Assert.False(File.Exists(protectedPath));
        }

        [Fact]
        public async Task ExecutorDeletesNativeTargetsPastMaxPath()
        {
            // A table folder below the classic limit holding a file past it
            var root = Path.Combine(_directory, new string('r', 100), new string('s', 230 - _directory.Length - 102));
            var table = Local(root: root);
            Assert.InRange(table.TableDir.Length, 200, 259);
            var name = new string('p', 300 - table.TableDir.Length) + ".parquet";
            var path = table.Put(name);
            Assert.True(path.Length >= 260, $"{path.Length} characters");

            var outcome = await table.Executor.DeleteAsync(new DeletionTarget(path, TargetKind.Payload), false, default);

            Assert.Equal(DeleteStatus.Deleted, outcome.Status);
            Assert.False(File.Exists(path));
        }

        [WindowsFact]
        public async Task ExecutorDeletesUnderAnExtendedTableRoot()
        {
            // A table rooted at an extended path keeps that form
            var extended = Local(root: @"\\?\" + Path.Combine(_directory, ScratchOwners.ShortId()));
            Assert.StartsWith(@"\\?\", extended.TableDir);
            var extendedPath = extended.Put("part-1.parquet");
            Assert.Equal(DeleteStatus.Deleted, (await extended.Executor.DeleteAsync(new DeletionTarget(extendedPath, TargetKind.Payload), false, default)).Status);
            Assert.False(File.Exists(extendedPath));
        }

        [Theory]
        [InlineData(StoreFlavor.Azure)]
        [InlineData(StoreFlavor.AzureHns)]
        public async Task AzureOrphanTargetsNeedPositiveSizeAtInventoryAndSubmission(StoreFlavor flavor)
        {
            await using var server = new LoopbackObjectStore(flavor);
            var cloud = Cloud(server);
            var five = Encoding.UTF8.GetBytes("12345");
            server.Put("t/live.parquet", five);
            server.Put("t/empty.parquet", Array.Empty<byte>());
            server.Put("t/unknown.parquet", five);
            server.Put("t/swapped.parquet", five);
            server.Put("t/dir/inner.parquet", five);
            server.Put("t/payload.parquet", five);
            // Emptied after the inventory, before submission
            server.Put("t/swapped.parquet", Array.Empty<byte>());
            cloud.ClearRequests();

            Assert.Equal(DeleteStatus.Deleted, (await cloud.Executor.DeleteAsync(cloud.Target("live.parquet") with { Size = 5 }, true, default)).Status);
            Assert.Equal(DeleteOutcome.Keep(KeepReason.UnknownSize), await cloud.Executor.DeleteAsync(cloud.Target("empty.parquet") with { Size = 0 }, true, default));
            Assert.Equal(DeleteOutcome.Keep(KeepReason.UnknownSize), await cloud.Executor.DeleteAsync(cloud.Target("unknown.parquet") with { Size = null }, true, default));
            Assert.Equal(DeleteOutcome.Keep(KeepReason.UnknownSize), await cloud.Executor.DeleteAsync(cloud.Target("dir") with { Size = 0 }, true, default));
            Assert.Equal(DeleteOutcome.Keep(KeepReason.UnknownSize), await cloud.Executor.DeleteAsync(cloud.Target("swapped.parquet") with { Size = 5 }, true, default));
            Assert.Equal(DeleteStatus.Absent, (await cloud.Executor.DeleteAsync(cloud.Target("gone.parquet") with { Size = 5 }, true, default)).Status);
            // A payload run deletes by its exact key without the size rule
            Assert.Equal(DeleteStatus.Deleted, (await cloud.Executor.DeleteAsync(cloud.Target("payload.parquet"), false, default)).Status);

            Assert.Equal(new[]
            {
                "HEAD /c/t/live.parquet HTTP/1.1", "DELETE /c/t/live.parquet HTTP/1.1",
                "HEAD /c/t/swapped.parquet HTTP/1.1", "HEAD /c/t/gone.parquet HTTP/1.1",
                "DELETE /c/t/payload.parquet HTTP/1.1"
            }, server.Requests);
            Assert.False(server.Contains("t/live.parquet"));
            Assert.True(server.Contains("t/empty.parquet"));
            Assert.True(server.Contains("t/unknown.parquet"));
            Assert.True(server.Contains("t/swapped.parquet"));
            Assert.True(server.Contains("t/dir/inner.parquet"));
        }

        [Fact]
        public async Task ExecutorKeepsEverythingOnAChangedOrInexactStore()
        {
            // The storage is no longer the instance classified at construction
            var table = Local();
            table.Put("part.parquet");
            var other = Files.Of.LocalDisk(table.Root);
            _disposables.Add(other);
            var changed = new DeletionExecutor(table.Configuration, MeteredFileStorage.Wrap(other, table.Buckets, ListingPaging.LocalDisk));
            Assert.Equal(DeleteOutcome.Keep(KeepReason.StorageChanged), await changed.DeleteAsync(table.Target("part.parquet"), false, default));
            Assert.True(File.Exists(table.Native("part.parquet")));

            // A wrapper nobody vouched for has no exact deletes
            var wrapped = new ForwardingStorage(table.Local);
            var unknown = new DeletionExecutor(CleanupConfiguration.Validate(wrapped, "t", false, CleanupOptIns.Off), wrapped);
            Assert.Equal(KeepReason.StoreUnsupported, unknown.TryTarget("part.parquet", TargetKind.Payload, out _));
            Assert.Equal(DeleteOutcome.Keep(KeepReason.StoreUnsupported), await unknown.DeleteAsync(new DeletionTarget("/t/part.parquet", TargetKind.Payload), false, default));
            Assert.True(File.Exists(table.Native("part.parquet")));

            // An Azure endpoint with a path segment deletes in another account
            await using var server = new LoopbackObjectStore(StoreFlavor.Azure, "devstoreaccount1/");
            var client = server.CreateStorage();
            _disposables.Add(client);
            server.Put("t/part.parquet");
            var prefixed = new DeletionExecutor(CleanupConfiguration.Validate(client, "c/t", false, CleanupOptIns.Off), client);
            Assert.Equal(DeleteOutcome.Keep(KeepReason.StoreUnsupported), await prefixed.DeleteAsync(new DeletionTarget("/c/t/part.parquet", TargetKind.Payload), false, default));
            Assert.Empty(server.Requests);
        }

        [Fact]
        public async Task EveryDeleteIsCharged()
        {
            var table = Local();
            table.Put("a.parquet");
            table.Put("b.parquet");
            foreach (var name in new[] { "a.parquet", "b.parquet", "absent.parquet" })
            {
                Assert.True((await table.Executor.DeleteAsync(table.Target(name), false, default)).Done);
            }
            Assert.Equal(3, table.Buckets.NativeRequests);
            Assert.Equal(3, table.Buckets.Requests.Charged);

            await using var server = new LoopbackObjectStore(StoreFlavor.Azure);
            var cloud = Cloud(server);
            server.Put("t/a.parquet");
            server.Put("t/b.parquet");
            Assert.Equal(DeleteStatus.Deleted, (await cloud.Executor.DeleteAsync(cloud.Target("a.parquet"), false, default)).Status);
            Assert.Equal(1, cloud.Buckets.Requests.Charged);
            // An orphan's submission Stat is charged as well
            Assert.Equal(DeleteStatus.Deleted, (await cloud.Executor.DeleteAsync(cloud.Target("b.parquet") with { Size = 1 }, true, default)).Status);
            Assert.Equal(3, cloud.Buckets.Requests.Charged);
            Assert.Equal(0, cloud.Buckets.NativeRequests);
        }
    }

    /// <summary>
    /// A connector-owned view of a storage whose listings, Stat answers and deletes a test can hold, change or count.
    /// </summary>
    internal class TestStorage : IFileStorage, IConnectorOwnedStorage
    {
        public TestStorage(IFileStorage inner)
        {
            Inner = inner;
        }

        // Settable, so a test can replace the storage below
        public IFileStorage Inner { get; set; }

        // Calls as "Verb path", in order
        public ConcurrentQueue<string> Calls { get; } = new ConcurrentQueue<string>();

        public Func<IOPath, Task>? BeforeLs { get; set; }

        public Func<IOPath, IOEntry?, IOEntry?>? StatAnswer { get; set; }

        public Func<IOPath, CancellationToken, Task>? BeforeRm { get; set; }

        // Rewrites a delivered listing
        public Func<IReadOnlyCollection<IOEntry>, IReadOnlyCollection<IOEntry>>? AfterLs { get; set; }

        public int Listings(string folder) => Calls.Count(x => x == "Ls " + folder);

        public async Task<IReadOnlyCollection<IOEntry>> Ls(IOPath? path = null, bool recurse = false, CancellationToken cancellationToken = default)
        {
            var folder = path ?? IOPath.Root;
            Calls.Enqueue("Ls " + folder.Full);
            if (BeforeLs != null)
            {
                await BeforeLs(folder);
            }
            var listed = await Inner.Ls(path, recurse, cancellationToken);
            return AfterLs != null ? AfterLs(listed) : listed;
        }

        public async Task<IOEntry?> Stat(IOPath path, CancellationToken cancellationToken = default)
        {
            Calls.Enqueue("Stat " + path.Full);
            var entry = await Inner.Stat(path, cancellationToken);
            return StatAnswer != null ? StatAnswer(path, entry) : entry;
        }

        public async Task Rm(IOPath path, CancellationToken cancellationToken = default)
        {
            Calls.Enqueue("Rm " + path.Full);
            if (BeforeRm != null)
            {
                await BeforeRm(path, cancellationToken);
            }
            await Inner.Rm(path, cancellationToken);
        }

        public Task<Stream> OpenWrite(IOPath path, CancellationToken cancellationToken = default) => Inner.OpenWrite(path, cancellationToken);

        public Task<Stream?> OpenRead(IOPath path, CancellationToken cancellationToken = default) => Inner.OpenRead(path, cancellationToken);

        public Task Ren(IOPath name, IOPath newName, CancellationToken cancellationToken = default) => Inner.Ren(name, newName, cancellationToken);

        public Task<string?> ReadText(IOPath path, Encoding? encoding = null, CancellationToken cancellationToken = default) => Inner.ReadText(path, encoding, cancellationToken);

        public Task WriteText(IOPath path, string contents, Encoding? encoding = null, CancellationToken cancellationToken = default) => Inner.WriteText(path, contents, encoding, cancellationToken);

        public Task<T?> ReadAsJson<T>(IOPath path, CancellationToken cancellationToken = default) => Inner.ReadAsJson<T>(path, cancellationToken);

        public Task WriteAsJson(IOPath path, object value, bool writeIndented = true, CancellationToken cancellationToken = default) => Inner.WriteAsJson(path, value, writeIndented, cancellationToken);

        public Task<bool> Exists(IOPath path, CancellationToken cancellationToken = default) => Inner.Exists(path, cancellationToken);

        public void Dispose()
        {
        }
    }

    /// <summary>
    /// The test view over local disk, which keeps native paths reachable.
    /// </summary>
    internal sealed class LocalTestStorage : TestStorage, ILocalDiskFileStorage
    {
        public LocalTestStorage(IFileStorage inner) : base(inner)
        {
        }

        public string ToNativeLocalPath(IOPath path) => ((ILocalDiskFileStorage)Inner).ToNativeLocalPath(path);
    }
}
