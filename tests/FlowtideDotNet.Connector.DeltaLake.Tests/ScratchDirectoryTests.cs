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
using System.Diagnostics;
using Xunit.Abstractions;

namespace FlowtideDotNet.Connector.DeltaLake.Tests
{
    public class ScratchDirectoryTests : IDisposable
    {
        private readonly ITestOutputHelper _output;
        private readonly string _root = Path.Combine(Path.GetTempPath(), "sd-" + ScratchOwners.ShortId());

        public ScratchDirectoryTests(ITestOutputHelper output)
        {
            _output = output;
        }

        public void Dispose()
        {
            foreach (var path in new[] { _root, _root + "-target" })
            {
                try
                {
                    Directory.Delete(path, true);
                }
                catch (IOException)
                {
                }
                catch (UnauthorizedAccessException)
                {
                }
            }
        }

        // The test assembly run as a process of its own that holds a scratch folder until its input closes
        private sealed class Child : IDisposable
        {
            private readonly Process _process;

            private Child(Process process, string folder)
            {
                _process = process;
                Folder = folder;
            }

            public string Folder { get; }

            public static async Task<Child> StartAsync(string root)
            {
                var info = new ProcessStartInfo(Environment.GetEnvironmentVariable("DOTNET_HOST_PATH") ?? "dotnet")
                {
                    RedirectStandardInput = true,
                    RedirectStandardOutput = true,
                    RedirectStandardError = true,
                    UseShellExecute = false
                };
                info.ArgumentList.Add(typeof(ScratchDirectoryTests).Assembly.Location);
                info.ArgumentList.Add("hold-scratch");
                info.ArgumentList.Add(root);
                var process = Process.Start(info)!;
                try
                {
                    var folder = await process.StandardOutput.ReadLineAsync().WaitAsync(TimeSpan.FromMinutes(1));
                    if (folder == null)
                    {
                        throw new InvalidOperationException("The child printed no folder: " + await process.StandardError.ReadToEndAsync());
                    }
                    return new Child(process, folder);
                }
                catch
                {
                    process.Kill(entireProcessTree: true);
                    process.Dispose();
                    throw;
                }
            }

            public async Task ExitAsync()
            {
                _process.StandardInput.Close();
                await _process.WaitForExitAsync().WaitAsync(TimeSpan.FromMinutes(1));
                Assert.Equal(0, _process.ExitCode);
            }

            public void Dispose()
            {
                if (!_process.HasExited)
                {
                    _process.Kill(entireProcessTree: true);
                }
                _process.Dispose();
            }
        }

        // A folder left by a process that is gone: its lock file, held by nobody, and a job's file
        private static string DeadSibling(string root)
        {
            var id = Guid.NewGuid().ToString("N");
            var job = Path.Combine(root, id, "jobs", "job");
            Directory.CreateDirectory(job);
            File.WriteAllText(Path.Combine(job, "out.parquet"), "output");
            File.WriteAllText(Path.Combine(root, id + ".lock"), "");
            return Path.Combine(root, id);
        }

        [Fact]
        public async Task LiveFolderOfAnotherProcessIsNeverDeleted()
        {
            using var child = await Child.StartAsync(_root);
            Assert.True(Directory.Exists(child.Folder));

            var directory = new ScratchDirectory(_root);
            Assert.True(directory.LocksEffective);
            Assert.True(Directory.Exists(child.Folder));
            Assert.True(File.Exists(child.Folder + ".lock"));
            await child.ExitAsync();
        }

        [Fact]
        public async Task DeadFolderIsCleanedAfterTheChildExits()
        {
            using var child = await Child.StartAsync(_root);
            await child.ExitAsync();
            // The lock file outlives its process, so the folder is cleaned behind it
            Assert.True(Directory.Exists(child.Folder));
            Assert.True(File.Exists(child.Folder + ".lock"));

            _ = new ScratchDirectory(_root);
            Assert.False(Directory.Exists(child.Folder));
            Assert.False(File.Exists(child.Folder + ".lock"));
        }

        [Fact]
        public void LiveFolderInTheSameProcessIsNeverDeleted()
        {
            var first = new ScratchDirectory(_root);
            using var job = first.CreateJob();
            var second = new ScratchDirectory(_root);
            Assert.True(second.LocksEffective);
            Assert.True(Directory.Exists(first.ProcessPath));
            Assert.True(Directory.Exists(job.DirectoryPath));
            Assert.True(first.LockHeldForTests);
        }

        [Fact]
        public void IneffectiveLocksSkipCleanup()
        {
            var dead = DeadSibling(_root);
            var directory = new ScratchDirectory(_root, locksEffectiveForTests: false);
            Assert.False(directory.LocksEffective);
            Assert.True(Directory.Exists(dead));
            Assert.Contains(directory.TakeNotes(), x => x.Reason == MaintenanceReason.ScratchLocksIneffective);

            // With working locks the same folder is cleaned
            _ = new ScratchDirectory(_root);
            Assert.False(Directory.Exists(dead));
        }

        [Fact]
        public void FolderWithoutLockIsLeft()
        {
            var orphan = Path.Combine(_root, Guid.NewGuid().ToString("N"));
            Directory.CreateDirectory(orphan);
            File.WriteAllText(Path.Combine(orphan, "file"), "x");
            _ = new ScratchDirectory(_root);
            Assert.True(File.Exists(Path.Combine(orphan, "file")));
        }

        // A symbolic link, or a junction where symbolic links need a privilege; false when the OS allows neither
        internal static bool TryLink(string link, string target, out string how)
        {
            try
            {
                Directory.CreateSymbolicLink(link, target);
                how = "symbolic link";
                return true;
            }
            catch (Exception e) when (e is IOException or UnauthorizedAccessException)
            {
                how = e.Message;
            }
            if (!OperatingSystem.IsWindows())
            {
                return false;
            }
            var info = new ProcessStartInfo("cmd.exe") { UseShellExecute = false, RedirectStandardOutput = true, RedirectStandardError = true };
            info.ArgumentList.Add("/c");
            info.ArgumentList.Add("mklink");
            info.ArgumentList.Add("/J");
            info.ArgumentList.Add(link);
            info.ArgumentList.Add(target);
            using var process = Process.Start(info)!;
            process.WaitForExit(30_000);
            if (Directory.Exists(link) && new DirectoryInfo(link).Attributes.HasFlag(FileAttributes.ReparsePoint))
            {
                how = "junction";
                return true;
            }
            how += "; mklink /J: " + process.StandardError.ReadToEnd();
            return false;
        }

        [Fact]
        public void CleanupRefusesLinkedEntries()
        {
            var target = _root + "-target";
            Directory.CreateDirectory(target);
            File.WriteAllText(Path.Combine(target, "keep.txt"), "keep");
            var dead = DeadSibling(_root);
            var link = Path.Combine(dead, "jobs", "link");
            if (!TryLink(link, target, out var how))
            {
                _output.WriteLine($"SKIPPED: the OS created no link ({how})");
                return;
            }
            _output.WriteLine($"Linked with a {how}");

            var directory = new ScratchDirectory(_root);
            Assert.True(File.Exists(Path.Combine(target, "keep.txt")));
            Assert.True(Directory.Exists(dead));
            Assert.Contains(directory.TakeNotes(), x => x.Reason == MaintenanceReason.LinkedScratchSkipped);
            // Only the link itself goes, never its target
            Directory.Delete(link);
        }

        [Fact]
        public async Task RootDeletedOnTheSecondTryLeavesNoPendingCharge()
        {
            var directory = new ScratchDirectory(_root);
            var storage = Files.Of.InternalMemory($"./scratch-double-delete-{Guid.NewGuid():N}");
            await DeltaMaintenanceWorkerTests.Commits(storage, 0, 5);
            await DeltaLakeCheckpointBuildTests.LegacyRoot(storage, "t", 3);
            // The downloaded root is held when the build first lets it go, and free again when the build ends, as an antivirus scan can do
            var held = true;
            directory.DeleteFailsForTests = path => path.EndsWith(".download.parquet") && held;
            var job = directory.CreateJob();
            var result = await CheckpointBuilder.BuildAsync(new CheckpointBuildContext()
            {
                Storage = storage,
                Table = "t",
                Scratch = job,
                Memory = new JobMemory(),
                Publish = false,
                BeforeValidationForTests = _ => held = false
            }, new BuildRequest() { Version = 5 });
            Assert.Equal(BuildStatus.Completed, result.Status);
            Assert.Equal(3L, result.RootVersion);
            job.Dispose();

            // The root's file is gone, so the next job's retry must not keep its charge pending for ever
            using var next = directory.CreateJob();
            Assert.Equal(0, directory.PendingBytes);
            Assert.Equal(0, directory.Ledger.Reserved);
        }

        [Fact]
        public async Task RunScratchIsDeletedWhenTheJobSettles()
        {
            var budget = new DeltaMaintenanceBudget(new MaintenanceBudgetSettings(2, Path.Combine(_root, "budget")), new FakeTimeProvider(), manualTick: true);
            var storage = Files.Of.InternalMemory($"./scratch-settles-{Guid.NewGuid():N}");
            await DeltaMaintenanceWorkerTests.Commits(storage, 0, 2);
            var worker = new DeltaMaintenanceWorker(budget, new MaintenanceWorkerSettings(2, null, TimeProvider.System), storage, "t", "t", null);
            budget.Register(worker);
            worker.Published(2);
            budget.TickNow();
            await worker.CurrentJob;
            Assert.Equal(2, worker.LastCompleted);
            Assert.NotNull(worker.LastJobFolderForTests);
            Assert.False(Directory.Exists(worker.LastJobFolderForTests));
            Assert.Equal(0, budget.Scratch.Ledger.Reserved);
        }
    }
}
