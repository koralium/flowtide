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
using Stowage;
using static FlowtideDotNet.Connector.DeltaLake.Tests.DeltaTestKit;

namespace FlowtideDotNet.Connector.DeltaLake.Tests
{
    public class CreatedFilesReaderTests
    {
        private static async Task<List<string>> CreatedFiles(IFileStorage storage, long version)
        {
            var created = new List<string>();
            var header = await CommitHeaderReader.ReadAsync(storage, CommitPath("test", version), HeaderMode.Strict, created.Add);
            Assert.True(header!.IsCommitInfo, header.Error);
            Assert.True(header.HasCreatedFiles);
            Assert.Equal(HeaderFlags.None, header.Flags);
            Assert.Equal(created.Count, header.CreatedFileCount);
            return created;
        }

        // The data, deletion vector and change data files the commit references that it wrote itself
        private static async Task<HashSet<string>> Written(IFileStorage storage, long version)
        {
            var actions = await ReadCommitActions(storage, "test", version);
            var removed = actions.Where(x => x.Remove != null).Select(x => x.Remove!.Path).ToHashSet();
            var written = new HashSet<string>();
            foreach (var action in actions)
            {
                if (action.Add != null)
                {
                    // A re-added path with a new deletion vector is not a new data file
                    if (!removed.Contains(action.Add.Path))
                    {
                        written.Add(action.Add.Path!);
                    }
                    if (action.Add.DeletionVector?.GetAbsolutePath() is { } dv && !actions.Any(x => x.Remove?.DeletionVector?.UniqueId == action.Add.DeletionVector.UniqueId))
                    {
                        written.Add(dv);
                    }
                }
                if (action.Cdc != null)
                {
                    written.Add(action.Cdc.Path!);
                }
            }
            return written;
        }

        [Fact]
        public async Task CreatedFilesMatchWrittenOutputs()
        {
            // Owned, so the in-memory store below is classified and its discarded change files are deleted inline
            var storage = new OwnedHookFileStorage(Files.Of.InternalMemory($"./{nameof(CreatedFilesMatchWrittenOutputs)}"));
            await using var stream = new DeltaLakeSinkStream(nameof(CreatedFilesMatchWrittenOutputs), storage, options =>
            {
                options.WriteChangeDataOnNewTables = true;
                options.MaxFileSizeBytes = 2_000;
            });
            stream.Generate(200);
            await stream.StartStream(UserInsert);
            await WaitForVersion(storage, "test", stream, 0);
            // One delete writes a deletion vector, many in one file rewrite it
            stream.DeleteUser(stream.Users[0]);
            await WaitForVersion(storage, "test", stream, 1);
            for (int i = 1; i < 60; i++)
            {
                stream.DeleteUser(stream.Users[i]);
            }
            await WaitForVersion(storage, "test", stream, 2);

            // The store's own record of what was written, so the oracle does not come from the commits it checks
            var opened = storage.Requests.Where(x => x.StartsWith("OpenWrite /test/") && !x.StartsWith("OpenWrite /test/_delta_log/")).Select(x => x.Substring("OpenWrite /test/".Length)).ToHashSet();
            var kept = new HashSet<string>();
            foreach (var path in opened)
            {
                if (await storage.Inner.Exists($"/test/{path}"))
                {
                    kept.Add(path);
                }
            }
            var owned = new List<string>();

            var sawDv = false;
            var sawRewrite = false;
            var sawCdc = false;
            for (long version = 0; version <= 2; version++)
            {
                var created = await CreatedFiles(storage.Inner, version);
                owned.AddRange(created);
                Assert.Equal(created.Count, created.Distinct().Count());
                Assert.Equal((await Written(storage, version)).OrderBy(x => x, StringComparer.Ordinal), created.OrderBy(x => x, StringComparer.Ordinal));
                foreach (var path in created)
                {
                    Assert.True(await storage.Exists($"/test/{path}"), path);
                }
                sawDv |= created.Any(x => x.EndsWith(".bin"));
                sawCdc |= created.Any(x => x.StartsWith("_change_data/"));
                sawRewrite |= version > 0 && created.Any(x => x.StartsWith("part-"));
            }
            Assert.True(sawDv && sawRewrite && sawCdc, $"dv {sawDv}, rewrite {sawRewrite}, cdc {sawCdc}");
            // Every surviving written output is owned by exactly one commit, discarded change files by none
            Assert.Equal(owned.Count, owned.Distinct().Count());
            Assert.Equal(kept.OrderBy(x => x, StringComparer.Ordinal), owned.OrderBy(x => x, StringComparer.Ordinal));
            Assert.Contains(opened, x => x.StartsWith("_change_data/") && !kept.Contains(x));
        }

        [Fact]
        public async Task DvReaddedDataPathIsNeverOwned()
        {
            var storage = Files.Of.InternalMemory($"./{nameof(DvReaddedDataPathIsNeverOwned)}");
            await using var stream = new DeltaLakeSinkStream(nameof(DvReaddedDataPathIsNeverOwned), storage);
            stream.Generate(100);
            await stream.StartStream(UserInsert);
            await WaitForVersion(storage, "test", stream, 0);
            stream.DeleteUser(stream.Users[0]);
            await WaitForVersion(storage, "test", stream, 1);

            var actions = await ReadCommitActions(storage, "test", 1);
            var readded = Assert.Single(actions, x => x.Add?.DeletionVector != null).Add!;
            Assert.Contains(actions, x => x.Remove?.Path == readded.Path);
            var created = await CreatedFiles(storage, 1);
            Assert.DoesNotContain(readded.Path, created);
            Assert.Equal(new[] { readded.DeletionVector!.GetAbsolutePath() }, created);
        }

        [Fact]
        public async Task OverwriteOwnsOnlyItsNewFiles()
        {
            var storage = Files.Of.InternalMemory($"./{nameof(OverwriteOwnsOnlyItsNewFiles)}");
            await WriteCommit(storage, "test", 0, Protocol(), Metadata(UserSchema), new DeltaAction() { Add = new DeltaAddAction() { Path = "old.parquet", Size = 1, DataChange = true, PartitionValues = new Dictionary<string, string>() } });
            await using var stream = new DeltaLakeSinkStream(nameof(OverwriteOwnsOnlyItsNewFiles), storage);
            stream.Generate(10);
            await stream.StartStream(UserInsert.Replace("INSERT INTO test", "INSERT OVERWRITE test"));
            await WaitForVersion(storage, "test", stream, 1);

            var created = await CreatedFiles(storage, 1);
            Assert.DoesNotContain("old.parquet", created);
            Assert.Equal((await Written(storage, 1)).OrderBy(x => x, StringComparer.Ordinal), created.OrderBy(x => x, StringComparer.Ordinal));
        }
    }
}
