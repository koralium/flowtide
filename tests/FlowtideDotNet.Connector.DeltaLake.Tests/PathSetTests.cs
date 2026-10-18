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
using FlowtideDotNet.Connector.DeltaLake.Internal.Maintenance.References;
using Stowage;
using System.Text;

namespace FlowtideDotNet.Connector.DeltaLake.Tests
{
    public class PathSetTests : IDisposable
    {
        // A few entries per arena, so sets spill into many runs
        private const long SmallArena = 16 << 10;

        private readonly string _directory = Path.Combine(Path.GetTempPath(), "ps-" + ScratchOwners.ShortId());
        private readonly ScratchOwners _owners = new ScratchOwners();
        private readonly JobMemory _memory = new JobMemory();
        private readonly JobScratch _scratch;

        public PathSetTests()
        {
            Directory.CreateDirectory(_directory);
            _scratch = _owners.Add(new ScratchDirectory(Path.Combine(_directory, "scratch"))).CreateJob();
        }

        public void Dispose()
        {
            _scratch.Dispose();
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

        private static string Key(int i) => $"part-{i:D5}.parquet";

        private static string Target(int i) => $"/c/t/part-{i:D5}.parquet";

        private static byte[] Payload(long value) => BitConverter.GetBytes(value);

        private long SealedBytes() => Directory.EnumerateFiles(_scratch.DirectoryPath).Sum(x => new FileInfo(x).Length);

        [Fact]
        public void PathSetSpillsAndDifferenceStreams()
        {
            const int total = 6000;
            var random = new Random(17);
            var order = Enumerable.Range(0, total).OrderBy(_ => random.Next()).ToArray();

            var candidates = new PathSet(_scratch, _memory, PathRole.Candidate, SmallArena);
            var protection = new PathSet(_scratch, _memory, PathRole.Protection, SmallArena);
            foreach (var i in order)
            {
                candidates.Add(Key(i), Target(i), Payload(i));
            }
            // Repeats keep the first payload
            foreach (var i in order.Take(total / 2))
            {
                candidates.Add(Key(i), Target(i), Payload(-1));
            }
            foreach (var i in order.Where(x => x % 3 == 0))
            {
                protection.Add(Key(i));
            }
            // Protection beyond every candidate, and between them
            for (int i = 0; i < 500; i++)
            {
                protection.Add($"zz-{i}");
                protection.Add($"part-{i:D5}.parquet.extra");
            }
            Assert.True(candidates.SpilledRuns > CheckpointSorter.FanIn, $"{candidates.SpilledRuns} runs exercise no merge pass");
            Assert.True(protection.SpilledRuns > 1);
            Assert.True(_memory.Managed > 0);

            candidates.Seal();
            protection.Seal();
            // Sealed sets hold no sorter memory, only their runs on the disk ledger
            Assert.Equal(0, _memory.Managed);
            Assert.Equal(total, candidates.Count);
            Assert.Equal(total / 3 + 1000, protection.Count);
            Assert.Equal(total * 3 / 2, candidates.Added);
            Assert.Equal(SealedBytes(), _scratch.Ledger.Reserved);
            Assert.Equal(2, Directory.EnumerateFiles(_scratch.DirectoryPath).Count());
            Assert.True(candidates.SpillBytes > SealedBytes());

            // The same protection serves every window, read again each time
            var expected = Enumerable.Range(0, total).Where(x => x % 3 != 0).ToList();
            for (int pass = 0; pass < 2; pass++)
            {
                long peak = 0;
                var emitted = new List<int>();
                using (var difference = PathSet.Difference(candidates, protection))
                {
                    while (difference.MoveNext())
                    {
                        peak = Math.Max(peak, _memory.Managed);
                        var key = Encoding.UTF8.GetString(difference.Key);
                        var i = int.Parse(key.Substring(5, 5));
                        Assert.Equal(Key(i), key);
                        Assert.Equal(Target(i), difference.DeletionPathText);
                        Assert.Equal(i, BitConverter.ToInt64(difference.Payload));
                        emitted.Add(i);
                    }
                    Assert.Equal(total / 3, difference.Protected);
                    Assert.Equal(expected.Count, difference.Emitted);
                    Assert.Equal(0, difference.Untargeted);
                }
                // In key order, and one run cursor a side whatever the set size
                Assert.Equal(expected, emitted);
                Assert.Equal(2L * RunWriter.BufferSize, peak);
                Assert.Equal(0, _memory.Managed);
            }

            candidates.Dispose();
            protection.Dispose();
            Assert.Equal(0, _scratch.Ledger.Reserved);
            Assert.Empty(Directory.EnumerateFiles(_scratch.DirectoryPath));
        }

        [Fact]
        public void ExtractorRefusesASetOfAnotherRole()
        {
            using var azure = StoreFixtures.Azure();
            var identity = PathCanonicalizer.Create(CleanupStoreContract.Classify(azure, "c/t", false));
            using var protection = new PathSet(_scratch, _memory, PathRole.Protection);
            using var candidates = new PathSet(_scratch, _memory, PathRole.Candidate);
            Assert.Equal(PathRole.Protection, protection.Role);
            Assert.Equal(PathRole.Candidate, candidates.Role);

            // A candidate extractor would count an absolute in-table reference instead of protecting it, so it cannot fill a protection set
            Assert.Throws<ArgumentException>(() => new CommitReferenceExtractor(identity, protection, PathRole.Candidate));
            Assert.Throws<ArgumentException>(() => new CommitReferenceExtractor(identity, candidates, PathRole.Protection));
            Assert.Equal(0, protection.Added);

            // Each set filled by its own role protects the referenced name
            Assert.True(new CommitReferenceExtractor(identity, protection, PathRole.Protection).AddLine("""{"add":{"path":"/t/part-a.parquet"}}"""u8));
            var listed = new CommitReferenceExtractor(identity, candidates, PathRole.Candidate);
            Assert.True(listed.AddName("part-a.parquet"));
            Assert.True(listed.AddName("part-b.parquet"));
            candidates.Seal();
            protection.Seal();
            using (var difference = PathSet.Difference(candidates, protection))
            {
                Assert.True(difference.MoveNext());
                Assert.Equal("/c/t/part-b.parquet", difference.DeletionPathText);
                Assert.False(difference.MoveNext());
            }

            // A difference takes each set on its own side only
            Assert.Throws<ArgumentException>(() => PathSet.Difference(protection, candidates));
            Assert.Throws<ArgumentException>(() => PathSet.Difference(candidates, candidates));
        }

        [Fact]
        public void SetsOfAnotherKeyDomainAreRefused()
        {
            using var s3 = StoreFixtures.S3();
            using var otherS3 = StoreFixtures.S3();
            var store = CleanupStoreContract.Classify(s3, "c/t", false);
            var cloud = PathCanonicalizer.Create(store);
            var sameStore = PathCanonicalizer.Create(store);
            // Another store of the same kind, and an asserted unknown stack whose keys are folded
            var otherStore = PathCanonicalizer.Create(CleanupStoreContract.Classify(otherS3, "c/t", false));
            var folded = PathCanonicalizer.Create(CleanupStoreContract.Classify(new ForwardingStorage(new ScriptedStorage()), "c/t", true));
            Assert.Equal(PathIdentity.Cloud, otherStore.Identity);
            Assert.Equal(PathIdentity.Folded, folded.Identity);

            using var protection = new PathSet(_scratch, _memory, PathRole.Protection);
            Assert.True(new CommitReferenceExtractor(cloud, protection, PathRole.Protection).AddLine("""{"add":{"path":"part-a.parquet"}}"""u8));
            // Another canonicalizer of the same store and identity fills the same set
            Assert.True(new CommitReferenceExtractor(sameStore, protection, PathRole.Protection).AddLine("""{"add":{"path":"part-b.parquet"}}"""u8));
            Assert.Throws<ArgumentException>(() => new CommitReferenceExtractor(folded, protection, PathRole.Protection));
            Assert.Throws<ArgumentException>(() => new CommitReferenceExtractor(otherStore, protection, PathRole.Protection));
            Assert.Equal(2, protection.Added);
            Assert.Equal(cloud.Domain, protection.Domain);
            // A set made for a domain takes no other, even while empty
            using var made = new PathSet(_scratch, _memory, sameStore.Domain, PathRole.Candidate);
            Assert.Throws<ArgumentException>(() => new CommitReferenceExtractor(folded, made, PathRole.Candidate));
            Assert.Equal(cloud.Domain, made.Domain);

            // Folded candidates never equal cloud protection keys, nor do another store's
            using var foldedCandidates = new PathSet(_scratch, _memory, PathRole.Candidate);
            Assert.True(new CommitReferenceExtractor(folded, foldedCandidates, PathRole.Candidate).AddName("part-a.parquet"));
            using var otherCandidates = new PathSet(_scratch, _memory, PathRole.Candidate);
            Assert.True(new CommitReferenceExtractor(otherStore, otherCandidates, PathRole.Candidate).AddName("part-a.parquet"));
            // Keys added without a canonicalizer belong to no domain
            using var rawCandidates = new PathSet(_scratch, _memory, PathRole.Candidate);
            rawCandidates.Add("part-a.parquet", "/c/t/part-a.parquet");
            Assert.Throws<ArgumentException>(() => new CommitReferenceExtractor(cloud, rawCandidates, PathRole.Candidate));
            using var candidates = new PathSet(_scratch, _memory, PathRole.Candidate);
            var listed = new CommitReferenceExtractor(cloud, candidates, PathRole.Candidate);
            Assert.True(listed.AddName("part-a.parquet"));
            Assert.True(listed.AddName("part-c.parquet"));
            protection.Seal();
            foldedCandidates.Seal();
            otherCandidates.Seal();
            rawCandidates.Seal();
            candidates.Seal();
            Assert.Throws<ArgumentException>(() => PathSet.Difference(foldedCandidates, protection));
            Assert.Throws<ArgumentException>(() => PathSet.Difference(otherCandidates, protection));
            Assert.Throws<ArgumentException>(() => PathSet.Difference(rawCandidates, protection));
            using (var difference = PathSet.Difference(candidates, protection))
            {
                Assert.True(difference.MoveNext());
                Assert.Equal("/c/t/part-c.parquet", difference.DeletionPathText);
                Assert.False(difference.MoveNext());
            }

            // On Windows one local store has two identities: long-form resolution and the portable rule
            if (OperatingSystem.IsWindows())
            {
                var root = Path.Combine(_directory, "local");
                Directory.CreateDirectory(Path.Combine(root, "t"));
                using var local = Files.Of.LocalDisk(root);
                var localStore = CleanupStoreContract.Classify(local, "t", false);
                var windows = PathCanonicalizer.Create(localStore);
                var portable = PathCanonicalizer.Create(localStore, portableForTests: true);
                Assert.NotEqual(windows.Identity, portable.Identity);
                using var localSet = new PathSet(_scratch, _memory, PathRole.Protection);
                Assert.True(new CommitReferenceExtractor(windows, localSet, PathRole.Protection).AddName("part-a.parquet"));
                Assert.Throws<ArgumentException>(() => new CommitReferenceExtractor(portable, localSet, PathRole.Protection));
            }
        }

        [Fact]
        public void PathSetKeepsEverySpellingOfAKey()
        {
            // A folded key may stand for several exact names, each its own target
            using var candidates = new PathSet(_scratch, _memory, PathRole.Candidate, SmallArena);
            using var protection = new PathSet(_scratch, _memory, PathRole.Protection, SmallArena);
            candidates.Add("PART-1.PARQUET", "/t/part-1.parquet");
            candidates.Add("PART-1.PARQUET", "/t/Part-1.parquet");
            candidates.Add("PART-2.PARQUET", "/t/part-2.parquet");
            candidates.Add("PART-2.PARQUET", "/t/PART-2.parquet");
            candidates.Add("PART-10.PARQUET", "/t/part-10.parquet");
            // A key that extends another is a different key
            protection.Add("PART-1");
            protection.Add("PART-2.PARQUET");
            Assert.Equal(0, candidates.SpilledRuns);
            candidates.Seal();
            protection.Seal();
            Assert.Equal(5, candidates.Count);
            // Sealed without a spill, the reducer's index is released too
            Assert.Equal(0, _memory.Managed);

            using var difference = PathSet.Difference(candidates, protection);
            var emitted = new List<string>();
            while (difference.MoveNext())
            {
                emitted.Add(difference.DeletionPathText);
            }
            Assert.Equal(new[] { "/t/Part-1.parquet", "/t/part-1.parquet", "/t/part-10.parquet" }, emitted);
            Assert.Equal(2, difference.Protected);
        }

        [Fact]
        public void UntargetedCandidatesAreNeverEmitted()
        {
            using var candidates = new PathSet(_scratch, _memory, PathRole.Candidate);
            using var protection = new PathSet(_scratch, _memory, PathRole.Protection);
            candidates.Add("a.parquet");
            candidates.Add("b.parquet", "/t/b.parquet");
            candidates.Seal();
            protection.Seal();
            using var difference = PathSet.Difference(candidates, protection);
            Assert.True(difference.MoveNext());
            Assert.Equal("/t/b.parquet", difference.DeletionPathText);
            Assert.False(difference.MoveNext());
            Assert.Equal(1, difference.Untargeted);
        }

        [Fact]
        public void PathSetRefusesKeysHoldingTheSeparator()
        {
            using var set = new PathSet(_scratch, _memory, PathRole.Candidate);
            Assert.Throws<ArgumentException>(() => set.Add("a\0b", "/t/a"));
            Assert.Throws<ArgumentException>(() => set.Add("a", "/t/a\0b"));
            Assert.Throws<ArgumentException>(() => set.Add("", "/t/a"));
            set.Add("a", "/t/a");
            set.Seal();
            Assert.Throws<InvalidOperationException>(() => set.Add("b", "/t/b"));
            Assert.Throws<InvalidOperationException>(() => set.Seal());
            using var reader = set.Open();
            Assert.True(reader.MoveNext());
            Assert.Equal("a", Encoding.UTF8.GetString(reader.Key));
            Assert.Equal("/t/a", Encoding.UTF8.GetString(reader.DeletionPath));
            Assert.False(reader.MoveNext());
        }

        [Fact]
        public void UnsealedSetReleasesItsRunsAndMemory()
        {
            var set = new PathSet(_scratch, _memory, PathRole.Candidate, SmallArena);
            for (int i = 0; i < 2000; i++)
            {
                set.Add(Key(i), Target(i));
            }
            Assert.True(set.SpilledRuns > 0);
            Assert.True(_scratch.Ledger.Reserved > 0);
            set.Dispose();
            Assert.Equal(0, _memory.Managed);
            Assert.Equal(0, _scratch.Ledger.Reserved);
            Assert.Empty(Directory.EnumerateFiles(_scratch.DirectoryPath));
        }

        [Fact]
        public void FailedSealReleasesEverything()
        {
            // Few enough runs that the cancelled merge is the final one, writing the sealed run
            var set = new PathSet(_scratch, _memory, PathRole.Candidate, 64 << 10);
            for (int i = 0; i < 4000; i++)
            {
                set.Add(Key(i), Target(i));
            }
            Assert.InRange(set.SpilledRuns, 1, CheckpointSorter.FanIn);
            using var cancelled = new CancellationTokenSource();
            cancelled.Cancel();
            Assert.ThrowsAny<OperationCanceledException>(() => set.Seal(cancelled.Token));
            Assert.Equal(0, _memory.Managed);
            Assert.Throws<InvalidOperationException>(() => set.Open());
            set.Dispose();
            Assert.Equal(0, _scratch.Ledger.Reserved);
            Assert.Empty(Directory.EnumerateFiles(_scratch.DirectoryPath));
        }

        [Fact]
        public void CancelledMultiPassSealLeavesNoRun()
        {
            var set = new PathSet(_scratch, _memory, PathRole.Candidate, SmallArena);
            // Long names fill the first merge group, which stays below the merge's first cancellation check
            var i = 0;
            while (set.SpilledRuns < CheckpointSorter.FanIn)
            {
                set.Add(new string('l', 200) + Key(i), Target(i));
                i++;
            }
            var firstGroup = i;
            // Short names fill the second group past it
            while (set.SpilledRuns < 2 * CheckpointSorter.FanIn)
            {
                set.Add(Key(i), Target(i));
                i++;
            }
            Assert.True(firstGroup < 1024, $"{firstGroup} entries in the first group reach the cancellation check");
            Assert.True(i - firstGroup > 1024, $"{i - firstGroup} entries in the second group never reach it");
            using var cancelled = new CancellationTokenSource();
            cancelled.Cancel();

            Assert.ThrowsAny<OperationCanceledException>(() => set.Seal(cancelled.Token));

            // Neither the first group's merged run nor the second group's partial one is left
            Assert.Equal(0, _memory.Managed);
            Assert.Equal(0, _scratch.Ledger.Reserved);
            Assert.Empty(Directory.EnumerateFiles(_scratch.DirectoryPath));
            set.Dispose();
            Assert.Empty(_scratch.Files);
        }

        [Fact]
        public void DiscardDeletesEveryRunOfAnUnfinishedMergePass()
        {
            var merges = 0;
            var sorter = new CheckpointSorter(_scratch, _memory)
            {
                // The first merge group completes, the second fails as it starts
                FaultForTests = point =>
                {
                    if (point == "merge" && ++merges == 2)
                    {
                        throw new IOException("injected");
                    }
                }
            };
            using var reducer = new TailReducer(_memory, SmallArena);
            for (var i = 0; sorter.Runs < 2 * CheckpointSorter.FanIn; i++)
            {
                var key = Encoding.UTF8.GetBytes(Key(i));
                if (!reducer.TryAdd(key, 0, -i - 1, default))
                {
                    sorter.Spill(reducer);
                    Assert.True(reducer.TryAdd(key, 0, -i - 1, default));
                }
            }

            Assert.Throws<IOException>(() => sorter.Reduce(reducer, (in ArenaEntry _) => { }));
            Assert.Equal(2, merges);
            sorter.Discard();

            Assert.Empty(_scratch.Files);
            Assert.Equal(0, _scratch.Ledger.Reserved);
            Assert.Empty(Directory.EnumerateFiles(_scratch.DirectoryPath));
            reducer.Dispose();
            Assert.Equal(0, _memory.Managed);
        }
    }
}
