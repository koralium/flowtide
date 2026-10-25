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
using FlowtideDotNet.Connector.DeltaLake.Internal.Delta.DeletionVectors;
using FlowtideDotNet.Connector.DeltaLake.Internal.Delta.Utils;
using FlowtideDotNet.Connector.DeltaLake.Internal.Maintenance;
using FlowtideDotNet.Connector.DeltaLake.Internal.Maintenance.References;
using Microsoft.Extensions.Time.Testing;
using Stowage;
using System.Runtime.InteropServices;
using System.Text;
using Xunit.Abstractions;

namespace FlowtideDotNet.Connector.DeltaLake.Tests
{
    [Collection(nameof(PinnedClientCollection))]
    public class PathIdentityTests : IDisposable
    {
        private readonly ITestOutputHelper _output;
        private readonly string _directory = Path.Combine(Path.GetTempPath(), "pi-" + ScratchOwners.ShortId());
        private readonly ScratchOwners _owners = new ScratchOwners();
        private readonly List<IDisposable> _disposables = new List<IDisposable>();
        private readonly JobMemory _memory = new JobMemory();
        private readonly JobScratch _scratch;

        public PathIdentityTests(ITestOutputHelper output)
        {
            _output = output;
            Directory.CreateDirectory(_directory);
            _scratch = _owners.Add(new ScratchDirectory(Path.Combine(_directory, "scratch"))).CreateJob();
        }

        public void Dispose()
        {
            for (int i = _disposables.Count - 1; i >= 0; i--)
            {
                _disposables[i].Dispose();
            }
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

        private sealed record LocalTable(IFileStorage Storage, CleanupStoreContract Store, string Native);

        // A local disk table at /t below a fresh root
        private LocalTable Local(string? rootName = null)
        {
            var root = Path.Combine(_directory, rootName ?? ScratchOwners.ShortId());
            var native = Path.Combine(root, "t");
            Directory.CreateDirectory(native);
            var local = Files.Of.LocalDisk(root);
            _disposables.Add(local);
            var store = CleanupStoreContract.Classify(local, "t", false);
            Assert.Equal(StoreKind.LocalDisk, store.Store.Kind);
            return new LocalTable(local, store, native);
        }

        private static CleanupStoreContract S3() => CleanupStoreContract.Classify(StoreFixtures.S3(), "c/t", false);

        private static CleanupStoreContract Azure() => CleanupStoreContract.Classify(StoreFixtures.Azure(), "c/t", false);

        private static PathCanonicalizer Native(LocalTable table, bool portable, NativeNames? names = null)
        {
            var identity = PathCanonicalizer.Create(table.Store, native: names, portableForTests: portable);
            Assert.Equal(OperatingSystem.IsWindows() && !portable ? PathIdentity.WindowsNative : PathIdentity.PortableNative, identity.Identity);
            return identity;
        }

        private PathSet NewSet(PathRole role)
        {
            var set = new PathSet(_scratch, _memory, role);
            _disposables.Add(set);
            return set;
        }

        private static PathReference Uri(string value) => new PathReference(ReferenceSource.Add, value, isUri: true);

        private static void Touch(LocalTable table, string relative)
        {
            var path = Path.Combine(table.Native, relative.Replace('/', Path.DirectorySeparatorChar));
            Directory.CreateDirectory(Path.GetDirectoryName(path)!);
            File.WriteAllText(path, "x");
        }

        private static List<(string Key, string Path)> Entries(PathSet set)
        {
            var entries = new List<(string, string)>();
            using var reader = set.Open();
            while (reader.MoveNext())
            {
                entries.Add((Encoding.UTF8.GetString(reader.Key), Encoding.UTF8.GetString(reader.DeletionPath)));
            }
            return entries;
        }

        private static List<string> Deletable(PathSet candidates, PathSet protection)
        {
            candidates.Seal();
            protection.Seal();
            using var difference = PathSet.Difference(candidates, protection);
            var paths = new List<string>();
            while (difference.MoveNext())
            {
                paths.Add(difference.DeletionPathText);
            }
            return paths;
        }

        private static List<(ReferenceSource Source, string? Value, bool IsUri, bool AbsoluteOnly, PathRejection Rejection)> Visit(string line)
        {
            var visited = new List<(ReferenceSource, string?, bool, bool, PathRejection)>();
            var problem = CommitReferenceExtractor.ReadLine(Encoding.UTF8.GetBytes(line), (in PathReference reference) =>
                visited.Add((reference.Source, reference.Value, reference.IsUri, reference.AbsoluteOnly, reference.Rejection)));
            Assert.Equal(PathRejection.None, problem);
            return visited;
        }

        private static string Vector(string storageType, string value, long? offset = 1) =>
            $$$"""{"storageType":"{{{storageType}}}","pathOrInlineDv":"{{{value}}}","offset":{{{(offset?.ToString() ?? "null")}}},"sizeInBytes":34,"cardinality":2}""";

        [Fact]
        public void ExtractorCoversAddRemoveDvAndCdc()
        {
            var shared = Guid.Parse("0f2a3b4c-5d6e-4f70-8192-a3b4c5d6e7f8");
            var removed = Guid.Parse("11111111-2222-4333-8444-555555555555");
            var sharedId = "ab" + Z85.EncodeGuid(shared);
            var lines = new[]
            {
                """{"commitInfo":{"timestamp":1,"operation":"WRITE","path":"not-a-file.parquet"}}""",
                """{"protocol":{"minReaderVersion":3,"minWriterVersion":7,"readerFeatures":["deletionVectors"],"writerFeatures":["deletionVectors"]}}""",
                """{"metaData":{"id":"x","format":{"provider":"parquet","options":{"path":"meta.parquet"}},"schemaString":"{}","partitionColumns":[],"configuration":{}}}""",
                """{"txn":{"appId":"a","version":1}}""",
                """{"domainMetadata":{"domain":"d","configuration":"{\"path\":\"domain.parquet\"}","removed":false}}""",
                $$$"""{"add":{"path":"part-1.parquet","partitionValues":{"path":"pv.parquet"},"size":1,"modificationTime":1,"dataChange":true,"stats":"{\"path\":\"stats.parquet\"}","tags":{"path":"tag.parquet"},"deletionVector":{{{Vector("u", sharedId, 1)}}}}}""",
                $$$"""{"add":{"path":"part-2.parquet","partitionValues":{},"size":1,"modificationTime":1,"dataChange":true,"deletionVector":{{{Vector("u", sharedId, 40)}}}}}""",
                $$$"""{"remove":{"path":"part-0.parquet","deletionTimestamp":5,"dataChange":true,"deletionVector":{{{Vector("u", Z85.EncodeGuid(removed), null)}}}}}""",
                """{"cdc":{"path":"_change_data/cdc-1.parquet","partitionValues":{},"size":3,"dataChange":false,"deletionVector":{"storageType":"x"}}}"""
            };

            var visited = lines.SelectMany(Visit).ToList();
            var sharedFile = $"ab/deletion_vector_{shared:D}.bin";
            var removedFile = $"deletion_vector_{removed:D}.bin";
            Assert.Equal(new (ReferenceSource, string?, bool, bool, PathRejection)[]
            {
                (ReferenceSource.Add, "part-1.parquet", true, false, PathRejection.None),
                (ReferenceSource.AddVector, sharedFile, false, false, PathRejection.None),
                (ReferenceSource.Add, "part-2.parquet", true, false, PathRejection.None),
                (ReferenceSource.AddVector, sharedFile, false, false, PathRejection.None),
                (ReferenceSource.Remove, "part-0.parquet", true, false, PathRejection.None),
                (ReferenceSource.RemoveVector, removedFile, false, false, PathRejection.None),
                (ReferenceSource.Cdc, "_change_data/cdc-1.parquet", true, false, PathRejection.None)
            }, visited);

            // Both offsets of the shared container collapse to one physical file
            var identity = PathCanonicalizer.Create(S3());
            var protection = NewSet(PathRole.Protection);
            var extractor = new CommitReferenceExtractor(identity, protection, PathRole.Protection);
            foreach (var line in lines)
            {
                Assert.True(extractor.AddLine(Encoding.UTF8.GetBytes(line)));
            }
            protection.Seal();
            Assert.Equal(7, extractor.Added);
            Assert.Equal(new[] { "_change_data/cdc-1.parquet", sharedFile, removedFile, "part-0.parquet", "part-1.parquet", "part-2.parquet" }.OrderBy(x => x, StringComparer.Ordinal),
                Entries(protection).Select(x => x.Key));
            Assert.All(Entries(protection), x => Assert.Equal("", x.Path));

            // Candidates keep the exact deletion path beside the key
            var candidates = NewSet(PathRole.Candidate);
            var collector = new CommitReferenceExtractor(identity, candidates, PathRole.Candidate);
            foreach (var line in lines)
            {
                Assert.True(collector.AddLine(Encoding.UTF8.GetBytes(line)));
            }
            Assert.Equal(0, collector.Kept);
            candidates.Seal();
            Assert.Contains(("part-0.parquet", "/c/t/part-0.parquet"), Entries(candidates));
            Assert.Contains((sharedFile, "/c/t/" + sharedFile), Entries(candidates));
            Assert.Contains(("_change_data/cdc-1.parquet", "/c/t/_change_data/cdc-1.parquet"), Entries(candidates));

            // A checkpoint root's add and remove records name the same files
            var records = new List<byte[]>();
            records.AddRange(TestRecords.FromAction(new DeltaAction()
            {
                Add = new DeltaAddAction()
                {
                    Path = "part-1.parquet",
                    PartitionValues = new Dictionary<string, string>() { ["path"] = "pv.parquet" },
                    Size = 1,
                    Tags = new Dictionary<string, string>() { ["path"] = "tag.parquet" },
                    Statistics = "{}",
                    DeletionVector = new DeletionVector() { StorageType = "u", PathOrInlineDv = sharedId, Offset = 9, SizeInBytes = 34, Cardinality = 2 }
                }
            }));
            records.AddRange(TestRecords.FromAction(new DeltaAction()
            {
                Remove = new DeltaRemoveFileAction()
                {
                    Path = "part-0.parquet",
                    DeletionTimestamp = 5,
                    DeletionVector = new DeletionVector() { StorageType = "u", PathOrInlineDv = Z85.EncodeGuid(removed), SizeInBytes = 34, Cardinality = 2 }
                }
            }));
            records.AddRange(TestRecords.FromAction(new DeltaAction() { Protocol = new DeltaProtocolAction() { MinReaderVersion = 1, MinWriterVersion = 2 } }));
            var fromRecords = new List<(ReferenceSource, string?)>();
            foreach (var record in records)
            {
                Assert.Equal(PathRejection.None, CommitReferenceExtractor.ReadRecord(record, (in PathReference reference) => fromRecords.Add((reference.Source, reference.Value))));
            }
            Assert.Equal(new (ReferenceSource, string?)[]
            {
                (ReferenceSource.Add, "part-1.parquet"),
                (ReferenceSource.AddVector, sharedFile),
                (ReferenceSource.Remove, "part-0.parquet"),
                (ReferenceSource.RemoveVector, removedFile)
            }, fromRecords);
        }

        [Fact]
        public void UnknownDvStorageTypeDeclines()
        {
            var identity = PathCanonicalizer.Create(S3());
            var valid = Z85.EncodeGuid(Guid.NewGuid());
            string Add(string vector) => $$$"""{"add":{"path":"part-1.parquet","size":1,"deletionVector":{{{vector}}}}}""";

            (bool Holds, PathRejection Declined, ReferenceSource Source, long Added) Protect(string line)
            {
                var extractor = new CommitReferenceExtractor(identity, NewSet(PathRole.Protection), PathRole.Protection);
                var holds = extractor.AddLine(Encoding.UTF8.GetBytes(line));
                return (holds, extractor.Declined, extractor.DeclinedSource, extractor.Added);
            }

            Assert.Equal((false, PathRejection.UnknownVectorType, ReferenceSource.AddVector, 1L), Protect(Add(Vector("x", valid))));
            Assert.Equal((false, PathRejection.UnknownVectorType, ReferenceSource.AddVector, 1L), Protect(Add("""{"pathOrInlineDv":"abc","sizeInBytes":1,"cardinality":1}""")));
            Assert.Equal((false, PathRejection.UnknownVectorType, ReferenceSource.AddVector, 1L), Protect(Add("""{"storageType":null,"pathOrInlineDv":"abc"}""")));
            // Inline vectors have no file
            Assert.Equal((true, PathRejection.None, ReferenceSource.Add, 1L), Protect(Add(Vector("i", "wi5b=000010000siXQKl0rr91000f55c8Xg0@@D72lkbi5=-{L"))));
            // A uuid that does not decode, or decodes past 32 bits a group, names no file
            Assert.Equal(PathRejection.InvalidVector, Protect(Add(Vector("u", "short"))).Declined);
            Assert.Equal(PathRejection.InvalidVector, Protect(Add(Vector("u", "~" + valid.Substring(1)))).Declined);
            Assert.Equal(PathRejection.InvalidVector, Protect(Add(Vector("u", "#####" + valid.Substring(5)))).Declined);
            Assert.Equal(PathRejection.InvalidVector, Protect(Add("""{"storageType":"u","pathOrInlineDv":null}""")).Declined);
            Assert.Equal((true, PathRejection.None, ReferenceSource.Add, 2L), Protect(Add(Vector("u", valid))));
            // An absolute vector path that is relative, or a repeated field, is unresolved
            Assert.Equal(PathRejection.InvalidVector, Protect(Add(Vector("p", "dv/deletion_vector.bin"))).Declined);
            Assert.Equal(PathRejection.InvalidVector, Protect(Add($$$"""{"storageType":"u","storageType":"x","pathOrInlineDv":"{{{valid}}}"}""")).Declined);
            // An action without a path, or a line that does not parse, declines too
            Assert.Equal(PathRejection.Malformed, Protect("""{"remove":{"deletionTimestamp":1}}""").Declined);
            Assert.Equal(PathRejection.Malformed, Protect("""{"add":{"path":"part-1.parquet"}""").Declined);
            Assert.Equal(PathRejection.Malformed, Protect("""{"add":{"path":"a"}} {"add":{"path":"b"}}""").Declined);
            Assert.Equal(PathRejection.Malformed, Protect("""{"add":["part-1.parquet"]}""").Declined);

            // A checkpoint record with an unknown type declines the same way
            var record = TestRecords.FromAction(new DeltaAction()
            {
                Add = new DeltaAddAction() { Path = "part-1.parquet", DeletionVector = new DeletionVector() { StorageType = "x", PathOrInlineDv = valid } }
            }).Single();
            var fromRecord = new CommitReferenceExtractor(identity, NewSet(PathRole.Protection), PathRole.Protection);
            Assert.False(fromRecord.AddRecord(record));
            Assert.Equal(PathRejection.UnknownVectorType, fromRecord.Declined);
            Assert.False(fromRecord.AddLine(Encoding.UTF8.GetBytes(Add(Vector("u", valid)))));

            // A candidate's unnamed vector is kept and counted; its data file stays a candidate
            var candidates = NewSet(PathRole.Candidate);
            var collector = new CommitReferenceExtractor(identity, candidates, PathRole.Candidate);
            Assert.True(collector.AddLine(Encoding.UTF8.GetBytes(Add(Vector("x", valid)))));
            Assert.True(collector.AddLine(Encoding.UTF8.GetBytes("""{"add":{"path":"part-2.parquet"}""")));
            Assert.Equal(1, collector.KeptFor(PathRejection.UnknownVectorType));
            Assert.Equal(1, collector.KeptFor(PathRejection.Malformed));
            Assert.Equal(2, collector.Kept);
            candidates.Seal();
            Assert.Equal(new[] { ("part-1.parquet", "/c/t/part-1.parquet") }, Entries(candidates));
        }

        [Fact]
        public void ReferencesDecodeOnceWithoutPlus()
        {
            var identity = PathCanonicalizer.Create(S3());
            PathKey Protect(string value) => identity.Reference(Uri(value), PathRole.Protection);
            PathKey Candidate(string value) => identity.Reference(Uri(value), PathRole.Candidate);

            // Escapes decode once; '+' stays '+', and a decoded '%' is never decoded again
            Assert.Equal("a+b+c%20d.parquet", Protect("a+b%2Bc%2520d.parquet").Key);
            Assert.Equal("caf\u00E9 1.parquet", Protect("caf%C3%A9%201.parquet").Key);
            Assert.Equal("dir=a/b#1.parquet", Protect("dir%3Da/b%231.parquet").Key);
            var target = Candidate("part%2D1.parquet");
            Assert.Equal(("part-1.parquet", "/c/t/part-1.parquet"), (target.Key, target.DeletionPath));
            // A decoded '+' or '%' is outside the transmission domain, so such a candidate is kept
            Assert.Equal(PathRejection.Unqualified, Candidate("a%2Bb.parquet").Rejection);
            Assert.Equal(PathRejection.Unqualified, Candidate("a+b.parquet").Rejection);

            // Unescaped URI delimiters and malformed escapes name no file
            Assert.Equal(PathRejection.Syntax, Protect("part#1.parquet").Rejection);
            Assert.Equal(PathRejection.Syntax, Protect("part?1.parquet").Rejection);
            Assert.Equal(PathRejection.Syntax, Protect("dir\\part.parquet").Rejection);
            Assert.Equal(PathRejection.Syntax, Protect("part%0A.parquet").Rejection);
            Assert.Equal(PathRejection.Encoding, Protect("part%2.parquet").Rejection);
            Assert.Equal(PathRejection.Encoding, Protect("part%G1.parquet").Rejection);
            Assert.Equal(PathRejection.Encoding, Protect("part%").Rejection);
            Assert.Equal(PathRejection.Encoding, Protect("part%FF.parquet").Rejection);
            Assert.Equal(PathRejection.Encoding, Protect("part%C3.parquet").Rejection);

            // Traversal is refused before and after decoding
            Assert.Equal(PathRejection.Traversal, Protect("a/../part.parquet").Rejection);
            Assert.Equal(PathRejection.Traversal, Protect("%2E%2E/part.parquet").Rejection);
            Assert.Equal(PathRejection.Traversal, Protect("a//part.parquet").Rejection);
            Assert.Equal(PathRejection.Traversal, Protect("./part.parquet").Rejection);
            Assert.Equal(PathRejection.Traversal, Protect("dir/").Rejection);
            Assert.Equal(PathRejection.Traversal, Protect("").Rejection);
            Assert.Equal(PathRejection.Traversal, Candidate("%2E%2E/part.parquet").Rejection);
            Assert.Equal(PathRejection.Syntax, Protect("a%5C..%5Cpart.parquet").Rejection);
            Assert.Equal(PathRejection.Encoding, identity.Name("a\ud800.parquet", PathRole.Protection).Rejection);
            Assert.Equal(PathRejection.Encoding, identity.Name("a\udc00.parquet", PathRole.Protection).Rejection);
            Assert.Equal(PathRejection.Encoding, identity.Name("a\ud800", PathRole.Protection).Rejection);
            Assert.Equal("a\U0001F600.parquet", identity.Name("a\U0001F600.parquet", PathRole.Protection).Key);

            // The decoded path may be at most 64 KiB
            Assert.True(Protect(new string('a', PathCanonicalizer.MaxPathBytes)).Accepted);
            Assert.Equal(PathRejection.TooLong, Protect(new string('a', PathCanonicalizer.MaxPathBytes + 1)).Rejection);
            Assert.Equal(PathRejection.TooLong, Protect(string.Concat(Enumerable.Repeat("%C3%A9", PathCanonicalizer.MaxPathBytes / 2 + 1))).Rejection);

            // JSON escapes resolve first, then one URI decode
            var references = Visit("""{"add":{"path":"\u002541%2525.parquet"}}""");
            Assert.Equal("%41%2525.parquet", references.Single().Value);
            Assert.Equal("A%25.parquet", identity.Reference(new PathReference(ReferenceSource.Add, references.Single().Value, true), PathRole.Protection).Key);
            var protection = NewSet(PathRole.Protection);
            var extractor = new CommitReferenceExtractor(identity, protection, PathRole.Protection);
            Assert.True(extractor.AddLine(Encoding.UTF8.GetBytes("""{"add":{"path":"a+b%2Bc%2520d.parquet"}}""")));
            Assert.False(extractor.AddLine(Encoding.UTF8.GetBytes("""{"add":{"path":"\ud800.parquet"}}""")));
            Assert.Equal(PathRejection.Encoding, extractor.Declined);
            protection.Seal();
            Assert.Equal("a+b+c%20d.parquet", Entries(protection).Single().Key);
        }

        [Fact]
        public void InventoryNamesAreNotDecoded()
        {
            // A listed name is the physical name, so its escapes stay as they are
            var cloud = PathCanonicalizer.Create(S3());
            Assert.Equal("a%2541.parquet", cloud.Name("a%2541.parquet", PathRole.Protection).Key);
            Assert.Equal(PathRejection.Unqualified, cloud.Name("part%2D1.parquet", PathRole.Candidate).Rejection);
            Assert.Equal(PathRejection.Unqualified, cloud.Name("part#1.parquet", PathRole.Candidate).Rejection);
            Assert.Equal(("part-1.parquet", "/c/t/part-1.parquet"), (cloud.Name("part-1.parquet", PathRole.Candidate).Key, cloud.Name("part-1.parquet", PathRole.Candidate).DeletionPath));

            foreach (var portable in new[] { false, true })
            {
                var table = Local();
                Touch(table, "a%41.parquet");
                Touch(table, "aA.parquet");
                Touch(table, "b%2B.parquet");
                var identity = Native(table, portable);

                var candidates = NewSet(PathRole.Candidate);
                var listed = new CommitReferenceExtractor(identity, candidates, PathRole.Candidate);
                Assert.True(listed.AddName("a%41.parquet"));
                Assert.True(listed.AddName("aA.parquet"));
                Assert.True(listed.AddName("b%2B.parquet"));
                Assert.Equal(0, listed.Kept);

                // The log names the physical "a%41.parquet" as "a%2541.parquet"
                var protection = NewSet(PathRole.Protection);
                var references = new CommitReferenceExtractor(identity, protection, PathRole.Protection);
                Assert.True(references.AddLine(Encoding.UTF8.GetBytes("""{"add":{"path":"a%2541.parquet"}}""")));
                Assert.True(references.AddLine(Encoding.UTF8.GetBytes("""{"add":{"path":"b%252B.parquet"}}""")));

                Assert.Equal(new[] { "/t/aA.parquet" }, Deletable(candidates, protection));
            }
        }

        [Fact]
        public void AbsoluteAliasProtectsNeverDeletes()
        {
            foreach (var portable in new[] { false, true })
            {
                var table = Local();
                Touch(table, "part-1.parquet");
                Touch(table, "part-2.parquet");
                Touch(table, "part-3.parquet");
                var identity = Native(table, portable);
                var forward = table.Native.Replace('\\', '/');
                var absolute = OperatingSystem.IsWindows()
                    ? new[] { "file:///" + forward + "/part-1.parquet", "file:/" + forward + "/part-2.parquet", forward + "/part-3.parquet" }
                    : new[] { "file://" + forward + "/part-1.parquet", "file://localhost" + forward + "/part-2.parquet", forward + "/part-3.parquet" };
                if (portable && OperatingSystem.IsWindows())
                {
                    // Drive paths need the Windows identity
                    Assert.All(absolute, x => Assert.Equal(PathRejection.Outside, identity.Reference(Uri(x), PathRole.Protection).Rejection));
                    continue;
                }

                var protection = NewSet(PathRole.Protection);
                var references = new CommitReferenceExtractor(identity, protection, PathRole.Protection);
                foreach (var value in absolute)
                {
                    Assert.True(references.AddLine(Encoding.UTF8.GetBytes($$$"""{"add":{"path":"{{{value}}}"}}""")), references.DeclineDetail);
                }

                // Absolute references are never candidates, even inside the table
                var candidates = NewSet(PathRole.Candidate);
                var collector = new CommitReferenceExtractor(identity, candidates, PathRole.Candidate);
                foreach (var value in absolute)
                {
                    Assert.True(collector.AddLine(Encoding.UTF8.GetBytes($$$"""{"remove":{"path":"{{{value}}}"}}""")));
                }
                Assert.Equal(3, collector.KeptFor(PathRejection.Absolute));
                Assert.Equal(0, collector.Added);
                Assert.True(collector.AddName("part-1.parquet"));
                Assert.True(collector.AddName("part-2.parquet"));
                Assert.True(collector.AddName("part-3.parquet"));
                Assert.True(collector.AddName("part-4.parquet"));
                Assert.Equal(new[] { "/t/part-4.parquet" }, Deletable(candidates, protection));

                // An absolute reference outside the table cannot be placed, so protection declines
                var other = Path.Combine(_directory, "other").Replace('\\', '/');
                var outside = new CommitReferenceExtractor(identity, NewSet(PathRole.Protection), PathRole.Protection);
                Assert.False(outside.AddLine(Encoding.UTF8.GetBytes($$$"""{"add":{"path":"file://{{{(OperatingSystem.IsWindows() ? "/" : "")}}}{{{other}}}/x.parquet"}}""")));
                Assert.Equal(PathRejection.Outside, outside.Declined);
                Assert.Equal(PathRejection.Outside, identity.Reference(Uri("s3://c/t/part-1.parquet"), PathRole.Protection).Rejection);
                Assert.Equal(PathRejection.Namespace, identity.Reference(Uri("file://server/share/t/part-1.parquet"), PathRole.Protection).Rejection);
            }

            // Cloud keys: every scheme alias of the bucket or container, and paths from its root
            var s3 = PathCanonicalizer.Create(S3());
            foreach (var value in new[] { "s3://c/t/part-1.parquet", "s3a://c/t/part-1.parquet", "S3N://c/t/part-1.parquet", "/t/part-1.parquet", "s3://c/t/part%2D1.parquet" })
            {
                Assert.Equal("part-1.parquet", s3.Reference(Uri(value), PathRole.Protection).Key);
                Assert.Equal(PathRejection.Absolute, s3.Reference(Uri(value), PathRole.Candidate).Rejection);
            }
            foreach (var value in new[] { "s3://other/t/part-1.parquet", "s3://c/u/part-1.parquet", "s3://c/t", "s3://c/tt/part-1.parquet", "gs://c/t/part-1.parquet", "abfss://c@a.dfs.core.windows.net/t/part-1.parquet", "s3:/c/t/part-1.parquet" })
            {
                Assert.Equal(PathRejection.Outside, s3.Reference(Uri(value), PathRole.Protection).Rejection);
            }
            Assert.Equal(PathRejection.Traversal, s3.Reference(Uri("s3://c/t/../t/part-1.parquet"), PathRole.Protection).Rejection);
            var azure = PathCanonicalizer.Create(Azure());
            foreach (var value in new[] { "abfss://c@account.dfs.core.windows.net/t/part-1.parquet", "wasbs://c@account.blob.core.windows.net/t/part-1.parquet", "abfs://c@x/t/part-1.parquet" })
            {
                Assert.Equal("part-1.parquet", azure.Reference(Uri(value), PathRole.Protection).Key);
            }
            Assert.Equal(PathRejection.Outside, azure.Reference(Uri("abfss://d@account.dfs.core.windows.net/t/part-1.parquet"), PathRole.Protection).Rejection);
            Assert.Equal(PathRejection.Outside, azure.Reference(Uri("abfss://account.dfs.core.windows.net/t/part-1.parquet"), PathRole.Protection).Rejection);
            Assert.Equal(PathRejection.Outside, azure.Reference(Uri("abfss://c/t/part-1.parquet"), PathRole.Protection).Rejection);
            Assert.Equal(PathRejection.Outside, azure.Reference(Uri("https://account.blob.core.windows.net/c/t/part-1.parquet"), PathRole.Protection).Rejection);

            // An absolute vector container inside the table protects it and is never a candidate
            var line = $$$"""{"add":{"path":"part-5.parquet","deletionVector":{{{Vector("p", "s3://c/t/dv/deletion%5Fvector.bin", 3)}}}}}""";
            var vectorProtection = NewSet(PathRole.Protection);
            Assert.True(new CommitReferenceExtractor(s3, vectorProtection, PathRole.Protection).AddLine(Encoding.UTF8.GetBytes(line)));
            vectorProtection.Seal();
            Assert.Equal(new[] { "dv/deletion_vector.bin", "part-5.parquet" }, Entries(vectorProtection).Select(x => x.Key));
            var vectorCandidates = new CommitReferenceExtractor(s3, NewSet(PathRole.Candidate), PathRole.Candidate);
            Assert.True(vectorCandidates.AddLine(Encoding.UTF8.GetBytes(line)));
            Assert.Equal(1, vectorCandidates.KeptFor(PathRejection.Absolute));
            Assert.Equal(1, vectorCandidates.Added);
        }

        [Fact]
        public void CloudCaseVariantsStayDistinct()
        {
            foreach (var store in new[] { S3(), Azure() })
            {
                var identity = PathCanonicalizer.Create(store);
                Assert.Equal(PathIdentity.Cloud, identity.Identity);
                var candidates = NewSet(PathRole.Candidate);
                var listed = new CommitReferenceExtractor(identity, candidates, PathRole.Candidate);
                foreach (var name in new[] { "Part-1.parquet", "part-1.parquet", "PART-1.PARQUET", "dir/part-1.parquet", "DIR/part-1.parquet" })
                {
                    Assert.True(listed.AddName(name));
                }
                var protection = NewSet(PathRole.Protection);
                var references = new CommitReferenceExtractor(identity, protection, PathRole.Protection);
                Assert.True(references.AddLine(Encoding.UTF8.GetBytes("""{"add":{"path":"part-1.parquet"}}""")));
                Assert.True(references.AddLine(Encoding.UTF8.GetBytes("""{"remove":{"path":"dir/part-1.parquet"}}""")));

                Assert.Equal(new[] { "/c/t/DIR/part-1.parquet", "/c/t/PART-1.PARQUET", "/c/t/Part-1.parquet" }, Deletable(candidates, protection));
                Assert.Equal(5, candidates.Count);
                Assert.Equal(new[] { "DIR/part-1.parquet", "PART-1.PARQUET", "Part-1.parquet", "dir/part-1.parquet", "part-1.parquet" }, Entries(candidates).Select(x => x.Key));
            }
        }

        [Fact]
        public void AzureTrailingDotSegmentsAreRefused()
        {
            var azure = PathCanonicalizer.Create(Azure());
            Assert.Equal(PathRejection.NameSyntax, azure.Name("part-1.parquet.", PathRole.Candidate).Rejection);
            Assert.Equal(PathRejection.NameSyntax, azure.Name("dir./part-1.parquet", PathRole.Candidate).Rejection);
            Assert.Equal(PathRejection.NameSyntax, azure.Reference(Uri("dir./part-1.parquet"), PathRole.Protection).Rejection);
            Assert.True(azure.Name("dir.x/part-1.parquet", PathRole.Candidate).Accepted);
            // S3 keys keep their dots
            var s3 = PathCanonicalizer.Create(S3());
            Assert.Equal("/c/t/part-1.parquet.", s3.Name("part-1.parquet.", PathRole.Candidate).DeletionPath);
        }

        [Theory]
        [InlineData(false)]
        [InlineData(true)]
        public void LocalDiskAsciiCaseVariantsProtect(bool portable)
        {
            var table = Local();
            Touch(table, "part-1.parquet");
            Touch(table, "dir/part-2.parquet");
            var identity = Native(table, portable);

            var candidates = NewSet(PathRole.Candidate);
            var listed = new CommitReferenceExtractor(identity, candidates, PathRole.Candidate);
            foreach (var name in new[] { "part-1.parquet", "dir/part-2.parquet", "part-3.parquet", "part-9.parquet" })
            {
                Assert.True(listed.AddName(name));
            }
            var protection = NewSet(PathRole.Protection);
            var references = new CommitReferenceExtractor(identity, protection, PathRole.Protection);
            Assert.True(references.AddLine(Encoding.UTF8.GetBytes("""{"add":{"path":"PART-1.PARQUET"}}""")));
            Assert.True(references.AddLine(Encoding.UTF8.GetBytes("""{"remove":{"path":"Dir/Part-2.Parquet"}}""")));
            // Absent on a case-sensitive file system, it still protects its case variant
            Assert.True(references.AddLine(Encoding.UTF8.GetBytes("""{"cdc":{"path":"Part-9.parquet"}}""")));

            Assert.Equal(new[] { "/t/part-3.parquet" }, Deletable(candidates, protection));
            Assert.Equal(new[] { "DIR/PART-2.PARQUET", "PART-1.PARQUET", "PART-9.PARQUET" }, Entries(protection).Select(x => x.Key));
            Assert.Contains(("PART-1.PARQUET", "/t/part-1.parquet"), Entries(candidates));
        }

        [Theory]
        [InlineData(false)]
        [InlineData(true)]
        public void NativeNonAsciiProtectionDeclines(bool portable)
        {
            // The table's own folder may be Unicode
            var table = Local("t\u00E5ble-" + ScratchOwners.ShortId());
            Touch(table, "part-1.parquet");
            Touch(table, "d\u00E4t\u00E4.parquet");
            var identity = Native(table, portable);

            var protection = NewSet(PathRole.Protection);
            var references = new CommitReferenceExtractor(identity, protection, PathRole.Protection);
            Assert.True(references.AddLine(Encoding.UTF8.GetBytes("""{"add":{"path":"part-1.parquet"}}""")));
            Assert.False(references.AddLine(Encoding.UTF8.GetBytes("""{"add":{"path":"d%C3%A4t%C3%A4.parquet"}}""")));
            Assert.Equal(PathRejection.NonAscii, references.Declined);
            // Refused before any lookup
            var resolutions = identity.NativeResolutions;
            Assert.Equal(PathRejection.NonAscii, identity.Reference(Uri("d\u00E4t\u00E4.parquet"), PathRole.Protection).Rejection);
            Assert.Equal(PathRejection.NonAscii, identity.Reference(new PathReference(ReferenceSource.AddVector, "\u00E4/deletion_vector_x.bin", false), PathRole.Protection).Rejection);
            Assert.Equal(resolutions, identity.NativeResolutions);

            // A non-ASCII candidate is kept and counted, never added
            var candidates = NewSet(PathRole.Candidate);
            var listed = new CommitReferenceExtractor(identity, candidates, PathRole.Candidate);
            Assert.True(listed.AddName("d\u00E4t\u00E4.parquet"));
            Assert.True(listed.AddName("part-1.parquet"));
            Assert.Equal(1, listed.KeptFor(PathRejection.NonAscii));
            candidates.Seal();
            Assert.Equal(new[] { ("PART-1.PARQUET", "/t/part-1.parquet") }, Entries(candidates));
        }

        [WindowsFact]
        public void WindowsShortAliasesWithAndWithoutTildeProtect()
        {
            var table = Local();
            var leaf = "part-00000-" + Guid.NewGuid().ToString("N") + ".snappy.parquet";
            Touch(table, leaf);
            Touch(table, "partition=long-value/inner-file-name.parquet");
            var shortLeaf = ShortName(Path.Combine(table.Native, leaf));
            var shortDirectory = ShortName(Path.Combine(table.Native, "partition=long-value"));
            if (shortLeaf == null || shortDirectory == null || shortLeaf.Equals(leaf, StringComparison.OrdinalIgnoreCase))
            {
                _output.WriteLine($"SKIPPED: 8.3 names are disabled on the volume of {table.Native}");
                return;
            }
            Assert.Contains('~', shortLeaf);
            _output.WriteLine($"Short aliases {shortLeaf} and {shortDirectory}");

            // A short alias protects its long name, and a candidate named by its alias deletes by its long name
            var identity = Native(table, portable: false);
            var protection = NewSet(PathRole.Protection);
            var references = new CommitReferenceExtractor(identity, protection, PathRole.Protection);
            Assert.True(references.AddLine(Encoding.UTF8.GetBytes($$$"""{"add":{"path":"{{{shortLeaf}}}"}}""")));
            Assert.True(references.AddLine(Encoding.UTF8.GetBytes($$$"""{"add":{"path":"{{{shortDirectory}}}/inner-file-name.parquet"}}""")));
            var candidates = NewSet(PathRole.Candidate);
            var listed = new CommitReferenceExtractor(identity, candidates, PathRole.Candidate);
            Assert.True(listed.AddName(leaf));
            Assert.True(listed.AddName("partition=long-value/inner-file-name.parquet"));
            Assert.True(listed.AddName("unreferenced.parquet"));
            Assert.Equal(new[] { "/t/unreferenced.parquet" }, Deletable(candidates, protection));

            var aliasCandidate = identity.Name(shortLeaf, PathRole.Candidate);
            Assert.Equal("/t/" + leaf, aliasCandidate.DeletionPath);
            Assert.Equal(leaf.ToUpperInvariant(), aliasCandidate.Key);

            // An ASCII alias of a non-ASCII or trailing-dot long name stands for an unsupported name
            var unicode = "d\u00E4t\u00E4-long-name.parquet";
            Touch(table, unicode);
            var trailing = @"\\?\" + Path.Combine(table.Native, "trailing-name.parquet.");
            File.WriteAllText(trailing, "x");
            try
            {
                var unicodeAlias = ShortName(Path.Combine(table.Native, unicode))!;
                var trailingAlias = ShortName(trailing)!;
                Assert.True(unicodeAlias.All(char.IsAscii), unicodeAlias);
                Assert.True(trailingAlias.All(char.IsAscii), trailingAlias);
                Assert.Equal(PathRejection.NonAscii, identity.Reference(Uri(unicodeAlias), PathRole.Protection).Rejection);
                Assert.Equal(PathRejection.NonAscii, identity.Name(unicodeAlias, PathRole.Candidate).Rejection);
                Assert.Equal(PathRejection.NameSyntax, identity.Reference(Uri(trailingAlias), PathRole.Protection).Rejection);
                Assert.Equal(PathRejection.NameSyntax, identity.Name(trailingAlias, PathRole.Candidate).Rejection);
            }
            finally
            {
                File.Delete(trailing);
            }

            // An alias needs no tilde: every existing path is resolved, whatever its spelling
            const string tildeless = "ALIAS.PAR";
            var aliases = new AliasNames(tildeless, leaf);
            var resolving = Native(table, portable: false, aliases);
            Assert.Equal(leaf.ToUpperInvariant(), resolving.Reference(Uri(tildeless), PathRole.Protection).Key);
            Assert.Equal("/t/" + leaf, resolving.Name(tildeless, PathRole.Candidate).DeletionPath);
            Assert.True(aliases.Resolved > 0);
            _output.WriteLine("The tilde-less alias is mapped by a lookup override, as setting one on the volume needs the restore privilege");
        }

        // The OS lookups, except that one tilde-less alias resolves to a long name as SetFileShortName would make it
        private sealed class AliasNames : WindowsNativeNames
        {
            private readonly string _alias;
            private readonly string _target;

            public AliasNames(string alias, string target)
            {
                _alias = "\\" + alias;
                _target = "\\" + target;
            }

            public int Resolved { get; private set; }

            private string Map(string path)
            {
                if (!path.EndsWith(_alias, StringComparison.OrdinalIgnoreCase))
                {
                    return path;
                }
                Resolved++;
                return path.Substring(0, path.Length - _alias.Length) + _target;
            }

            public override NativeStatus LongPath(string path, out string longPath) => base.LongPath(Map(path), out longPath);

            public override NativeStatus Attributes(string path, out FileAttributes attributes) => base.Attributes(Map(path), out attributes);
        }

        // Lookups of the platform that refuse or fail chosen names
        private sealed class FailingNames : NativeNames
        {
            private readonly NativeNames _inner;

            public FailingNames(NativeNames inner)
            {
                _inner = inner;
            }

            public Func<string, NativeStatus?> Fail { get; set; } = _ => null;

            // A long form the lookup reports instead of the real one
            public Func<string, string?> Rewrite { get; set; } = _ => null;

            public override NativeStatus LongPath(string path, out string longPath)
            {
                longPath = path;
                if (Rewrite(path) is { } rewritten)
                {
                    longPath = rewritten;
                    return NativeStatus.Found;
                }
                return Fail(path) ?? _inner.LongPath(path, out longPath);
            }

            public override NativeStatus Attributes(string path, out FileAttributes attributes)
            {
                attributes = default;
                return Fail(path) ?? _inner.Attributes(path, out attributes);
            }
        }

        private static NativeNames PlatformNames(bool portable) => OperatingSystem.IsWindows() && !portable ? new WindowsNativeNames() : NativeNames.Portable;

        [Theory]
        [InlineData(false)]
        [InlineData(true)]
        public void PathResolutionFailureDeclines(bool portable)
        {
            var table = Local();
            Touch(table, "denied.parquet");
            Touch(table, "broken.parquet");
            Touch(table, "part-1.parquet");
            var names = new FailingNames(PlatformNames(portable))
            {
                Fail = path => path.EndsWith("denied.parquet", StringComparison.Ordinal) ? NativeStatus.AccessDenied
                    : path.EndsWith("broken.parquet", StringComparison.Ordinal) ? NativeStatus.Failed
                    : null
            };
            var identity = Native(table, portable, names);

            var refused = new CommitReferenceExtractor(identity, NewSet(PathRole.Protection), PathRole.Protection);
            Assert.True(refused.AddLine(Encoding.UTF8.GetBytes("""{"add":{"path":"part-1.parquet"}}""")));
            Assert.False(refused.AddLine(Encoding.UTF8.GetBytes("""{"add":{"path":"denied.parquet"}}""")));
            Assert.Equal(PathRejection.AccessDenied, refused.Declined);
            Assert.Equal(ReferenceSource.Add, refused.DeclinedSource);
            Assert.False(refused.AddLine(Encoding.UTF8.GetBytes("""{"add":{"path":"part-1.parquet"}}""")));
            Assert.Equal(PathRejection.Unresolved, identity.Reference(Uri("broken.parquet"), PathRole.Protection).Rejection);

            var candidates = NewSet(PathRole.Candidate);
            var listed = new CommitReferenceExtractor(identity, candidates, PathRole.Candidate);
            Assert.True(listed.AddName("denied.parquet"));
            Assert.True(listed.AddName("broken.parquet"));
            Assert.True(listed.AddName("part-1.parquet"));
            Assert.Equal(1, listed.KeptFor(PathRejection.AccessDenied));
            Assert.Equal(1, listed.KeptFor(PathRejection.Unresolved));
            candidates.Seal();
            Assert.Equal(new[] { "/t/part-1.parquet" }, Entries(candidates).Select(x => x.Path));

            if (OperatingSystem.IsWindows() && !portable)
            {
                // A long form in another folder, or with other segments than the name, is not trusted even where such files exist
                var twin = Local();
                Assert.Equal(table.Native.Length, twin.Native.Length);
                Touch(table, "inside.parquet");
                Touch(twin, "inside.parquet");
                Touch(table, "a/split.parquet");
                var rewriting = new FailingNames(new WindowsNativeNames())
                {
                    Rewrite = path => path.EndsWith(@"\inside.parquet", StringComparison.Ordinal) ? @"\\?\" + Path.Combine(twin.Native, "inside.parquet")
                        : path.EndsWith(@"\split.parquet", StringComparison.Ordinal) && !path.EndsWith(@"\a\split.parquet", StringComparison.Ordinal) ? path.Substring(0, path.Length - "split.parquet".Length) + @"a\split.parquet"
                        : null
                };
                var rewritten = Native(table, portable, rewriting);
                Assert.True(rewritten.Reference(Uri("part-1.parquet"), PathRole.Protection).Accepted);
                Assert.True(rewritten.Reference(Uri("a/split.parquet"), PathRole.Protection).Accepted);
                Assert.Equal(PathRejection.Unresolved, rewritten.Reference(Uri("inside.parquet"), PathRole.Protection).Rejection);
                Assert.Equal(PathRejection.Unresolved, rewritten.Reference(Uri("split.parquet"), PathRole.Protection).Rejection);
            }

            // A table folder that is refused, a file or missing resolves nothing
            var rootRefused = new FailingNames(PlatformNames(portable)) { Fail = path => path.EndsWith(Path.DirectorySeparatorChar + "t", StringComparison.Ordinal) ? NativeStatus.AccessDenied : null };
            Assert.Equal(PathRejection.AccessDenied, Native(table, portable, rootRefused).Reference(Uri("part-1.parquet"), PathRole.Protection).Rejection);
            var file = Local();
            Directory.Delete(file.Native);
            File.WriteAllText(file.Native, "x");
            Assert.Equal(PathRejection.Unresolved, Native(file, portable).Reference(Uri("part-1.parquet"), PathRole.Protection).Rejection);
            var missing = Local();
            Directory.Delete(missing.Native);
            Assert.Equal(PathRejection.Unresolved, Native(missing, portable).Reference(Uri("part-1.parquet"), PathRole.Protection).Rejection);
            Assert.Equal(PathRejection.Unresolved, Native(missing, portable).Name("part-1.parquet", PathRole.Candidate).Rejection);
        }

        [Theory]
        [InlineData(false)]
        [InlineData(true)]
        public void NativeReparsePointsAndDirectoriesDecline(bool portable)
        {
            var table = Local();
            Touch(table, "dir/part-1.parquet");
            var identity = Native(table, portable);
            Assert.Equal(PathRejection.Directory, identity.Reference(Uri("dir"), PathRole.Protection).Rejection);
            Assert.Equal(PathRejection.Directory, identity.Name("dir", PathRole.Candidate).Rejection);
            Assert.True(identity.Reference(Uri("dir/part-1.parquet"), PathRole.Protection).Accepted);
            // Nothing exists below a file
            Assert.True(identity.Reference(Uri("dir/part-1.parquet/x"), PathRole.Protection).Accepted);

            var target = Path.Combine(_directory, "elsewhere-" + ScratchOwners.ShortId());
            Directory.CreateDirectory(target);
            File.WriteAllText(Path.Combine(target, "part-1.parquet"), "x");
            if (!ScratchDirectoryTests.TryLink(Path.Combine(table.Native, "link"), target, out var how))
            {
                _output.WriteLine($"SKIPPED the link half: the OS created no link ({how})");
                return;
            }
            _output.WriteLine($"Linked with a {how}");
            try
            {
                var linked = Native(table, portable);
                Assert.Equal(PathRejection.Reparse, linked.Reference(Uri("link/part-1.parquet"), PathRole.Protection).Rejection);
                Assert.Equal(PathRejection.Reparse, linked.Reference(Uri("link/absent.parquet"), PathRole.Protection).Rejection);
                Assert.Equal(PathRejection.Reparse, linked.Name("link/part-1.parquet", PathRole.Candidate).Rejection);
                Assert.Equal(PathRejection.Reparse, linked.Name("link", PathRole.Candidate).Rejection);
            }
            finally
            {
                Directory.Delete(Path.Combine(table.Native, "link"));
            }
        }

        [Fact]
        public void WindowsNameSyntaxIsRefused()
        {
            foreach (var name in new[] { "a.", "a ", "dir./a", "a:b", "a::$DATA", "a*b", "a?b", "a<b", "a>b", "a|b", "a\"b", "CON", "con.parquet", "Nul .txt", "LPT1.parquet", "com9", "COM\u00B9", "CONIN$", "conout$.x", "CLOCK$" })
            {
                Assert.Equal(PathRejection.NameSyntax, PathCanonicalizer.CheckWindowsSegments(name, '/'));
            }
            foreach (var name in new[] { "a", "con-x.parquet", "COM10", "LPT", "consol.parquet", " a", "a.b", "dir/part~1.par", "a b" })
            {
                Assert.Equal(PathRejection.None, PathCanonicalizer.CheckWindowsSegments(name, '/'));
            }
            Assert.Equal(PathRejection.Traversal, PathCanonicalizer.CheckWindowsSegments("a//b", '/'));
            Assert.Equal(PathRejection.Syntax, PathCanonicalizer.CheckWindowsSegments("a\u0001", '/'));
        }

        [WindowsFact]
        public void WindowsIdentityRefusesNameSyntax()
        {
            var table = Local();
            Touch(table, "part-1.parquet");
            var identity = Native(table, portable: false);
            foreach (var value in new[] { "part-1.parquet.", "part-1.parquet%20", "dir/part-1.parquet:stream", "dir/part-1.parquet::$DATA", "part-*.parquet", "NUL.parquet", "dir%2E/part-1.parquet" })
            {
                Assert.Equal(PathRejection.NameSyntax, identity.Reference(Uri(value), PathRole.Protection).Rejection);
                Assert.Equal(PathRejection.NameSyntax, identity.Reference(Uri(value), PathRole.Candidate).Rejection);
            }
            Assert.True(identity.Reference(Uri("part-1.parquet"), PathRole.Protection).Accepted);
        }

        [Fact]
        public void AssertedUnknownStoreUsesFoldedAsciiIdentity()
        {
            var store = CleanupStoreContract.Classify(Files.Of.InternalMemory(nameof(AssertedUnknownStoreUsesFoldedAsciiIdentity)), "t", true);
            var identity = PathCanonicalizer.Create(store);
            Assert.Equal(PathIdentity.Folded, identity.Identity);
            var target = identity.Name("Part-1.parquet", PathRole.Candidate);
            Assert.Equal(("PART-1.PARQUET", "/t/Part-1.parquet"), (target.Key, target.DeletionPath));
            Assert.Equal("PART-1.PARQUET", identity.Reference(Uri("part-1.parquet"), PathRole.Protection).Key);
            Assert.Equal(PathRejection.NonAscii, identity.Reference(Uri("d%C3%A4.parquet"), PathRole.Protection).Rejection);
            Assert.Equal(PathRejection.NameSyntax, identity.Reference(Uri("PART-0~1.PAR"), PathRole.Protection).Rejection);
            Assert.Equal(PathRejection.Outside, identity.Reference(Uri("file:///t/part-1.parquet"), PathRole.Protection).Rejection);
            // Without the assertion nothing is a target
            var unasserted = PathCanonicalizer.Create(CleanupStoreContract.Classify(new ForwardingStorage(Files.Of.InternalMemory("x")), "t", false));
            Assert.Equal(PathRejection.Unqualified, unasserted.Name("part-1.parquet", PathRole.Candidate).Rejection);
        }

        [Theory]
        [InlineData("part-1.parquet.")]
        [InlineData("part-1.parquet%20")]
        [InlineData("part-1.parquet::$DATA")]
        public void FoldedProtectionDeclinesSpellingsTheStackMayAlias(string spelling)
        {
            var store = CleanupStoreContract.Classify(Files.Of.InternalMemory(nameof(FoldedProtectionDeclinesSpellingsTheStackMayAlias)), "t", true);
            var identity = PathCanonicalizer.Create(store);
            Assert.Equal(PathIdentity.Folded, identity.Identity);
            var candidate = identity.Name("part-1.parquet", PathRole.Candidate);
            // The candidate side refuses these spellings as targets, so protection must not accept them under another key
            var protection = identity.Reference(new PathReference(ReferenceSource.Add, spelling, isUri: true), PathRole.Protection);
            Assert.True(!protection.Accepted || protection.Key == candidate.Key, $"{spelling} protects key {protection.Key}, the candidate's key is {candidate.Key}");
        }

        [Theory]
        [InlineData("part-1.parquet.", "part-1.parquet.")]
        [InlineData("part-1.parquet%20", "part-1.parquet ")]
        [InlineData("dir%2E/part-1.parquet", "dir./part-1.parquet")]
        [InlineData("dir%20/part-1.parquet", "dir /part-1.parquet")]
        [InlineData("dir/part-1.parquet%3A%3A$DATA", "dir/part-1.parquet::$DATA")]
        [InlineData("dir/part-1.parquet%3Astream", "dir/part-1.parquet:stream")]
        [InlineData("PART-1~1.PAR", "PART-1~1.PAR")]
        [InlineData("DIR~1/part-1.parquet", "DIR~1/part-1.parquet")]
        public void FoldedAndPortableIdentitiesDeclineAliasSpellingsInBothRoles(string reference, string name)
        {
            var table = Local();
            Touch(table, "part-1.parquet");
            Touch(table, "dir/part-1.parquet");
            var folded = PathCanonicalizer.Create(CleanupStoreContract.Classify(Files.Of.InternalMemory(nameof(FoldedAndPortableIdentitiesDeclineAliasSpellingsInBothRoles)), "t", true));
            Assert.Equal(PathIdentity.Folded, folded.Identity);
            foreach (var identity in new[] { folded, Native(table, portable: true) })
            {
                Assert.Equal(PathRejection.NameSyntax, identity.Reference(Uri(reference), PathRole.Protection).Rejection);
                Assert.Equal(PathRejection.NameSyntax, identity.Name(name, PathRole.Protection).Rejection);
                Assert.Equal(PathRejection.NameSyntax, identity.Name(name, PathRole.Candidate).Rejection);
                // The plain spellings stay usable
                Assert.True(identity.Reference(Uri("dir/part-1.parquet"), PathRole.Protection).Accepted);
                Assert.True(identity.Name("part-1.parquet", PathRole.Candidate).Accepted);
            }
            // The protection decline stops the extractor, so payload planning declines
            var protection = NewSet(PathRole.Protection);
            var references = new CommitReferenceExtractor(folded, protection, PathRole.Protection);
            Assert.False(references.AddLine(Encoding.UTF8.GetBytes($$$"""{"add":{"path":"{{{reference}}}"}}""")));
            Assert.Equal(PathRejection.NameSyntax, references.Declined);
        }

        [Fact]
        public async Task NativeResolutionIsChargedAsOwedRequests()
        {
            var table = Local();
            Touch(table, "part-1.parquet");
            var buckets = new MaintenanceBuckets(new FakeTimeProvider(DateTimeOffset.UnixEpoch), requestBurst: 1000);
            var metered = MeteredFileStorage.Wrap(table.Storage, buckets, ListingPaging.LocalDisk);
            var identity = PathCanonicalizer.Create(table.Store, metered);
            Assert.True(identity.Name("part-1.parquet", PathRole.Candidate).Accepted);
            Assert.True(identity.Reference(Uri("part-2.parquet"), PathRole.Protection).Accepted);
            // Rejected before any lookup
            Assert.False(identity.Reference(Uri("../part-3.parquet"), PathRole.Protection).Accepted);
            // The table folder, then one request per resolved path
            Assert.Equal(3, identity.Owed);
            Assert.Equal(0, buckets.NativeRequests);
            await identity.ChargeOwedAsync(default);
            Assert.Equal(0, identity.Owed);
            Assert.Equal(3, buckets.NativeRequests);
            Assert.Equal(3, identity.NativeResolutions);

            // Cloud keys need no lookup
            var cloud = PathCanonicalizer.Create(S3(), metered);
            Assert.True(cloud.Name("part-1.parquet", PathRole.Candidate).Accepted);
            Assert.Equal(0, cloud.Owed);
        }

        internal static string? ShortName(string path)
        {
            var buffer = new char[1024];
            var length = GetShortPathNameW(path, buffer, (uint)buffer.Length);
            if (length == 0 || length >= buffer.Length)
            {
                return null;
            }
            return Path.GetFileName(new string(buffer, 0, (int)length));
        }

        [DllImport("kernel32.dll", CharSet = CharSet.Unicode, SetLastError = true, ExactSpelling = true)]
        private static extern uint GetShortPathNameW(string longPath, [Out] char[] shortPath, uint bufferLength);
    }
}
