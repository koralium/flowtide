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

using FlowtideDotNet.Connector.DeltaLake.Internal.Delta.DeletionVectors;
using FlowtideDotNet.Connector.DeltaLake.Internal.Delta.ParquetFormat;
using FlowtideDotNet.Connector.DeltaLake.Internal.Delta.Utils;
using FlowtideDotNet.Connector.DeltaLake.Internal.Maintenance;
using FlowtideDotNet.Connector.DeltaLake.Internal.Maintenance.Deletion;
using FlowtideDotNet.Connector.DeltaLake.Internal.Maintenance.References;
using Stowage;
using static FlowtideDotNet.Connector.DeltaLake.Tests.DeltaTestKit;

namespace FlowtideDotNet.Connector.DeltaLake.Tests
{
    [Collection(nameof(PinnedClientCollection))]
    public class ObjectKeyDomainTests
    {
        private static readonly CleanupOptIns Retention = new CleanupOptIns(TimeSpan.FromDays(7), null);

        // Names a client could send unencoded so that the request names another object, and names outside the domain for other reasons
        private static readonly string[] s_unqualified =
        {
            "part#x.parquet", "part?x.parquet", "part%23x.parquet", "part+x.parquet", "part x.parquet", "part:x.parquet", "part~1.parquet",
            "part&x.parquet", "a=1/part.parquet", "part!.parquet", "part$x.parquet", "part@x.parquet", "part,x.parquet", "part;x.parquet"
        };

        // An unknown stack may be Azure underneath, which drops a segment's trailing dot
        private static readonly string[] s_trailingDots = { "x./part.parquet", "part.parquet." };

        [Fact]
        public async Task AssertedUnknownStackDeletesOnlyQualifiedExactKeys()
        {
            // A user decorator, vouched for, over objects that record every request
            var objects = new ScriptedStorage();
            var wrapper = new ForwardingStorage(objects);
            var configuration = CleanupConfiguration.Validate(wrapper, "t", true, CleanupOptIns.Off);
            Assert.Equal(StoreKind.Unknown, configuration.Store.Store.Kind);
            Assert.True(configuration.Store.Eligible);
            var executor = new DeletionExecutor(configuration, wrapper);
            const string Own = "part-00000-0f8fad5b-d9cb-469f-a165-70867728950e.zstd.parquet";
            objects.Put("/t/" + Own);
            foreach (var name in s_unqualified.Concat(s_trailingDots))
            {
                objects.Put("/t/" + name);
            }
            objects.Put("/t/dir/inner.parquet");

            foreach (var name in s_unqualified.Concat(s_trailingDots).Append("dir/"))
            {
                Assert.Equal(KeepReason.Unqualified, executor.TryTarget(name, TargetKind.Payload, out _));
                Assert.Equal(DeleteOutcome.Keep(KeepReason.Unqualified), await executor.DeleteAsync(new DeletionTarget("/t/" + name, TargetKind.Payload), false, default));
            }
            Assert.Empty(objects.Requests.Where(x => x.StartsWith("Rm ", StringComparison.Ordinal)));

            // The sink's own name is one exact delete
            Assert.Equal(KeepReason.None, executor.TryTarget(Own, TargetKind.Payload, out var target));
            Assert.Equal(DeleteStatus.Deleted, (await executor.DeleteAsync(target, false, default)).Status);
            Assert.Equal(new[] { "Rm /t/" + Own }, objects.Requests.Where(x => x.StartsWith("Rm ", StringComparison.Ordinal)));

            // Candidates outside the domain are kept, while protection still names them
            var identity = PathCanonicalizer.Create(configuration.Store);
            Assert.Equal(PathIdentity.Folded, identity.Identity);
            Assert.Equal(PathRejection.Unqualified, identity.Name("part#x.parquet", PathRole.Candidate).Rejection);
            Assert.True(identity.Name("part#x.parquet", PathRole.Protection).Accepted);
            Assert.Equal("/t/" + Own, identity.Name(Own, PathRole.Candidate).DeletionPath);

            // A table segment with a trailing dot qualifies no key below it
            Assert.False(CleanupStoreContract.Classify(wrapper, "t./u", true).TryQualifyTarget(Own, out _));

            // Every store but local disk is held to the domain, whatever its descriptor says
            var bare = CleanupStoreContract.Create(objects, new StoreDescriptor { Kind = StoreKind.Unknown, NeedsAssertion = true, ExactDeletes = true, Detail = "a wrapper" }, "t", true);
            Assert.False(bare.Store.QualifiedDomain);
            Assert.True(bare.TryQualifyTarget(Own, out _));
            Assert.All(s_unqualified, x => Assert.False(bare.TryQualifyTarget(x, out _), x));
            Assert.NotNull(CleanupStoreContract.Create(objects, bare.Store, "my t", true).PathProblem);

            // The pinned in-memory store is held to the same domain
            var memory = CleanupStoreContract.Classify(Files.Of.InternalMemory(nameof(AssertedUnknownStackDeletesOnlyQualifiedExactKeys)), "t", false);
            Assert.Equal(StoreKind.InMemory, memory.Store.Kind);
            Assert.True(memory.TryQualifyTarget(Own, out _));
            Assert.All(s_unqualified, x => Assert.False(memory.TryQualifyTarget(x, out _), x));
        }

        [Theory]
        [InlineData("my tables/t")]
        [InlineData("t+1")]
        [InlineData("t#1")]
        [InlineData("t%2F1")]
        public void UnknownStackNeedsTheWholeKeyQualified(string table)
        {
            var objects = new ScriptedStorage();
            var wrapper = new ForwardingStorage(objects);
            var store = CleanupStoreContract.Classify(wrapper, table, true);
            Assert.NotNull(store.PathProblem);
            Assert.False(store.Eligible);
            Assert.False(store.TryQualifyTarget("part-00000-1.parquet", out _));
            Assert.Equal(KeepReason.StoreUnsupported, new DeletionExecutor(CleanupConfiguration.Validate(wrapper, table, true, CleanupOptIns.Off), wrapper).TryTarget("part-00000-1.parquet", TargetKind.Payload, out _));
            // With an opt-in the table path is refused at construction
            var e = Assert.Throws<ArgumentException>(() => CleanupConfiguration.Validate(wrapper, table, true, Retention));
            Assert.Equal(CleanupConfiguration.StorageOption, e.ParamName);
            Assert.Contains($"segments of {CleanupStoreContract.QualifiedCharacters}", e.Message);
            Assert.Empty(objects.Requests);
        }

        [Fact]
        public async Task SinkGeneratedNamesStayInsideTheDomain()
        {
            var testName = nameof(SinkGeneratedNamesStayInsideTheDomain);
            var storage = new OwnedHookFileStorage(Files.Of.InternalMemory($"./{testName}"));
            await using var stream = new DeltaLakeSinkStream(testName, storage, options =>
            {
                options.WriteChangeDataOnNewTables = true;
                options.MaxFileSizeBytes = 2_000;
            });
            stream.Generate(200);
            await stream.StartStream(UserInsert);
            await WaitForVersion(storage, "test", stream, 0);
            // One delete writes a deletion vector
            stream.DeleteUser(stream.Users[0]);
            await WaitForVersion(storage, "test", stream, 1);

            // Every name the sink wrote, from the store's own record
            const string Prefix = "OpenWrite /test/";
            var written = storage.Requests.Where(x => x.StartsWith(Prefix, StringComparison.Ordinal)).Select(x => x.Substring(Prefix.Length)).Distinct().ToList();
            var part = written.First(x => x.StartsWith("part-00000-", StringComparison.Ordinal));
            Assert.Contains(written, x => x.StartsWith("_change_data/cdc-00000-", StringComparison.Ordinal));
            Assert.Contains(written, x => x.StartsWith("deletion_vector_", StringComparison.Ordinal));
            Assert.Contains(written, x => x.StartsWith("_delta_log/.", StringComparison.Ordinal) && x.EndsWith(".tmp", StringComparison.Ordinal));
            var names = new List<string>(written);

            // The same data name under every codec's extension
            var extension = DeltaParquetCompression.Resolve(null, null, out _).FileExtension;
            Assert.EndsWith(extension, part);
            var codecs = Enum.GetValues<DeltaCompressionCodec>().Select(x => DeltaParquetCompression.Resolve(null, x, out _))
                .Append(DeltaParquetCompression.Resolve(new Dictionary<string, string>() { [DeltaParquetCompression.CodecProperty] = "lz4" }, null, out _));
            names.AddRange(codecs.Select(x => part.Substring(0, part.Length - extension.Length) + x.FileExtension));

            // Deletion vector names, whatever characters their encoded id has, and with a random alphanumeric prefix
            var random = new Random(7);
            var bytes = new byte[16];
            for (int i = 0; i < 64; i++)
            {
                random.NextBytes(bytes);
                var encoded = Z85.EncodeGuid(new Guid(bytes));
                foreach (var stored in new[] { encoded, "ab" + encoded, "Z9" + encoded })
                {
                    names.Add(new DeletionVector() { StorageType = "u", PathOrInlineDv = stored }.GetAbsolutePath()!);
                }
            }
            names.Add(DeletionVectorWriter.GenerateDestination().path);

            // Checkpoint temporaries exist only on local disk, and every name the temporary rule accepts is in the domain too
            foreach (var id in new[] { Guid.NewGuid().ToString("N"), Guid.NewGuid().ToString("N").ToUpperInvariant() })
            {
                foreach (var suffix in new[] { ".checkpoint.tmp", ".last_checkpoint.tmp" })
                {
                    var temporary = ".flowtide-" + id + suffix;
                    Assert.True(DeletionExecutor.IsTemporary(temporary));
                    names.Add("_delta_log/" + temporary);
                }
            }

            using var s3 = StoreFixtures.S3();
            using var azure = StoreFixtures.Azure();
            var contracts = new[]
            {
                CleanupStoreContract.Classify(s3, "c/t", false),
                CleanupStoreContract.Classify(azure, "c/t", false),
                CleanupStoreContract.Classify(new ForwardingStorage(new ScriptedStorage()), "c/t", true),
                CleanupStoreContract.Classify(Files.Of.InternalMemory(testName + "-classified"), "c/t", false)
            };
            foreach (var contract in contracts)
            {
                foreach (var name in names)
                {
                    Assert.True(contract.TryQualifyTarget(name, out var target), $"{contract.Store.Kind}: {name}");
                    Assert.Equal("/c/t/" + name, target.Full);
                    Assert.Null(CleanupStoreContract.CheckSegments(target.Full.Substring(1), qualifiedDomain: true));
                }
            }
        }
    }
}
