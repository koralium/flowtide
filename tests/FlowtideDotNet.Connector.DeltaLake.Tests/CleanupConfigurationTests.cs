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

using FlowtideDotNet.Connector.DeltaLake.Internal;
using FlowtideDotNet.Connector.DeltaLake.Internal.Deletes;
using FlowtideDotNet.Connector.DeltaLake.Internal.Maintenance;
using FlowtideDotNet.Substrait.Relations;
using FlowtideDotNet.Substrait.Type;
using Stowage;
using System.Collections.Concurrent;
using System.Threading.Tasks.Dataflow;
using static FlowtideDotNet.Connector.DeltaLake.Tests.DeltaTestKit;

namespace FlowtideDotNet.Connector.DeltaLake.Tests
{
    [Collection(nameof(PinnedClientCollection))]
    public class CleanupConfigurationTests
    {
        private static readonly CleanupOptIns Retention = new CleanupOptIns(TimeSpan.FromDays(7), null);
        private static readonly CleanupOptIns RetentionAndOrphans = new CleanupOptIns(TimeSpan.FromDays(7), TimeSpan.FromDays(7));

        // Each sink construction case runs with retention alone and with orphan sweeping too
        private static readonly CleanupOptIns[] OptInVariants = { Retention, RetentionAndOrphans };

        // Names the pinned S3 and Azure clients would send unencoded, so the service could read another name or another request
        public static TheoryData<string> ReservedNames => new TheoryData<string>
        {
            "c/t+x", "c/t%2Fx", "c/t x", "c/t?x", "c/t#x", "c/t&x", "c/t=x", "c/té", "c/t~x", "c/t:x", "c/t\"x", "c/t*x",
            // A table path is required below the bucket or container
            "c"
        };

        internal static WriteRelation Relation(params string[] names)
        {
            var schema = new NamedStruct() { Names = new List<string>() { "a" }, Struct = new Struct() { Types = new List<SubstraitBaseType>() { new Int64Type() } } };
            return new WriteRelation()
            {
                Input = new ReadRelation() { BaseSchema = schema, NamedTable = new NamedTable() { Names = new List<string>() { "source" } } },
                TableSchema = schema,
                NamedObject = new NamedTable() { Names = names.ToList() }
            };
        }

        private static DeltaLakeSink Construct(IFileStorage storage, params string[] names)
        {
            return Construct(storage, null, names);
        }

        private static DeltaLakeSink Construct(IFileStorage storage, Action<DeltaLakeOptions>? configure, params string[] names)
        {
            var options = new DeltaLakeOptions() { StorageLocation = storage };
            configure?.Invoke(options);
            return new DeltaLakeSink(options, Relation(names), new ExecutionDataflowBlockOptions());
        }

        // The sink with the opt-ins on, as the table name's segments
        private static CleanupConfiguration ConstructRetained(IFileStorage storage, string table, CleanupOptIns optIns, bool assumeComplete = false)
        {
            return Construct(storage, options =>
            {
                options.HistoryRetention = optIns.Retention;
                options.OrphanFileSweepInterval = optIns.OrphanSweepInterval;
                options.AssumeCompleteStorageListing = assumeComplete;
            }, table.Split('/')).CleanupForTests;
        }

        // The sink refuses with the opt-ins on and constructs with them off
        private static void AssertSinkRefused(IFileStorage storage, string table, CleanupOptIns optIns, bool assumeComplete, string option, string remedy)
        {
            AssertRefused(() => ConstructRetained(storage, table, optIns, assumeComplete), option, remedy);
            var off = Construct(storage, options => options.AssumeCompleteStorageListing = assumeComplete, table.Split('/')).CleanupForTests;
            Assert.False(off.OptIns.Any);
            Assert.False(off.Store.Eligible);
        }

        private static void AssertRefused(Func<CleanupConfiguration> validate, string option, string remedy)
        {
            var e = Assert.Throws<ArgumentException>(validate);
            Assert.Equal(option, e.ParamName);
            Assert.Contains(remedy, e.Message);
            Assert.Contains("disable", e.Message, StringComparison.OrdinalIgnoreCase);
        }

        [Theory]
        [MemberData(nameof(ReservedNames))]
        public void S3ReservedPrefixesRefusedAtConstructionWhenOptedIn(string name)
        {
            using var storage = StoreFixtures.S3();
            AssertRefused(() => CleanupConfiguration.Validate(storage, name, false, Retention), CleanupConfiguration.StorageOption, CleanupStoreContract.QualifiedCharacters);
            AssertRefused(() => CleanupConfiguration.Validate(storage, name, true, RetentionAndOrphans), CleanupConfiguration.StorageOption, CleanupStoreContract.QualifiedCharacters);
            Assert.False(CleanupConfiguration.Validate(storage, name, false, CleanupOptIns.Off).Store.Eligible);
            // The qualified neighbor is accepted
            Assert.True(CleanupConfiguration.Validate(storage, "c/a.b-c_d/T9", false, RetentionAndOrphans).Store.Eligible);
        }

        [Theory]
        [MemberData(nameof(ReservedNames))]
        public void AzureReservedPrefixesRefusedAtConstructionWhenOptedIn(string name)
        {
            using var storage = StoreFixtures.Azure();
            AssertRefused(() => CleanupConfiguration.Validate(storage, name, false, Retention), CleanupConfiguration.StorageOption, CleanupStoreContract.QualifiedCharacters);
            AssertRefused(() => CleanupConfiguration.Validate(storage, name, true, RetentionAndOrphans), CleanupConfiguration.StorageOption, CleanupStoreContract.QualifiedCharacters);
            Assert.False(CleanupConfiguration.Validate(storage, name, false, CleanupOptIns.Off).Store.Eligible);
            Assert.True(CleanupConfiguration.Validate(storage, "c/a.b-c_d/T9", false, RetentionAndOrphans).Store.Eligible);
        }

        [Theory]
        [InlineData("c/x/../t")]
        [InlineData("c/t/..")]
        [InlineData("c/./t")]
        [InlineData("c//t")]
        [InlineData("c/t/")]
        [InlineData("/c/t")]
        [InlineData("c\\t")]
        [InlineData("c/t\u0001")]
        [InlineData("")]
        public void OriginalDotDotAndEmptySegmentsCannotNormalizeIntoEligibility(string name)
        {
            var directory = StoreFixtures.TempDirectory(nameof(OriginalDotDotAndEmptySegmentsCannotNormalizeIntoEligibility));
            foreach (var storage in new[] { StoreFixtures.S3(), StoreFixtures.Azure(), Files.Of.LocalDisk(directory) })
            {
                using (storage)
                {
                    AssertRefused(() => CleanupConfiguration.Validate(storage, name, false, Retention), CleanupConfiguration.StorageOption, "without '.' or '..'");
                    var off = CleanupConfiguration.Validate(storage, name, false, CleanupOptIns.Off).Store;
                    Assert.False(off.Eligible);
                    Assert.NotNull(off.PathProblem);
                    Assert.False(off.TryQualifyTarget("part-1.parquet", out _));
                }
            }
            // Path normalization would have made some of these look qualified
            Assert.Equal("/c/t", new IOPath("c/x/../t").Full);
            Assert.Equal("/c/t", new IOPath("c//t").Full);

            // A listing adds one terminal slash, which is not an empty segment
            var contract = CleanupStoreContract.Classify(StoreFixtures.S3(), "c/t", false);
            Assert.True(contract.QualifiesPrefix("t/_delta_log/"));
            Assert.False(contract.QualifiesPrefix("t/_delta_log//"));
            Assert.False(contract.QualifiesPrefix("t//_delta_log/"));
            Assert.False(contract.QualifiesPrefix("t/../_delta_log/"));
            Assert.False(contract.QualifiesPrefix("t/./_delta_log/"));
            Assert.False(contract.QualifiesPrefix("t/_delta_log"));
            Assert.False(contract.QualifiesPrefix("/"));
        }

        [Fact]
        public void GcsRefusedAtConstructionEvenVouchedWhenOptedIn()
        {
            using var storage = StoreFixtures.Gcs();
            AssertRefused(() => CleanupConfiguration.Validate(storage, "bucket/t", false, Retention), CleanupConfiguration.StorageOption, $"{CleanupConfiguration.AssertionOption} cannot repair");
            AssertRefused(() => CleanupConfiguration.Validate(storage, "bucket/t", true, Retention), CleanupConfiguration.StorageOption, $"{CleanupConfiguration.AssertionOption} cannot repair");
            AssertRefused(() => CleanupConfiguration.Validate(storage, "bucket/t", true, RetentionAndOrphans), CleanupConfiguration.StorageOption, "first page");
            Assert.False(CleanupConfiguration.Validate(storage, "bucket/t", true, CleanupOptIns.Off).Store.Eligible);
        }

        [Fact]
        public void KnownFailedProviderCannotBeVouched()
        {
            // Deletes through an endpoint with a path segment miss the object, and a connector-owned wrapper hides nothing
            var known = new IFileStorage[]
            {
                Files.Of.AzureBlobStorageWithLocalEmulator(),
                StoreFixtures.Azure("devstoreaccount1/"),
                new OwnedForwardingStorage(StoreFixtures.Gcs()),
                new OwnedForwardingStorage(StoreFixtures.Azure("account/"))
            };
            foreach (var storage in known)
            {
                using (storage)
                {
                    var contract = CleanupStoreContract.Classify(storage, "c/t", true);
                    Assert.True(contract.Store.KnownFailed);
                    Assert.False(contract.Eligible);
                    AssertRefused(() => CleanupConfiguration.Validate(storage, "c/t", true, Retention), CleanupConfiguration.StorageOption, $"{CleanupConfiguration.AssertionOption} cannot repair");
                }
            }
            // The endpoint's deletes are not exact, so not even an inline target qualifies
            Assert.False(CleanupStoreContract.Classify(StoreFixtures.Azure("devstoreaccount1/"), "c/t", true).TryQualifyTarget("part-1.parquet", out _));
            Assert.True(CleanupStoreContract.Classify(StoreFixtures.Azure(), "c/t", false).TryQualifyTarget("part-1.parquet", out _));
        }

        [Fact]
        public void UnknownWrapperOptInRequiresVouchAtConstruction()
        {
            var directory = StoreFixtures.TempDirectory(nameof(UnknownWrapperOptInRequiresVouchAtConstruction));
            var wrappers = new IFileStorage[]
            {
                new HookFileStorage(Files.Of.InternalMemory(nameof(UnknownWrapperOptInRequiresVouchAtConstruction))),
                // A local disk marker interface on a wrapper does not make it the pinned local disk
                HookFileStorage.Wrap(Files.Of.LocalDisk(directory)),
                new ForwardingStorage(StoreFixtures.S3()),
                Files.Of.InternalMemory(nameof(UnknownWrapperOptInRequiresVouchAtConstruction) + "raw")
            };
            foreach (var storage in wrappers)
            {
                AssertRefused(() => CleanupConfiguration.Validate(storage, "t", false, Retention), CleanupConfiguration.AssertionOption, "Verify that the whole storage stack");
                var vouched = CleanupConfiguration.Validate(storage, "t", true, Retention);
                Assert.True(vouched.Store.Eligible);
                Assert.True(vouched.Store.Asserted);
            }
            // Vouching covers the listing, not a path outside the structural rules
            AssertRefused(() => CleanupConfiguration.Validate(wrappers[0], "t/../u", true, Retention), CleanupConfiguration.StorageOption, "without '.' or '..'");
        }

        [Fact]
        public void InvalidOptInValuesThrowNamingTheOption()
        {
            using var storage = StoreFixtures.S3();
            foreach (var retention in new[] { TimeSpan.FromHours(23), TimeSpan.FromDays(1) + TimeSpan.FromMinutes(30), TimeSpan.FromDays(1) + TimeSpan.FromTicks(1), TimeSpan.Zero, TimeSpan.FromDays(-7), TimeSpan.MaxValue })
            {
                AssertRefused(() => CleanupConfiguration.Validate(storage, "c/t", false, new CleanupOptIns(retention, null)), CleanupConfiguration.RetentionOption, "whole hours");
            }
            AssertRefused(() => CleanupConfiguration.Validate(storage, "c/t", false, new CleanupOptIns(null, TimeSpan.FromDays(7))), CleanupConfiguration.RetentionOption, "required by orphan sweeping");
            AssertRefused(() => CleanupConfiguration.Validate(storage, "c/t", false, new CleanupOptIns(TimeSpan.FromDays(7), TimeSpan.FromHours(23))), CleanupConfiguration.OrphanOption, "at least one day");
            Assert.NotNull(CleanupConfiguration.Validate(storage, "c/t", false, new CleanupOptIns(TimeSpan.FromHours(24), TimeSpan.FromDays(1))));
            Assert.NotNull(CleanupConfiguration.Validate(storage, "c/t", false, new CleanupOptIns(TimeSpan.FromHours(25), null)));
        }

        [Fact]
        public void OrphanOptInRequiresDirectoryExclusion()
        {
            using var storage = StoreFixtures.Azure();
            var qualified = CleanupStoreContract.Classify(storage, "c/t", false);
            Assert.True(qualified.Store.DirectoryExclusion);
            Assert.NotNull(CleanupConfiguration.Validate(qualified, RetentionAndOrphans));
            // A descriptor whose directory exclusion is not qualified permits retention alone
            var unqualified = CleanupStoreContract.Create(storage, qualified.Store with { DirectoryExclusion = false }, "c/t", false);
            AssertRefused(() => CleanupConfiguration.Validate(unqualified, RetentionAndOrphans), CleanupConfiguration.OrphanOption, "Disable orphan sweeping");
            Assert.True(CleanupConfiguration.Validate(unqualified, Retention).Store.Eligible);
            // An asserted unknown store has no qualified directory exclusion
            var unknown = new HookFileStorage(Files.Of.InternalMemory(nameof(OrphanOptInRequiresDirectoryExclusion)));
            AssertRefused(() => CleanupConfiguration.Validate(unknown, "t", true, RetentionAndOrphans), CleanupConfiguration.OrphanOption, "Disable orphan sweeping");
            Assert.NotNull(CleanupConfiguration.Validate(unknown, "t", true, Retention));
        }

        [Fact]
        public void DeletionTargetsMustBeExactQualifiedNames()
        {
            var contract = CleanupStoreContract.Classify(StoreFixtures.S3(), "c/tables/t", false);
            Assert.True(contract.TryQualifyTarget("part-00000-1.parquet", out var target));
            Assert.Equal("/c/tables/t/part-00000-1.parquet", target.Full);
            Assert.True(contract.TryQualifyTarget("_change_data/cdc-1.parquet", out target));
            Assert.Equal("/c/tables/t/_change_data/cdc-1.parquet", target.Full);
            foreach (var foreign in new[] { "a=1/part.parquet", "part 1.parquet", "../u/part.parquet", "x/./part.parquet", "x//part.parquet", "x/", "", "part%201.parquet", "pärt.parquet" })
            {
                Assert.False(contract.TryQualifyTarget(foreign, out _), foreign);
            }
            // Local disk keeps its own names but never a dot segment or a backslash
            var local = CleanupStoreContract.Classify(Files.Of.LocalDisk(StoreFixtures.TempDirectory(nameof(DeletionTargetsMustBeExactQualifiedNames))), "t", false);
            Assert.True(local.TryQualifyTarget("a=1/part 1.parquet", out target));
            Assert.Equal("/t/a=1/part 1.parquet", target.Full);
            Assert.False(local.TryQualifyTarget("../u/part.parquet", out _));
            Assert.False(local.TryQualifyTarget("x\\part.parquet", out _));
        }

        [Fact]
        public void GcsTargetsNeedTheWholeKeyQualified()
        {
            using var storage = StoreFixtures.Gcs();
            var contract = CleanupStoreContract.Classify(storage, "sales/t", false);
            // The sink's own fresh outputs qualify below a qualified table prefix
            Assert.True(contract.TryQualifyTarget("part-00000-0f8fad5b-d9cb-469f-a165-70867728950e.parquet", out var target));
            Assert.Equal("/sales/t/part-00000-0f8fad5b-d9cb-469f-a165-70867728950e.parquet", target.Full);
            Assert.True(contract.TryQualifyTarget("_change_data/cdc-00000-0f8fad5b-d9cb-469f-a165-70867728950e.parquet", out target));
            Assert.Equal("/sales/t/_change_data/cdc-00000-0f8fad5b-d9cb-469f-a165-70867728950e.parquet", target.Full);
            foreach (var foreign in new[] { "part#x.parquet", "part?x.parquet", "part 1.parquet", "part%201.parquet", "pärt.parquet", "a=1/part.parquet" })
            {
                Assert.False(contract.TryQualifyTarget(foreign, out _), foreign);
            }
            // An unsafe table prefix qualifies no target, and the assertion cannot change that
            foreach (var table in new[] { "sales#2024/t", "sales 2024/t", "sales/té", "sales%2Ft", "sales?x/t" })
            {
                foreach (var asserted in new[] { false, true })
                {
                    var unsafePrefix = CleanupStoreContract.Classify(storage, table, asserted);
                    Assert.NotNull(unsafePrefix.PathProblem);
                    Assert.False(unsafePrefix.TryQualifyTarget("part-00000-1.parquet", out _), table);
                }
            }
        }

        [Fact]
        public void OnlyPinnedGcsDeletesExactly()
        {
            using var storage = StoreFixtures.Gcs();
            var pinned = StoreDescriptor.Of(storage);
            Assert.True(pinned.Pinned);
            Assert.Equal(StoreDescriptor.Gcs(pinned: true), pinned);
            Assert.True(pinned.ExactDeletes);
            Assert.True(CleanupStoreContract.Create(storage, pinned, "sales/t", false).TryQualifyTarget("part-00000-1.parquet", out _));
            // Another build's client was never qualified, so not even the sink's own output is a target
            var other = StoreDescriptor.Gcs(pinned: false);
            Assert.False(other.ExactDeletes);
            foreach (var asserted in new[] { false, true })
            {
                Assert.False(CleanupStoreContract.Create(storage, other, "sales/t", asserted).TryQualifyTarget("part-00000-1.parquet", out _));
            }
        }

        [Fact]
        public void UnassertedUnknownStorageQualifiesNoTarget()
        {
            var directory = StoreFixtures.TempDirectory(nameof(UnassertedUnknownStorageQualifiesNoTarget));
            var unknown = new IFileStorage[]
            {
                // A user decorator over the pinned S3 client, which sends names unencoded
                new ForwardingStorage(StoreFixtures.S3()),
                HookFileStorage.Wrap(Files.Of.LocalDisk(directory)),
                new HookFileStorage(Files.Of.InternalMemory(nameof(UnassertedUnknownStorageQualifiesNoTarget))),
                // Another implementation of the pinned build
                (IFileStorage)Files.Of.VFS()
            };
            foreach (var storage in unknown)
            {
                Assert.Equal(StoreKind.Unknown, CleanupStoreContract.Classify(storage, "c/t", false).Store.Kind);
                Assert.False(CleanupStoreContract.Classify(storage, "c/t", false).TryQualifyTarget("part-00000-1.parquet", out _), storage.GetType().Name);
                // The assertion vouches for the whole stack, its absence answers and deletes included
                Assert.True(CleanupStoreContract.Classify(storage, "c/t", true).TryQualifyTarget("part-00000-1.parquet", out var target));
                Assert.Equal("/c/t/part-00000-1.parquet", target.Full);
                AssertRefused(() => CleanupConfiguration.Validate(storage, "c/t", false, Retention), CleanupConfiguration.AssertionOption, "absent only when it does not exist and deletes exactly the named object");
            }
            // The pinned in-memory store is known, so its targets need no assertion
            Assert.True(CleanupStoreContract.Classify(Files.Of.InternalMemory(nameof(UnassertedUnknownStorageQualifiesNoTarget) + "raw"), "t", false).TryQualifyTarget("part-00000-1.parquet", out _));
        }

        [Fact]
        public void OptInsOffNeverThrowNewStoreOrDomainConfigurationExceptions()
        {
            var directory = StoreFixtures.TempDirectory(nameof(OptInsOffNeverThrowNewStoreOrDomainConfigurationExceptions));
            var stores = new IFileStorage?[]
            {
                StoreFixtures.S3(), StoreFixtures.Azure(), StoreFixtures.Azure("devstoreaccount1/"), StoreFixtures.Gcs(), Files.Of.LocalDisk(directory),
                Files.Of.InternalMemory(nameof(OptInsOffNeverThrowNewStoreOrDomainConfigurationExceptions)), new ForwardingStorage(StoreFixtures.S3()),
                new BrokenOwnedStorage(StoreFixtures.S3()), null
            };
            foreach (var storage in stores)
            {
                foreach (var name in new[] { "c/t", "c", "c/t x", "c/../t", "c//t", "", "c/té" })
                {
                    foreach (var asserted in new[] { false, true })
                    {
                        var configuration = CleanupConfiguration.Validate(storage, name, asserted, CleanupOptIns.Off);
                        Assert.False(configuration.OptIns.Any);
                    }
                }
                // The sink constructs over every store and name
                Assert.False(Construct(storage!, "c", "t x").CleanupForTests.OptIns.Any);
                Assert.False(Construct(storage!, options => options.HistoryRetention = null, "c", "..", "t").CleanupForTests.OptIns.Any);
            }
            // A storage that cannot be unwrapped is classified as unknown and failed
            Assert.True(CleanupStoreContract.Classify(new BrokenOwnedStorage(StoreFixtures.S3()), "c/t", true).Store.KnownFailed);
            // The sink validates the joined original names, before normalization folds them into c/t
            Assert.NotNull(Construct(StoreFixtures.S3(), "c", "x", "..", "t").CleanupForTests.Store.PathProblem);
            Assert.Null(Construct(StoreFixtures.S3(), "c", "t").CleanupForTests.Store.PathProblem);
        }

        [Fact]
        public void ConstructionValidationIssuesNoStorageRequests()
        {
            var storage = new HookFileStorage(Files.Of.InternalMemory(nameof(ConstructionValidationIssuesNoStorageRequests)));
            var sink = Construct(storage, "t");
            Assert.NotNull(sink.CleanupForTests);
            CleanupConfiguration.Validate(storage, "t", true, Retention);
            Assert.Throws<ArgumentException>(() => CleanupConfiguration.Validate(storage, "t", false, Retention));
            Assert.Throws<ArgumentException>(() => CleanupConfiguration.Validate(storage, "t", true, RetentionAndOrphans));
            Assert.Empty(storage.Requests);
        }

        [Theory]
        [InlineData(StoreFlavor.Azure)]
        [InlineData(StoreFlavor.S3)]
        public async Task ConstructionValidationIssuesNoRequestsToPinnedClients(StoreFlavor flavor)
        {
            await using var server = new LoopbackObjectStore(flavor);
            using var storage = server.CreateStorage();
            Construct(storage, LoopbackObjectStore.Container, "tables", "t");
            CleanupConfiguration.Validate(storage, "c/tables/t", false, RetentionAndOrphans);
            Assert.Throws<ArgumentException>(() => CleanupConfiguration.Validate(storage, "c/t x", false, Retention));
            // A request sent now would be the first the server sees
            Assert.Null(await storage.Stat("/c/tables/t/x"));
            Assert.Single(server.Requests);
        }

        [Theory]
        [MemberData(nameof(ReservedNames))]
        public void SinkWithRetentionRefusesS3AndAzureReservedPrefixes(string name)
        {
            foreach (var storage in new[] { StoreFixtures.S3(), StoreFixtures.Azure() })
            {
                using (storage)
                {
                    foreach (var optIns in OptInVariants)
                    {
                        AssertSinkRefused(storage, name, optIns, false, CleanupConfiguration.StorageOption, CleanupStoreContract.QualifiedCharacters);
                        AssertSinkRefused(storage, name, optIns, true, CleanupConfiguration.StorageOption, CleanupStoreContract.QualifiedCharacters);
                        var accepted = ConstructRetained(storage, "c/a.b-c_d/T9", optIns);
                        Assert.True(accepted.Store.Eligible);
                        Assert.Equal(optIns, accepted.OptIns);
                    }
                }
            }
        }

        [Theory]
        [InlineData("c/x/../t")]
        [InlineData("c/t/..")]
        [InlineData("c/./t")]
        [InlineData("c//t")]
        [InlineData("c/t/")]
        [InlineData("/c/t")]
        [InlineData("c\\t")]
        [InlineData("c/t\u0001")]
        [InlineData("")]
        public void SinkWithRetentionRefusesDotDotAndEmptySegments(string name)
        {
            var directory = StoreFixtures.TempDirectory(nameof(SinkWithRetentionRefusesDotDotAndEmptySegments));
            foreach (var storage in new[] { StoreFixtures.S3(), StoreFixtures.Azure(), Files.Of.LocalDisk(directory) })
            {
                using (storage)
                {
                    foreach (var optIns in OptInVariants)
                    {
                        AssertSinkRefused(storage, name, optIns, false, CleanupConfiguration.StorageOption, "without '.' or '..'");
                    }
                }
            }
        }

        [Fact]
        public void SinkWithRetentionRefusesGcsAndKnownFailedStores()
        {
            using (var gcs = StoreFixtures.Gcs())
            {
                foreach (var optIns in OptInVariants)
                {
                    AssertSinkRefused(gcs, "bucket/t", optIns, false, CleanupConfiguration.StorageOption, $"{CleanupConfiguration.AssertionOption} cannot repair");
                    AssertSinkRefused(gcs, "bucket/t", optIns, true, CleanupConfiguration.StorageOption, $"{CleanupConfiguration.AssertionOption} cannot repair");
                }
            }
            var known = new IFileStorage[]
            {
                Files.Of.AzureBlobStorageWithLocalEmulator(),
                StoreFixtures.Azure("devstoreaccount1/"),
                new OwnedForwardingStorage(StoreFixtures.Gcs()),
                new OwnedForwardingStorage(StoreFixtures.Azure("account/"))
            };
            foreach (var storage in known)
            {
                using (storage)
                {
                    foreach (var optIns in OptInVariants)
                    {
                        AssertSinkRefused(storage, "c/t", optIns, true, CleanupConfiguration.StorageOption, $"{CleanupConfiguration.AssertionOption} cannot repair");
                    }
                }
            }
        }

        [Fact]
        public void SinkWithRetentionRequiresVouchForUnknownWrapper()
        {
            var directory = StoreFixtures.TempDirectory(nameof(SinkWithRetentionRequiresVouchForUnknownWrapper));
            var wrappers = new IFileStorage[]
            {
                new HookFileStorage(Files.Of.InternalMemory(nameof(SinkWithRetentionRequiresVouchForUnknownWrapper))),
                HookFileStorage.Wrap(Files.Of.LocalDisk(directory)),
                new ForwardingStorage(StoreFixtures.S3()),
                Files.Of.InternalMemory(nameof(SinkWithRetentionRequiresVouchForUnknownWrapper) + "raw")
            };
            // Only the pinned in-memory store tells directories apart from files
            var orphansAccepted = new[] { false, false, false, true };
            for (int i = 0; i < wrappers.Length; i++)
            {
                var storage = wrappers[i];
                // The missing assertion is reported before the orphan opt-in
                foreach (var optIns in OptInVariants)
                {
                    AssertSinkRefused(storage, "t", optIns, false, CleanupConfiguration.AssertionOption, "Verify that the whole storage stack");
                }
                var vouched = ConstructRetained(storage, "t", Retention, true);
                Assert.True(vouched.Store.Eligible);
                Assert.True(vouched.Store.Asserted);
                Assert.Equal(Retention.Retention, vouched.OptIns.Retention);
                if (orphansAccepted[i])
                {
                    var orphans = ConstructRetained(storage, "t", RetentionAndOrphans, true);
                    Assert.True(orphans.Store.Eligible);
                    Assert.Equal(RetentionAndOrphans, orphans.OptIns);
                }
                else
                {
                    AssertRefused(() => ConstructRetained(storage, "t", RetentionAndOrphans, true), CleanupConfiguration.OrphanOption, "Disable orphan sweeping");
                }
            }
            foreach (var optIns in OptInVariants)
            {
                AssertRefused(() => ConstructRetained(wrappers[0], "t/../u", optIns, true), CleanupConfiguration.StorageOption, "without '.' or '..'");
            }
        }

        [Fact]
        public void SinkWithRetentionIssuesNoStorageRequests()
        {
            var storage = new HookFileStorage(Files.Of.InternalMemory(nameof(SinkWithRetentionIssuesNoStorageRequests)));
            Assert.True(ConstructRetained(storage, "t", Retention, true).Store.Eligible);
            AssertRefused(() => ConstructRetained(storage, "t", RetentionAndOrphans, true), CleanupConfiguration.OrphanOption, "Disable orphan sweeping");
            foreach (var optIns in OptInVariants)
            {
                Assert.Throws<ArgumentException>(() => ConstructRetained(storage, "t", optIns));
            }
            Assert.Empty(storage.Requests);
            // Local disk needs no assertion
            using var local = Files.Of.LocalDisk(StoreFixtures.TempDirectory(nameof(SinkWithRetentionIssuesNoStorageRequests)));
            foreach (var optIns in OptInVariants)
            {
                Assert.True(ConstructRetained(local, "t", optIns).Store.Eligible);
            }
        }

        [Theory]
        [InlineData(StoreFlavor.Azure)]
        [InlineData(StoreFlavor.S3)]
        public async Task SinkWithRetentionIssuesNoRequestsToPinnedClients(StoreFlavor flavor)
        {
            await using var server = new LoopbackObjectStore(flavor);
            using var storage = server.CreateStorage();
            foreach (var optIns in OptInVariants)
            {
                Assert.True(ConstructRetained(storage, $"{LoopbackObjectStore.Container}/tables/t", optIns).Store.Eligible);
                AssertRefused(() => ConstructRetained(storage, $"{LoopbackObjectStore.Container}/t x", optIns), CleanupConfiguration.StorageOption, CleanupStoreContract.QualifiedCharacters);
            }
            // A request sent now would be the first the server sees
            Assert.Null(await storage.Stat("/c/tables/t/x"));
            Assert.Single(server.Requests);
        }

        [Fact]
        public void OrphanIntervalValidationAtConstruction()
        {
            var directory = StoreFixtures.TempDirectory(nameof(OrphanIntervalValidationAtConstruction));
            using var storage = Files.Of.LocalDisk(directory);
            foreach (var interval in new[] { TimeSpan.FromHours(23), TimeSpan.FromDays(1) - TimeSpan.FromTicks(1), TimeSpan.Zero, TimeSpan.FromDays(-7), TimeSpan.MinValue })
            {
                AssertConstructionRefused(storage, Retention.Retention, interval, CleanupConfiguration.OrphanOption, "use at least one day");
            }
            AssertConstructionRefused(storage, null, TimeSpan.FromDays(7), CleanupConfiguration.RetentionOption, "required by orphan sweeping");

            // With the largest whole-hours retention
            var largest = TimeSpan.FromTicks(TimeSpan.MaxValue.Ticks / TimeSpan.TicksPerHour * TimeSpan.TicksPerHour);
            foreach (var interval in new[] { TimeSpan.FromDays(1), TimeSpan.FromDays(7), TimeSpan.MaxValue })
            {
                var options = new DeltaLakeOptions() { StorageLocation = storage, HistoryRetention = largest, OrphanFileSweepInterval = interval };
                foreach (var sink in new[] { new DeltaLakeSink(options, Relation("t"), new ExecutionDataflowBlockOptions()), Assert.IsType<DeltaLakeSink>(new DeltaLakeSinkFactory(options).CreateSink(Relation("t"), null!, new ExecutionDataflowBlockOptions())) })
                {
                    Assert.Equal(interval, sink.CleanupForTests.OptIns.OrphanSweepInterval);
                    Assert.Equal(largest, sink.CleanupForTests.OptIns.Retention);
                    Assert.True(sink.CleanupForTests.Store.Eligible);
                }
            }
            Assert.False(Construct(storage, "t").CleanupForTests.OptIns.Any);
        }

        // The constructor and the factory every stream uses both refuse
        private static void AssertConstructionRefused(IFileStorage storage, TimeSpan? retention, TimeSpan interval, string option, string remedy)
        {
            var options = new DeltaLakeOptions() { StorageLocation = storage, HistoryRetention = retention, OrphanFileSweepInterval = interval };
            AssertRefused(() => new DeltaLakeSink(options, Relation("t"), new ExecutionDataflowBlockOptions()).CleanupForTests, option, remedy);
            AssertRefused(() => ((DeltaLakeSink)new DeltaLakeSinkFactory(options).CreateSink(Relation("t"), null!, new ExecutionDataflowBlockOptions())).CleanupForTests, option, remedy);
        }

        [Fact]
        public void AzureOrphanConstructionRequiresQualifiedDirectoryExclusion()
        {
            // Azure underneath, behind a wrapper classification never looks through
            using var storage = new ForwardingStorage(StoreFixtures.Azure());
            AssertRefused(() => ConstructRetained(storage, "c/t", RetentionAndOrphans, true), CleanupConfiguration.OrphanOption, "Disable orphan sweeping");
            var retained = ConstructRetained(storage, "c/t", Retention, true);
            Assert.True(retained.Store.Eligible);
            Assert.False(retained.Store.Store.DirectoryExclusion);
            // The pinned client itself qualifies for both
            using var pinned = StoreFixtures.Azure();
            Assert.True(ConstructRetained(pinned, "c/t", RetentionAndOrphans).Store.Eligible);
        }

        [Fact]
        public async Task OptInsOffIneligibleStoreUsesInlineWithoutAbsenceCleanup()
        {
            var testName = nameof(OptInsOffIneligibleStoreUsesInlineWithoutAbsenceCleanup);
            // In-memory storage without the assertion: no listing may stand as deletion evidence, but its deletes name exactly one object
            var known = new OwnedHookFileStorage(Files.Of.InternalMemory($"./{testName}"));
            var knownSink = await InlineRun(testName, known);
            Assert.Equal(StoreKind.InMemory, knownSink.CleanupForTests.Store.Store.Kind);
            // Discarded change data is still deleted inline
            Assert.Contains(known.Requests, x => x.StartsWith("Rm ") && x.Contains("/_change_data/"));
            Assert.Empty((await known.Inner.Ls("/test/_change_data/")).Where(x => !x.Path.IsFolder));
            Assert.True(knownSink.DiscardsForTests.Deleted > 0);
            Assert.Equal(0, knownSink.DiscardsForTests.Kept);

            // An unknown wrapper without the assertion: its deletes are not known to be exact either, so its discards are kept and counted
            var unknown = new HookFileStorage(Files.Of.InternalMemory($"./{testName}_unknown"));
            var unknownSink = await InlineRun(testName + "_unknown", unknown);
            Assert.Equal(StoreKind.Unknown, unknownSink.CleanupForTests.Store.Store.Kind);
            Assert.DoesNotContain(unknown.Requests, x => x.StartsWith("Rm ") && x.Contains("/_change_data/"));
            var left = (await unknown.Inner.Ls("/test/_change_data/")).Where(x => !x.Path.IsFolder).ToList();
            Assert.NotEmpty(left);
            Assert.Equal(left.Count, unknownSink.DiscardsForTests.Kept);
            Assert.Equal(0, unknownSink.DiscardsForTests.Deleted);
        }

        // One run that discards change data on an ineligible store: inline mode, no recursive inventory, no absence evidence, and deletes only of inline discards or stage files
        private static async Task<DeltaLakeSink> InlineRun(string testName, HookFileStorage storage)
        {
            DeltaLakeSink? sink = null;
            await using var stream = new DeltaLakeSinkStream(testName, storage, options =>
            {
                options.WriteChangeDataOnNewTables = true;
                // Insert-only versions still write change files to discard
                options.DeleteLimitsOverride = new DeleteLimits() { InsertChangeData = true };
                options.MaxFileSizeBytes = 500;
                options.SinkCreatedForTests = created => sink = created;
            });
            stream.Generate(200);
            await stream.StartStream(UserInsert);
            await WaitForVersion(storage.Inner, "test", stream, 0);
            await stream.HealthyFor(TimeSpan.FromMilliseconds(300));
            await stream.Maintenance.TickAndSettleAsync(0);

            var cleanup = sink!.CleanupForTests;
            Assert.False(cleanup.OptIns.Any);
            Assert.False(cleanup.Store.Eligible);
            Assert.Equal(DiscardMode.Inline, sink.DiscardModeForTests);
            Assert.Null(sink.DiscardRouteForTests);
            Assert.Equal(0, stream.Maintenance.Budget.Discards.Queued);
            // The staged sweep's stage-only listing probes its successor; nothing else is removed on absence
            Assert.All(storage.Requests.Where(x => x.StartsWith("Rm ")), x => Assert.True(x.Contains("/_change_data/") || (x.Contains("/_delta_log/.") && x.EndsWith(".tmp") && x.Contains(".json.")), x));
            Assert.DoesNotContain(storage.Requests, x => x == "Ls /test/");
            Assert.Equal(ListingCheck.StoreIneligible, (await ListingEvidence.ReadLogAsync(cleanup, storage, -1, default)).Check);
            Assert.Equal(0, stream.FailureNotificationCount);
            return sink;
        }

        [Fact]
        public async Task RestoreCommitAndWorkerDoNotThrowConfigurationExceptions()
        {
            var testName = nameof(RestoreCommitAndWorkerDoNotThrowConfigurationExceptions);
            var storage = new HookFileStorage(Files.Of.InternalMemory($"./{testName}"));
            var sinks = new ConcurrentQueue<DeltaLakeSink>();
            DeltaLakeOptions? options = null;
            await using var stream = new DeltaLakeSinkStream(testName, storage, configure =>
            {
                options = configure;
                configure.CheckpointInterval = 1;
                configure.SinkCreatedForTests = sinks.Enqueue;
            });
            stream.Generate(10);
            await stream.StartStream(UserInsert);
            await WaitForVersion(storage.Inner, "test", stream, 0);
            var sink = Assert.Single(sinks);
            var cleanup = sink.CleanupForTests;

            // Configuration changed after construction is not validated again
            options!.AssumeCompleteStorageListing = true;
            options.StorageLocation = new HookFileStorage(storage.Inner);
            await stream.Crash();
            stream.Generate(1);
            await WaitForVersion(storage.Inner, "test", stream, 1);
            await WaitForCheckpoint(storage.Inner, "test", stream, 1);
            await stream.HealthyFor(TimeSpan.FromMilliseconds(300));

            // The reused vertex keeps its construction snapshot and sees the replaced instance
            Assert.Same(sink, Assert.Single(sinks));
            Assert.Same(cleanup, sink.CleanupForTests);
            Assert.False(cleanup.Store.Asserted);
            Assert.True(cleanup.StorageChanged(options.StorageLocation));
        }

        [Fact]
        public async Task ChangedStorageInstanceDeclinesWithoutConfigurationException()
        {
            var original = new ScriptedStorage();
            original.Put("/t/_delta_log/00000000000000000000.json");
            var configuration = CleanupConfiguration.Validate(new OwnedForwardingStorage(original), "t", true, Retention);

            // Connector-owned wrappers are looked through; anything else is another instance
            Assert.False(configuration.StorageChanged(original));
            Assert.False(configuration.StorageChanged(new OwnedForwardingStorage(new OwnedForwardingStorage(original))));
            Assert.True(configuration.StorageChanged(new ForwardingStorage(original)));
            Assert.True(configuration.StorageChanged(new ScriptedStorage()));
            Assert.True(configuration.StorageChanged(new BrokenOwnedStorage(original)));
            Assert.True(configuration.StorageChanged(null));

            // A replaced instance declines before any request and the original classification authorizes nothing through it
            var replacement = new ScriptedStorage();
            replacement.Put("/t/_delta_log/00000000000000000000.json");
            Assert.Equal(ListingCheck.StorageChanged, (await ListingEvidence.ReadLogAsync(configuration, replacement, -1, default)).Check);
            Assert.Empty(replacement.Requests);
            Assert.Equal(ListingCheck.Passed, (await ListingEvidence.ReadLogAsync(configuration, original, 0, default)).Check);

            // A sink whose options are given another storage keeps its snapshot
            var options = new DeltaLakeOptions() { StorageLocation = StoreFixtures.S3(), AssumeCompleteStorageListing = true };
            var sink = new DeltaLakeSink(options, Relation("c", "t"), new ExecutionDataflowBlockOptions());
            Assert.False(sink.CleanupForTests.StorageChanged(options.StorageLocation));
            Assert.False(sink.CleanupForTests.StorageReplaced());
            options.StorageLocation = StoreFixtures.Gcs();
            options.AssumeCompleteStorageListing = false;
            Assert.True(sink.CleanupForTests.StorageChanged(options.StorageLocation));
            // The sink's configuration reads its storage again, so no caller has to pass it
            Assert.True(sink.CleanupForTests.StorageReplaced());
            Assert.False(configuration.StorageReplaced());
            Assert.Equal(StoreKind.S3, sink.CleanupForTests.Store.Store.Kind);
            Assert.True(sink.CleanupForTests.Store.Asserted);
        }
    }
}
