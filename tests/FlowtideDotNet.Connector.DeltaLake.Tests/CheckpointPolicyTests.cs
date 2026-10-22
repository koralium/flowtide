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
using FlowtideDotNet.Connector.DeltaLake.Internal.Delta.Checkpoints;
using FlowtideDotNet.Connector.DeltaLake.Internal.Delta.DeletionVectors;
using FlowtideDotNet.Connector.DeltaLake.Internal.Maintenance;

namespace FlowtideDotNet.Connector.DeltaLake.Tests
{
    public class CheckpointPolicyTests
    {
        private static readonly DateTimeOffset Now = DateTimeOffset.FromUnixTimeMilliseconds(1_760_000_000_000);

        private static Dictionary<string, string> Retention(string value) => new Dictionary<string, string>() { [CheckpointExpiry.RetentionProperty] = value };

        [Fact]
        public void TombstoneAtTheCutoffIsKept()
        {
            var cutoff = CheckpointExpiry.CutoffTicks(Now, TimeSpan.FromDays(1));
            var atCutoff = Now.ToUnixTimeMilliseconds() - (long)TimeSpan.FromDays(1).TotalMilliseconds;
            Assert.False(CheckpointExpiry.Expired(atCutoff, cutoff));
            Assert.True(CheckpointExpiry.Expired(atCutoff - 1, cutoff));
            Assert.Equal(atCutoff, CheckpointExpiry.CutoffMs(cutoff));
        }

        [Fact]
        public void MissingTimestampExpires()
        {
            Assert.True(CheckpointExpiry.Expired(null, CheckpointExpiry.CutoffTicks(Now, TimeSpan.FromDays(7))));
        }

        [Fact]
        public void FutureTombstoneIsKept()
        {
            Assert.False(CheckpointExpiry.Expired(Now.ToUnixTimeMilliseconds() + 60_000, CheckpointExpiry.CutoffTicks(Now, TimeSpan.FromDays(7))));
        }

        [Fact]
        public void AbsentRetentionMeansSevenDays()
        {
            Assert.Equal(TimeSpan.FromDays(7), CheckpointExpiry.Retention(null));
            Assert.Equal(TimeSpan.FromDays(7), CheckpointExpiry.Retention(new Dictionary<string, string>()));
            Assert.Equal(TimeSpan.FromHours(30), CheckpointExpiry.Retention(Retention("interval 30 hours")));
        }

        [Fact]
        public void InvalidRetentionDeclines()
        {
            var declined = Assert.Throws<CheckpointDeclinedException>(() => CheckpointExpiry.Retention(Retention("seven days")));
            Assert.Equal(MaintenanceReason.InvalidRetention, declined.Reason);
            Assert.Equal(DeclineDependency.Protocol, declined.Dependency);
        }

        [Fact]
        public void HugeRetentionKeepsEveryTombstone()
        {
            var cutoff = CheckpointExpiry.CutoffTicks(Now, TimeSpan.MaxValue);
            foreach (var ts in new long[] { -62_135_596_800_000, -1, 0, 1, Now.ToUnixTimeMilliseconds(), long.MaxValue })
            {
                Assert.False(CheckpointExpiry.Expired(ts, cutoff));
            }
            Assert.True(CheckpointExpiry.CutoffMs(cutoff) < -62_135_596_800_000);
            Assert.Equal(long.MinValue, CheckpointExpiry.CutoffMs((Int128)long.MinValue * 100_000));
        }

        [Fact]
        public void FractionalCutoffMatchesExactArithmetic()
        {
            // R = 1.5 ms puts the cutoff between whole milliseconds, on both sides of the epoch
            var retention = CheckpointExpiry.Retention(Retention("interval 1500 microseconds"));
            Assert.Equal(15_000, retention.Ticks);
            foreach (var nowMs in new long[] { 1_000, 1, 0, -1, -1_000 })
            {
                var now = DateTimeOffset.UnixEpoch.AddMilliseconds(nowMs);
                var cutoff = CheckpointExpiry.CutoffTicks(now, retention);
                var cutoffMs = CheckpointExpiry.CutoffMs(cutoff);
                for (var ts = nowMs - 5; ts <= nowMs + 1; ts++)
                {
                    var exact = (Int128)ts * 10_000 < (Int128)nowMs * 10_000 - 15_000;
                    Assert.Equal(exact, CheckpointExpiry.Expired(ts, cutoff));
                    Assert.Equal(exact, ts < cutoffMs);
                }
                // ceil(now - 1.5): one millisecond below now
                Assert.Equal(nowMs - 1, cutoffMs);
            }
        }

        private static DeltaProtocolAction Protocol(int reader, int writer, string[]? readerFeatures = null, string[]? writerFeatures = null)
        {
            return new DeltaProtocolAction() { MinReaderVersion = reader, MinWriterVersion = writer, ReaderFeatures = readerFeatures, WriterFeatures = writerFeatures };
        }

        private static DeltaMetadataAction MetaData(Dictionary<string, string>? configuration = null) => new DeltaMetadataAction() { Id = "t", Configuration = configuration };

        [Fact]
        public void CapabilityAllowlistMatchesProtocolReview()
        {
            // Every PROTOCOL.md feature and the RFC features has a decision and a reason
            var protocolFeatures = new[]
            {
                "appendOnly", "invariants", "checkConstraints", "generatedColumns", "allowColumnDefaults", "changeDataFeed", "identityColumns", "rowTracking",
                "domainMetadata", "icebergCompatV1", "icebergCompatV2", "clustering", "inCommitTimestamp", "materializePartitionColumns",
                "columnMapping", "deletionVectors", "timestampNtz", "v2Checkpoint", "catalogManaged", "vacuumProtocolCheck",
                "typeWidening", "typeWidening-preview", "variantType", "variantType-preview", "checkpointProtection"
            };
            foreach (var feature in protocolFeatures)
            {
                Assert.True(CheckpointCapability.Allowed.ContainsKey(feature) ^ CheckpointCapability.Excluded.ContainsKey(feature), feature);
            }
            // Exact lists, so a feature added to either side is a reviewed change
            Assert.Equal(new[]
            {
                "allowColumnDefaults", "appendOnly", "changeDataFeed", "checkConstraints", "clustering", "columnMapping", "deletionVectors", "domainMetadata",
                "generatedColumns", "icebergCompatV1", "icebergCompatV2", "identityColumns", "inCommitTimestamp", "invariants", "materializePartitionColumns",
                "rowTracking", "timestampNtz", "typeWidening", "typeWidening-preview", "v2Checkpoint", "vacuumProtocolCheck", "variantType", "variantType-preview"
            }, CheckpointCapability.Allowed.Keys.Order(StringComparer.Ordinal));
            Assert.Equal(new[] { "catalogManaged", "catalogOwned-preview", "checkpointProtection", "coordinatedCommits-preview" }, CheckpointCapability.Excluded.Keys.Order(StringComparer.Ordinal));
            Assert.All(CheckpointCapability.Allowed.Values.Concat(CheckpointCapability.Excluded.Values), x => Assert.False(string.IsNullOrWhiteSpace(x)));
            Assert.Empty(CheckpointCapability.Allowed.Keys.Intersect(CheckpointCapability.Excluded.Keys));

            CheckpointCapability.Check(Protocol(3, 7, new[] { "deletionVectors" }, new[] { "deletionVectors", "rowTracking", "domainMetadata" }), MetaData());
            var excluded = Assert.Throws<CheckpointDeclinedException>(() => CheckpointCapability.Check(Protocol(3, 7, new[] { "catalogManaged" }, new[] { "catalogManaged" }), MetaData()));
            Assert.Equal(MaintenanceReason.ExcludedFeature, excluded.Reason);
            var unknown = Assert.Throws<CheckpointDeclinedException>(() => CheckpointCapability.Check(Protocol(1, 7, null, new[] { "someFutureFeature" }), MetaData()));
            Assert.Equal(MaintenanceReason.UnsupportedFeature, unknown.Reason);
        }

        [Fact]
        public void CheckpointProtectionDeclines()
        {
            var declined = Assert.Throws<CheckpointDeclinedException>(() => CheckpointCapability.Check(Protocol(1, 7, null, new[] { "checkpointProtection" }), MetaData()));
            Assert.Equal(MaintenanceReason.ExcludedFeature, declined.Reason);
        }

        [Fact]
        public void CatalogManagedDeclines()
        {
            var declined = Assert.Throws<CheckpointDeclinedException>(() => CheckpointCapability.Check(Protocol(3, 7, new[] { "catalogManaged" }, new[] { "catalogManaged", "inCommitTimestamp" }), MetaData()));
            Assert.Equal(MaintenanceReason.ExcludedFeature, declined.Reason);
        }

        [Fact]
        public void LegacyWriterVersionsImplyTheirFeatures()
        {
            // Exact sets, so an extra implied feature fails as well as a missing one
            Assert.Empty(CheckpointCapability.WriterFeatures(Protocol(1, 1)));
            Assert.Equal(new[] { "appendOnly", "invariants" }, CheckpointCapability.WriterFeatures(Protocol(1, 2)).Order());
            Assert.Equal(new[] { "appendOnly", "checkConstraints", "invariants" }, CheckpointCapability.WriterFeatures(Protocol(1, 3)).Order());
            Assert.Equal(new[] { "appendOnly", "changeDataFeed", "checkConstraints", "generatedColumns", "invariants" }, CheckpointCapability.WriterFeatures(Protocol(1, 4)).Order());
            Assert.Equal(new[] { "appendOnly", "changeDataFeed", "checkConstraints", "columnMapping", "generatedColumns", "invariants" }, CheckpointCapability.WriterFeatures(Protocol(2, 5)).Order());
            Assert.Equal(new[] { "appendOnly", "changeDataFeed", "checkConstraints", "columnMapping", "generatedColumns", "identityColumns", "invariants" }, CheckpointCapability.WriterFeatures(Protocol(2, 6)).Order());
            // A legacy version ignores a features array
            Assert.DoesNotContain("catalogManaged", CheckpointCapability.WriterFeatures(Protocol(1, 2, null, new[] { "catalogManaged" })));
        }

        [Theory]
        [InlineData(1, 8, false, false)]
        [InlineData(4, 7, true, true)]
        [InlineData(1, 7, false, false)]
        [InlineData(3, 6, false, false)]
        [InlineData(3, 7, false, true)]
        [InlineData(0, 2, false, false)]
        [InlineData(1, 0, false, false)]
        public void UnknownProtocolVersionsDecline(int reader, int writer, bool readerFeatures, bool writerFeatures)
        {
            var protocol = Protocol(reader, writer, readerFeatures ? Array.Empty<string>() : null, writerFeatures ? Array.Empty<string>() : null);
            var declined = Assert.Throws<CheckpointDeclinedException>(() => CheckpointCapability.WriterFeatures(protocol));
            Assert.Equal(MaintenanceReason.UnknownProtocolVersion, declined.Reason);
        }

        [Fact]
        public void ReaderFeaturesMustBeWriterFeatures()
        {
            var declined = Assert.Throws<CheckpointDeclinedException>(() => CheckpointCapability.WriterFeatures(Protocol(3, 7, new[] { "deletionVectors" }, new[] { "domainMetadata" })));
            Assert.Equal(MaintenanceReason.UnknownProtocolVersion, declined.Reason);
        }

        [Fact]
        public void LayoutWithoutJsonStatsDeclines()
        {
            var protocol = Protocol(1, 2);
            CheckpointCapability.Check(protocol, MetaData(new Dictionary<string, string>() { ["delta.checkpoint.writeStatsAsJson"] = "true", ["delta.checkpoint.writeStatsAsStruct"] = "false" }));
            Assert.Equal(MaintenanceReason.CheckpointLayout, Assert.Throws<CheckpointDeclinedException>(() =>
                CheckpointCapability.Check(protocol, MetaData(new Dictionary<string, string>() { ["delta.checkpoint.writeStatsAsJson"] = "FALSE" }))).Reason);
            Assert.Equal(MaintenanceReason.CheckpointLayout, Assert.Throws<CheckpointDeclinedException>(() =>
                CheckpointCapability.Check(protocol, MetaData(new Dictionary<string, string>() { ["delta.checkpoint.writeStatsAsStruct"] = "true" }))).Reason);
        }

        [Fact]
        public void ParsedOnlyRootDeclines()
        {
            // As ruled: counts convert to JSON stats, any parsed min, max or null count declines
            Assert.Equal("{\"numRecords\":42,\"tightBounds\":true}", CheckpointCapability.ParsedStatistics(42, true, false, false, false));
            Assert.Equal("{\"numRecords\":7}", CheckpointCapability.ParsedStatistics(7, null, false, false, false));
            Assert.True(CheckpointCapability.HasRecordCount(CheckpointCapability.ParsedStatistics(7, null, false, false, false)));
            foreach (var (min, max, nulls) in new[] { (true, false, false), (false, true, false), (false, false, true) })
            {
                var declined = Assert.Throws<CheckpointDeclinedException>(() => CheckpointCapability.ParsedStatistics(1, true, min, max, nulls));
                Assert.Equal(MaintenanceReason.ParsedOnlyRoot, declined.Reason);
                Assert.Equal(DeclineDependency.Root, declined.Dependency);
            }
        }

        private static byte[] Record(DeltaAction action) => TestRecords.FromAction(action)[0];

        [Fact]
        public void DomainsWithoutTheWriterFeatureDecline()
        {
            var domain = Record(new DeltaAction() { DomainMetadata = new DeltaDomainMetadataAction() { Domain = "d", Configuration = "{}" } });
            CheckpointCapability.CheckEmitted(domain, new HashSet<string>() { "domainMetadata" });
            var declined = Assert.Throws<CheckpointDeclinedException>(() => CheckpointCapability.CheckEmitted(domain, new HashSet<string>() { "deletionVectors" }));
            Assert.Equal(MaintenanceReason.DomainWithoutFeature, declined.Reason);
        }

        [Fact]
        public void DvRowsWithoutTheWriterFeatureDecline()
        {
            var vector = new DeletionVector() { StorageType = "u", PathOrInlineDv = "abcdefghijklmnopqrst", SizeInBytes = 1, Cardinality = 1 };
            var add = Record(new DeltaAction() { Add = new DeltaAddAction() { Path = "a", Statistics = "{\"numRecords\":3}", DeletionVector = vector } });
            var remove = Record(new DeltaAction() { Remove = new DeltaRemoveFileAction() { Path = "a", DeletionVector = vector } });
            var plain = Record(new DeltaAction() { Add = new DeltaAddAction() { Path = "b" } });
            var features = new HashSet<string>() { "deletionVectors" };
            CheckpointCapability.CheckEmitted(add, features);
            CheckpointCapability.CheckEmitted(remove, features);
            CheckpointCapability.CheckEmitted(plain, new HashSet<string>());
            Assert.Equal(MaintenanceReason.DvWithoutFeature, Assert.Throws<CheckpointDeclinedException>(() => CheckpointCapability.CheckEmitted(add, new HashSet<string>())).Reason);
            Assert.Equal(MaintenanceReason.DvWithoutFeature, Assert.Throws<CheckpointDeclinedException>(() => CheckpointCapability.CheckEmitted(remove, new HashSet<string>())).Reason);

            var uncounted = Record(new DeltaAction() { Add = new DeltaAddAction() { Path = "c", Statistics = "{\"minValues\":{}}", DeletionVector = vector } });
            Assert.Equal(MaintenanceReason.DvWithoutRecordCount, Assert.Throws<CheckpointDeclinedException>(() => CheckpointCapability.CheckEmitted(uncounted, features)).Reason);
        }

        private static readonly IReadOnlySet<string> WithDomains = new HashSet<string>() { "domainMetadata" };

        [Fact]
        public void UnmarkedRootRecordsAnInheritedGap()
        {
            var marker = CoverageMarker.Compute(30, 5_000, 1_000, hasRoot: true, rootVersion: 20, rootMarker: null, WithDomains, firstTailCommitTimestamp: 4_000, sorted: false);
            Assert.Equal(new CoverageGap(20, new[] { "remove", "txn", "domainMetadata" }, 4_000 + 3_600_000), marker.Inherited, new GapComparer());

            var noTail = CoverageMarker.Compute(30, 5_000, 1_000, true, 20, null, WithDomains, null, false);
            Assert.Equal(5_000 + 3_600_000, noTail.Inherited!.TsBound);
            var saturated = CoverageMarker.Compute(30, long.MaxValue - 5, 1_000, true, 20, null, WithDomains, null, false);
            Assert.Equal(long.MaxValue, saturated.Inherited!.TsBound);
        }

        private sealed class GapComparer : IEqualityComparer<CoverageGap?>
        {
            public bool Equals(CoverageGap? x, CoverageGap? y) => x?.AtOrBelow == y?.AtOrBelow && x?.TsBound == y?.TsBound && (x?.Families ?? Array.Empty<string>()).SequenceEqual(y?.Families ?? Array.Empty<string>());

            public int GetHashCode(CoverageGap? obj) => 0;
        }

        [Fact]
        public void GapOmitsDomainsWithoutTheWriterFeature()
        {
            var marker = CoverageMarker.Compute(30, 5_000, 1_000, true, 20, null, new HashSet<string>() { "deletionVectors" }, 4_000, false);
            Assert.Equal(new[] { "remove", "txn" }, marker.Inherited!.Families);
        }

        [Fact]
        public void NullFeatureNameDeclines()
        {
            // The tail encoder keeps a null list element, as the reader does
            var declined = Assert.Throws<CheckpointDeclinedException>(() => CheckpointCapability.Check(Protocol(1, 7, null, new[] { "appendOnly", null! }), MetaData()));
            Assert.Equal(MaintenanceReason.UnknownProtocolVersion, declined.Reason);
            Assert.Equal(DeclineDependency.Protocol, declined.Dependency);
        }

        [Fact]
        public void MarkedRootWithoutAGapPassesNoGapOn()
        {
            var complete = new CoverageMarker(20, 3_000, 900, null, true);
            Assert.Null(CoverageMarker.Compute(30, 5_000, 1_000, true, 20, complete, WithDomains, 4_000, false).Inherited);
        }

        [Fact]
        public void MarkedRootPassesItsGapOn()
        {
            var root = new CoverageMarker(20, 3_000, 900, new CoverageGap(10, new[] { "remove", "txn" }, 2_000), true);
            var marker = CoverageMarker.Compute(30, 5_000, 1_000, true, 20, root, WithDomains, 4_000, false);
            Assert.Same(root.Inherited, marker.Inherited);

            var replay = CoverageMarker.Compute(30, 5_000, 1_000, false, -1, null, WithDomains, 4_000, false);
            Assert.Null(replay.Inherited);
        }

        [Fact]
        public void GrowingRetentionKeepsTheRootCutoff()
        {
            var root = new CoverageMarker(20, 3_000, 2_500, null, false);
            Assert.Equal(2_500, CoverageMarker.Compute(30, 5_000, 1_000, true, 20, root, WithDomains, null, false).TombstoneCutoff);
            Assert.Equal(3_000, CoverageMarker.Compute(30, 5_000, 3_000, true, 20, root, WithDomains, null, false).TombstoneCutoff);
        }

        [Fact]
        public void MarkerRoundTrips()
        {
            var marker = new CoverageMarker(30, 5_000, -7, new CoverageGap(20, new[] { "remove", "txn", "domainMetadata" }, long.MaxValue), true);
            var parsed = CoverageMarker.Parse(marker.ToJson())!;
            Assert.Equal((marker.Version, marker.BuildNow, marker.TombstoneCutoff, marker.Sorted), (parsed.Version, parsed.BuildNow, parsed.TombstoneCutoff, parsed.Sorted));
            Assert.Equal(marker.Inherited, parsed.Inherited, new GapComparer());
            Assert.Null(CoverageMarker.Parse(new CoverageMarker(1, 2, 3, null, false).ToJson())!.Inherited);
        }

        [Fact]
        public void UnknownMarkerFormatCountsAsUnmarked()
        {
            var json = new CoverageMarker(30, 5_000, 1, null, true).ToJson();
            Assert.NotNull(CoverageMarker.Parse(json));
            Assert.Null(CoverageMarker.Parse(json.Replace("\"format\":1", "\"format\":2")));
            Assert.Null(CoverageMarker.Parse(null));
        }

        [Theory]
        [InlineData("not json")]
        [InlineData("[]")]
        [InlineData("{\"format\":1}")]
        [InlineData("{\"format\":1,\"version\":-1,\"buildNow\":1,\"tombstoneCutoff\":1,\"inherited\":null,\"sorted\":false}")]
        [InlineData("{\"format\":1,\"version\":5,\"buildNow\":1,\"tombstoneCutoff\":1,\"inherited\":{\"atOrBelow\":6,\"families\":[],\"tsBound\":1},\"sorted\":false}")]
        [InlineData("{\"format\":1,\"version\":5,\"buildNow\":1,\"tombstoneCutoff\":1,\"inherited\":{\"atOrBelow\":1,\"families\":[\"add\"],\"tsBound\":1},\"sorted\":false}")]
        [InlineData("{\"format\":1,\"version\":5,\"buildNow\":1,\"tombstoneCutoff\":1,\"inherited\":{\"atOrBelow\":1,\"families\":[\"txn\",\"txn\"],\"tsBound\":1},\"sorted\":false}")]
        [InlineData("{\"format\":1,\"version\":5,\"buildNow\":1.5,\"tombstoneCutoff\":1,\"inherited\":null,\"sorted\":false}")]
        [InlineData("{\"format\":1,\"version\":5,\"buildNow\":1,\"tombstoneCutoff\":1,\"inherited\":null,\"sorted\":\"yes\"}")]
        [InlineData("{\"format\":1,\"version\":5,\"buildNow\":1,\"tombstoneCutoff\":1,\"inherited\":{\"a\":{\"b\":{\"c\":{\"d\":1}}}},\"sorted\":false}")]
        public void MalformedMarkerIsBounded(string value)
        {
            Assert.Null(CoverageMarker.Parse(value));
        }

        [Fact]
        public void OversizedMarkerIsUnmarked()
        {
            var json = new CoverageMarker(30, 5_000, 1, null, true).ToJson();
            Assert.Null(CoverageMarker.Parse(json + new string(' ', CoverageMarker.MaxLength)));
        }

        [Theory]
        [InlineData("{\"numRecords\":1} garbage")]
        [InlineData("{\"numRecords\":1}{}")]
        public void RecordCountRequiresOneCompleteJsonDocument(string statistics)
        {
            Assert.False(CheckpointCapability.HasRecordCount(statistics));
        }

        [Fact]
        public void RecordCountBesideALoneSurrogateNameIsAnsweredNotThrown()
        {
            // An escaped property name the lookup has to unescape, holding a lone surrogate
            Assert.False(CheckpointCapability.HasRecordCount("{\"\\udc00bcde\":1}"));
        }

        [Fact]
        public void InvalidEscapedFamilyMakesMarkerUnmarked()
        {
            var marker = "{\"format\":1,\"version\":5,\"buildNow\":1,\"tombstoneCutoff\":1,\"inherited\":{\"atOrBelow\":1,\"families\":[\"\\uD800\"],\"tsBound\":1},\"sorted\":false}";
            Assert.Null(CoverageMarker.Parse(marker));
        }
    }
}
