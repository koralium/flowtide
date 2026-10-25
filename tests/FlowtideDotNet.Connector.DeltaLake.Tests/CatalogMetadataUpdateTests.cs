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

using FlowtideDotNet.Connector.DeltaLake.Internal.Catalog;
using FlowtideDotNet.Connector.DeltaLake.Internal.Delta;
using FlowtideDotNet.Connector.DeltaLake.Internal.Delta.Actions;
using FlowtideDotNet.Connector.DeltaLake.Internal.Delta.Checkpoints;
using FlowtideDotNet.Connector.DeltaLake.Internal.Delta.ParquetFormat.Bounded;
using FlowtideDotNet.Connector.DeltaLake.Internal.Delta.Schema;
using FlowtideDotNet.Connector.DeltaLake.Internal.Delta.Schema.Converters;
using FlowtideDotNet.Connector.DeltaLake.Internal.Delta.Schema.Types;
using FlowtideDotNet.Connector.DeltaLake.Internal.Maintenance;
using FlowtideDotNet.Storage.Memory;
using FlowtideDotNet.Storage.Persistence.Reservoir;
using FlowtideDotNet.Storage.Persistence.Reservoir.Internal;
using FlowtideDotNet.Storage.Persistence.Reservoir.MemoryDisk;
using FlowtideDotNet.Storage.StateManager;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Time.Testing;
using Stowage;
using System.Diagnostics.Metrics;
using System.Text.Json;
using static FlowtideDotNet.Connector.DeltaLake.Tests.DeltaTestKit;

namespace FlowtideDotNet.Connector.DeltaLake.Tests
{
    public class CatalogMetadataUpdateTests : IDisposable
    {
        private const string Published = "interval 168 hours";

        private readonly string _directory = Path.Combine(Path.GetTempPath(), "cm-" + ScratchOwners.ShortId());
        private readonly ScratchOwners _owners = new ScratchOwners();

        public CatalogMetadataUpdateTests()
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
        }

        private static void Retained(DeltaLakeOptions options)
        {
            options.HistoryRetention = TimeSpan.FromDays(7);
            options.AssumeCompleteStorageListing = true;
        }

        private static List<KeyValuePair<string, string>> Sorted(IReadOnlyDictionary<string, string>? map)
        {
            return map == null ? new List<KeyValuePair<string, string>>() : map.OrderBy(x => x.Key, StringComparer.Ordinal).ToList();
        }

        private static async Task WriteText(IFileStorage storage, string path, string text)
        {
            using var write = await storage.OpenWrite(path);
            await write.WriteAsync(System.Text.Encoding.UTF8.GetBytes(text));
        }

        // An idle sink with retention on publishes the properties at version 1
        private static async Task<DeltaMetadataAction> PublishProperties(string testName, IFileStorage storage)
        {
            await using var stream = new DeltaLakeSinkStream(testName, storage, Retained);
            stream.WaitForUpdateDoesNotRequireDataChange();
            await stream.StartStream(UserInsert);
            await WaitForVersionCheckpointing(storage, "test", stream, 1);
            return Assert.Single(await ReadCommitActions(storage, "test", 1), x => x.MetaData != null).MetaData!;
        }

        [Theory]
        [InlineData(false)]
        [InlineData(true)]
        public async Task MetadataRewritePreservesAllProtocolFieldsFromJsonAndCheckpoint(bool checkpointRoot)
        {
            var testName = $"{nameof(MetadataRewritePreservesAllProtocolFieldsFromJsonAndCheckpoint)}_{checkpointRoot}";
            var storage = Files.Of.InternalMemory($"./{testName}");
            var schema = JsonSerializer.Serialize(UserSchema);
            // Every field set, a provider other than the default, null map values, a retention key in another case and an older retention
            await WriteText(storage, CommitPath("test", 0), string.Join("\n",
                "{\"commitInfo\":{\"timestamp\":1,\"operation\":\"CREATE TABLE\"}}",
                "{\"protocol\":{\"minReaderVersion\":1,\"minWriterVersion\":2}}",
                "{\"metaData\":{\"id\":\"table-id\",\"name\":\"named\",\"description\":\"described\",\"format\":{\"provider\":\"custom-format\",\"options\":{\"compression\":\"zstd\",\"empty\":null}},"
                + $"\"schemaString\":{schema},\"partitionColumns\":[\"name\"],\"createdTime\":1700000000000,"
                + "\"configuration\":{\"custom\":\"kept\",\"nullable\":null,\"DELTA.LOGRETENTIONDURATION\":\"interval 30 days\",\"delta.deletedFileRetentionDuration\":\"interval 7 days\",\"delta.enableChangeDataFeed\":\"false\"}}}") + "\n");
            if (checkpointRoot)
            {
                await LegacyCheckpointWriter.WriteCheckpoint(storage, "test", (await DeltaTransactionReader.ReadTable(storage, "test"))!);
            }
            var source = (await DeltaTransactionReader.ReadTable(storage, "test"))!;
            Assert.Equal(checkpointRoot ? 0 : null, source.StartCheckpointVersion);
            // Preservation is measured against the persisted metaData, which the reader's model of either root keeps whole
            var metadata = Assert.Single(await ReadCommitActions(storage, "test", 0), x => x.MetaData != null).MetaData!;
            Assert.Equal(Sorted(metadata.Configuration), Sorted(source.Metadata.Configuration));
            Assert.Equal(Sorted(metadata.Format!.Options), Sorted(source.Metadata.Format!.Options));

            var written = await PublishProperties(testName, storage);

            Assert.Equal(metadata.Id, written.Id);
            Assert.Equal(metadata.Name, written.Name);
            Assert.Equal(metadata.Description, written.Description);
            Assert.Equal(metadata.CreatedTime, written.CreatedTime);
            Assert.Equal(metadata.SchemaString, written.SchemaString);
            Assert.Equal("custom-format", metadata.Format!.Provider);
            Assert.Equal(metadata.Format.Provider, written.Format!.Provider);
            Assert.Equal(Sorted(metadata.Format.Options), Sorted(written.Format.Options));
            Assert.Equal(new[] { "name" }, written.PartitionColumns);
            Assert.Equal(metadata.PartitionColumns, written.PartitionColumns);
            // Only the two exact keys change, another spelling stays as it is
            var expected = new Dictionary<string, string>(metadata.Configuration!);
            Assert.Equal("interval 30 days", expected["DELTA.LOGRETENTIONDURATION"]);
            expected[RetentionProperties.LogKey] = Published;
            expected[RetentionProperties.DeletedFileKey] = Published;
            Assert.Equal(Sorted(expected), Sorted(written.Configuration));
            Assert.Null(written.Configuration!["nullable"]);
            Assert.Null(written.Format.Options!["empty"]);
        }

        [Theory]
        [InlineData(false)]
        [InlineData(true)]
        public async Task NullFeaturePropertiesReadAsDisabled(bool checkpointRoot)
        {
            var testName = $"{nameof(NullFeaturePropertiesReadAsDisabled)}_{checkpointRoot}";
            var storage = Files.Of.InternalMemory($"./{testName}");
            var schema = JsonSerializer.Serialize(UserSchema);
            // Both writer features on, with null values under their keys and under the column mapping mode
            await WriteText(storage, CommitPath("test", 0), string.Join("\n",
                "{\"commitInfo\":{\"timestamp\":1,\"operation\":\"CREATE TABLE\"}}",
                "{\"protocol\":{\"minReaderVersion\":3,\"minWriterVersion\":7,\"readerFeatures\":[\"deletionVectors\"],\"writerFeatures\":[\"deletionVectors\",\"changeDataFeed\"]}}",
                "{\"metaData\":{\"id\":\"table-id\",\"format\":{\"provider\":\"parquet\",\"options\":{}}," + $"\"schemaString\":{schema},\"partitionColumns\":[],\"createdTime\":1700000000000,"
                + "\"configuration\":{\"delta.enableDeletionVectors\":null,\"delta.enableChangeDataFeed\":null,\"delta.columnMapping.mode\":null}}}") + "\n");
            if (checkpointRoot)
            {
                await LegacyCheckpointWriter.WriteCheckpoint(storage, "test", (await DeltaTransactionReader.ReadTable(storage, "test"))!);
            }

            var table = (await DeltaTransactionReader.ReadTable(storage, "test"))!;

            Assert.Equal(checkpointRoot ? 0 : null, table.StartCheckpointVersion);
            Assert.Null(table.Metadata.Configuration!["delta.enableDeletionVectors"]);
            Assert.False(table.DeleteVectorEnabled);
            Assert.False(table.ChangeDataEnabled);
            Assert.False(table.ColumnMappingEnabled);
        }

        [Fact]
        public async Task CheckpointReadKeepsNullValuesOnlyInTheMetadataMaps()
        {
            var testName = nameof(CheckpointReadKeepsNullValuesOnlyInTheMetadataMaps);
            var storage = Files.Of.InternalMemory($"./{testName}");
            var schema = JsonSerializer.Serialize(UserSchema);
            await WriteText(storage, CommitPath("test", 0), string.Join("\n",
                "{\"commitInfo\":{\"timestamp\":1,\"operation\":\"CREATE TABLE\"}}",
                "{\"protocol\":{\"minReaderVersion\":1,\"minWriterVersion\":2}}",
                "{\"metaData\":{\"id\":\"table-id\",\"format\":{\"provider\":\"parquet\",\"options\":{\"empty\":null}}," + $"\"schemaString\":{schema},\"partitionColumns\":[\"name\"],\"createdTime\":1700000000000,"
                + "\"configuration\":{\"nullable\":null}}}",
                "{\"add\":{\"path\":\"name=a/part-0.parquet\",\"partitionValues\":{\"name\":null},\"size\":1,\"modificationTime\":1,\"dataChange\":true,\"tags\":{\"kept\":\"tag\",\"empty\":null}}}") + "\n");
            var json = (await DeltaTransactionReader.ReadTable(storage, "test"))!;
            Assert.Null(Assert.Single(json.AddFiles).PartitionValues!["name"]);
            await LegacyCheckpointWriter.WriteCheckpoint(storage, "test", json);

            var table = (await DeltaTransactionReader.ReadTable(storage, "test"))!;

            Assert.Equal(0, table.StartCheckpointVersion);
            Assert.Null(table.Metadata.Configuration!["nullable"]);
            Assert.Null(table.Metadata.Format!.Options!["empty"]);
            // A null partition value or tag is dropped
            var add = Assert.Single(table.AddFiles);
            Assert.Empty(add.PartitionValues!);
            Assert.Equal(new[] { "kept" }, add.Tags!.Keys);
        }

        [Fact]
        public async Task SchemaStringIsCopiedVerbatim()
        {
            var testName = nameof(SchemaStringIsCopiedVerbatim);
            var storage = Files.Of.InternalMemory($"./{testName}");
            var schema = "{ \"type\" : \"struct\",  \"fields\" : [ { \"name\" : \"userkey\", \"type\" : \"long\", \"nullable\" : true, \"metadata\" : { \"comment\" : \"nøkkel – 𝄞\" } },"
                + " { \"name\" : \"name\", \"type\" : \"string\", \"nullable\" : true, \"metadata\" : { } } ] }";
            await WriteCommit(storage, "test", 0, Protocol(), Metadata(schema));

            var written = await PublishProperties(testName, storage);

            Assert.Equal(schema, written.SchemaString, StringComparer.Ordinal);
            var table = (await DeltaTransactionReader.ReadTable(storage, "test"))!;
            Assert.Equal(schema, table.Metadata.SchemaString, StringComparer.Ordinal);
            Assert.Equal(new[] { "userkey", "name" }, table.Schema.Fields.Select(x => x.Name));
            // The parsed schema written again is another string
            var options = new JsonSerializerOptions();
            options.Converters.Add(new TypeConverter());
            Assert.NotEqual(schema, JsonSerializer.Serialize(table.Schema as SchemaBaseType, options));
        }

        [Fact]
        public void MetadataCopySharesNothingWithItsSource()
        {
            var source = new DeltaMetadataAction()
            {
                Id = "id",
                Format = new DeltaMetadataFormat() { Provider = "parquet", Options = new Dictionary<string, string>() { ["o"] = "1" } },
                SchemaString = UserSchema,
                PartitionColumns = new List<string>() { "name" },
                Configuration = new Dictionary<string, string>() { ["custom"] = "kept", ["Delta.DeletedFileRetentionDuration"] = "interval 1 days", ["nullable"] = null! }
            };
            var copy = DeltaMetadataCopy.Copy(source, Published);

            Assert.NotSame(source.Format, copy.Format);
            Assert.NotSame(source.Format.Options, copy.Format!.Options);
            Assert.NotSame(source.PartitionColumns, copy.PartitionColumns);
            Assert.NotSame(source.Configuration, copy.Configuration);
            Assert.Equal(new[] { "Delta.DeletedFileRetentionDuration", "custom", RetentionProperties.DeletedFileKey, RetentionProperties.LogKey, "nullable" }, copy.Configuration!.Keys.Order(StringComparer.Ordinal));
            Assert.Equal("interval 1 days", copy.Configuration["Delta.DeletedFileRetentionDuration"]);
            Assert.Null(copy.Configuration["nullable"]);
            // The table model writes into its metadata's configuration in place
            var table = new DeltaTable(copy, new DeltaProtocolAction(), new List<DeltaAddAction>(), new StructType(new List<StructField>()), 0, Array.Empty<DeltaRemoveFileAction>(),
                new Dictionary<string, DeltaTransactionAction>(), new Dictionary<string, DeltaDomainMetadataAction>(), null);
            table.MaxColumnId = 9;
            copy.Format.Options!["o"] = "2";
            copy.PartitionColumns!.Add("userkey");
            Assert.Equal(new[] { "Delta.DeletedFileRetentionDuration", "custom", "nullable" }, source.Configuration.Keys.Order(StringComparer.Ordinal));
            Assert.Equal("1", source.Format.Options["o"]);
            Assert.Equal(new[] { "name" }, source.PartitionColumns);

            // Without a retention value the configuration is copied unchanged; null fields stay null
            var unchanged = DeltaMetadataCopy.Copy(source, null);
            Assert.Equal(Sorted(source.Configuration), Sorted(unchanged.Configuration));
            Assert.NotSame(source.Configuration, unchanged.Configuration);
            var empty = DeltaMetadataCopy.Copy(new DeltaMetadataAction(), null);
            Assert.Null(empty.Format);
            Assert.Null(empty.PartitionColumns);
            Assert.Null(empty.Configuration);
            Assert.Equal(2, DeltaMetadataCopy.Copy(new DeltaMetadataAction(), Published).Configuration!.Count);
        }

        [Fact]
        public async Task PropertiesPublicationPreservesProjection()
        {
            const string table = "catalog_metadata_update";
            var storage = Files.Of.InternalMemory($"./{table}");
            await WriteCommit(storage, table, 0, new[] { Protocol(), Metadata(UserSchema) }.Concat(Enumerable.Range(0, 8).Select(FileAdd)).ToArray());
            var options = new DeltaLakeOptions() { StorageLocation = storage, ReservationOverride = new PruningReservation() };
            var persistence = new ReservoirPersistentStorage(new ReservoirStorageOptions() { FileProvider = new MemoryFileProvider() });
            using var persistenceScope = persistence;
            using var stateManager = new StateManagerSync<object>(new StateManagerOptions() { CachePageCount = 64, MinCachePageCount = 1000, PersistentStorage = persistence },
                NullLoggerFactory.Instance, new Meter(table), table, GlobalMemoryManager.Instance);
            await stateManager.InitializeAsync();
            using var catalog = await DeltaSinkCatalog.Open(stateManager.GetOrCreateClient("catalog"), options, table, new[] { "userkey", "name" }, GlobalMemoryManager.Instance, NullLogger.Instance);
            var readOptions = new DeltaReadOptions() { SkipTombstones = true };
            await catalog.Validate(readOptions);
            var layout = catalog.ScanLayout;
            var schema = catalog.Header!.Schema;
            var protocol = catalog.Header.Protocol;
            var files = await FileRecords(catalog);
            var bounds = await BoundRows(catalog);
            Assert.Equal(8, files.Count);

            // The sink's properties commit at version 1
            var updated = DeltaMetadataCopy.Copy(catalog.Header.Metadata, Published);
            await WriteOwnCommit(storage, table, 1, 1, 2, new DeltaAction() { MetaData = updated });
            var overlay = catalog.BeginCommit(1, null);
            overlay.UpdatedMetadata = updated;
            catalog.MarkPublished(overlay);
            await catalog.ApplyPublished();

            Assert.Equal(1, catalog.Head);
            Assert.Same(updated, catalog.Header.Metadata);
            Assert.True(RetentionProperties.Matches(catalog.Header.Metadata.Configuration, Published));
            // No new schema, projection or file set
            Assert.Same(layout, catalog.ScanLayout);
            Assert.Same(schema, catalog.Header.Schema);
            Assert.Same(protocol, catalog.Header.Protocol);
            Assert.Equal(files, await FileRecords(catalog));
            Assert.Equal(bounds, await BoundRows(catalog));
            Assert.Equal(8, catalog.LiveFiles);

            // The log agrees with the catalog: the next listing keeps it
            await catalog.Validate(readOptions);
            Assert.Same(layout, catalog.ScanLayout);
            Assert.Same(updated, catalog.Header.Metadata);
        }

        private static DeltaAction FileAdd(int key) => new DeltaAction()
        {
            Add = new DeltaAddAction()
            {
                Path = $"part-{key}.parquet",
                PartitionValues = new Dictionary<string, string>(),
                Size = 100,
                ModificationTime = 1,
                DataChange = true,
                Statistics = $"{{\"numRecords\":1,\"minValues\":{{\"userkey\":{key}}},\"maxValues\":{{\"userkey\":{key}}},\"nullCount\":{{\"userkey\":0}}}}"
            }
        };

        private static async Task<List<string>> FileRecords(DeltaSinkCatalog catalog)
        {
            var records = new List<string>();
            await foreach (var (id, record) in catalog.ScanFiles())
            {
                records.Add($"{id} {JsonSerializer.Serialize(record.ToAdd())}");
            }
            return records;
        }

        private static async Task<List<string>> BoundRows(DeltaSinkCatalog catalog)
        {
            var rows = new List<string>();
            await catalog.ScanBounds((id, row) => rows.Add($"{id} {Convert.ToHexString(row.ToArray())}"));
            return rows;
        }

        [Fact]
        public async Task UnknownMetadataFieldsDoNotChangePhaseThreeBuildBehavior()
        {
            var schema = JsonSerializer.Serialize(UserSchema);
            // Lines per table, with and without members the action model does not know
            async Task<(Built Before, Built After)> Run(bool unknown)
            {
                var storage = Files.Of.InternalMemory($"./unknown-metadata-{unknown}-{Guid.NewGuid():N}");
                var extra = unknown ? ",\"futureField\":{\"nested\":[1,{\"x\":null}]},\"futureFlag\":true" : "";
                var formatExtra = unknown ? ",\"futureOption\":\"x\"" : "";
                await WriteText(storage, CommitPath("t", 0), string.Join("\n",
                    "{\"commitInfo\":{\"timestamp\":1}}",
                    "{\"protocol\":{\"minReaderVersion\":1,\"minWriterVersion\":2}}",
                    $"{{\"metaData\":{{\"id\":\"table-id\",\"format\":{{\"provider\":\"parquet\",\"options\":{{}}{formatExtra}}},\"schemaString\":{schema},\"partitionColumns\":[],\"configuration\":{{}}{extra}}}}}") + "\n");
                await WriteCommit(storage, "t", 1, DeltaLakeCheckpointBuildTests.Add("part-1.parquet"));
                var before = await Build(storage, "t", 1);

                // The sink's rewrite drops the unknown members and adds the properties
                var header = (await DeltaTransactionReader.ReadTable(storage, "t"))!;
                await WriteOwnCommit(storage, "t", 2, 1, 3, new DeltaAction() { MetaData = DeltaMetadataCopy.Copy(header.Metadata, Published) });
                var after = await Build(storage, "t", 2);
                return (before, after);
            }

            var known = await Run(false);
            var unknown = await Run(true);

            Assert.Equal(BuildStatus.Completed, unknown.Before.Result.Status);
            Assert.Equal(BuildStatus.Completed, unknown.After.Result.Status);
            Assert.Equal(known.Before.Lines, unknown.Before.Lines);
            Assert.Equal(known.After.Lines, unknown.After.Lines);
            var metaData = Assert.Single(unknown.After.Lines, x => x.Contains("\"metaData\""));
            Assert.Contains($"\"{RetentionProperties.LogKey}\":\"{Published}\"", metaData);
            Assert.Contains($"\"{RetentionProperties.DeletedFileKey}\":\"{Published}\"", metaData);
            Assert.DoesNotContain("future", metaData);
        }

        private sealed record Built(BuildResult Result, List<string> Lines);

        // Builds into a fresh scratch folder, unpublished, and reads the output back
        private async Task<Built> Build(IFileStorage storage, string table, long version)
        {
            var directory = _owners.Add(new ScratchDirectory(Path.Combine(_directory, ScratchOwners.ShortId())));
            var memory = new JobMemory();
            var lines = new List<string>();
            BuildResult result;
            using (var scratch = directory.CreateJob())
            {
                var context = new CheckpointBuildContext()
                {
                    Storage = storage,
                    Table = table,
                    Scratch = scratch,
                    Memory = memory,
                    Clock = new FakeTimeProvider(new DateTimeOffset(2026, 10, 1, 12, 0, 0, TimeSpan.Zero)),
                    Publish = false
                };
                result = await CheckpointBuilder.BuildAsync(context, new BuildRequest() { Version = version, Root = RootPolicy.Auto, KnownUnsupported = new HashSet<ObjectIdentity>() });
                if (result.Checkpoint != null)
                {
                    var scan = CheckpointPageScan.Scan(result.Checkpoint.LocalPath, memory);
                    var records = new List<byte[]>();
                    using (var reader = new BoundedCheckpointReader(result.Checkpoint.LocalPath, scan, memory))
                    {
                        reader.Read(record => records.Add(record.ToArray()));
                    }
                    memory.Release(scan.ModelBytes);
                    lines = CheckpointOracle.Canonical(records.Select(x => CheckpointRecord.ToAction(x)));
                }
            }
            _owners.Release(directory);
            return new Built(result, lines);
        }
    }
}
