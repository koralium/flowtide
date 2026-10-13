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
using FlowtideDotNet.Connector.DeltaLake.Internal.Delta.ParquetFormat.Bounded;
using FlowtideDotNet.Connector.DeltaLake.Internal.Maintenance;
using ParquetSharp;
using Stowage;
using Xunit.Abstractions;

namespace FlowtideDotNet.Connector.DeltaLake.Tests
{
    public class CheckpointPageScanTests : IDisposable
    {
        private readonly ITestOutputHelper _output;
        private readonly string _directory;

        public CheckpointPageScanTests(ITestOutputHelper output)
        {
            _output = output;
            _directory = Path.Combine(Path.GetTempPath(), "flowtide-scan-" + Guid.NewGuid().ToString("N"));
            Directory.CreateDirectory(_directory);
        }

        public void Dispose()
        {
            Directory.Delete(_directory, true);
        }

        internal static IEnumerable<byte[]> RealisticRecords(int adds)
        {
            var vector = new DeletionVector() { StorageType = "u", PathOrInlineDv = "abcdefghijklmnopqrst", Offset = 1, SizeInBytes = 40, Cardinality = 3 };
            var actions = new List<DeltaAction>()
            {
                new DeltaAction() { Protocol = new DeltaProtocolAction() { MinReaderVersion = 3, MinWriterVersion = 7, ReaderFeatures = new List<string>() { "deletionVectors" }, WriterFeatures = new List<string>() { "deletionVectors", "domainMetadata", "rowTracking" } } },
                new DeltaAction() { MetaData = new DeltaMetadataAction() { Id = "id", SchemaString = DeltaTestKit.UserSchema, Format = new DeltaMetadataFormat() { Provider = "parquet", Options = new Dictionary<string, string>() }, PartitionColumns = new List<string>() { "p" }, Configuration = new Dictionary<string, string>() { ["delta.enableDeletionVectors"] = "true" }, CreatedTime = 1 } },
                new DeltaAction() { Txn = new DeltaTransactionAction() { AppId = "app", Version = 3, LastUpdated = 4 } },
                new DeltaAction() { DomainMetadata = new DeltaDomainMetadataAction() { Domain = "delta.rowTracking", Configuration = "{}" } },
                new DeltaAction() { Remove = new DeltaRemoveFileAction() { Path = "gone.parquet", DeletionTimestamp = 5, ExtendedFileMetadata = true, PartitionValues = new Dictionary<string, string>() { ["p"] = "1" }, Size = 10 } }
            };
            for (int i = 0; i < adds; i++)
            {
                actions.Add(new DeltaAction()
                {
                    Add = new DeltaAddAction()
                    {
                        Path = $"p={i % 7}/part-{i:D7}-c000.zstd.parquet",
                        PartitionValues = new Dictionary<string, string>() { ["p"] = (i % 7).ToString() },
                        Size = 1000 + i,
                        ModificationTime = 1_760_000_000_000 + i,
                        DataChange = true,
                        Statistics = "{\"numRecords\":" + i + ",\"minValues\":{\"userkey\":" + i + "},\"maxValues\":{\"userkey\":" + (i + 10) + "},\"nullCount\":{\"userkey\":0}}",
                        DeletionVector = i % 5 == 0 ? vector : null,
                        BaseRowId = i * 100L,
                        DefaultRowCommitVersion = 2
                    }
                });
            }
            return actions.SelectMany(x => TestRecords.FromAction(x));
        }

        private string WriteOwn(IEnumerable<byte[]> records, CheckpointWriterLimits? limits = null, Compression compression = Compression.Zstd)
        {
            var path = Path.Combine(_directory, Guid.NewGuid().ToString("N") + ".parquet");
            using var writer = new CheckpointFileWriter(path, new DiskLedger(), new JobMemory(), compression, new CoverageMarker(1, 1, 1, null, false).ToJson(), limits);
            foreach (var record in records)
            {
                writer.Append(record);
            }
            writer.Close();
            return path;
        }

        private string WriteSynthetic(SyntheticParquet parquet)
        {
            var path = Path.Combine(_directory, Guid.NewGuid().ToString("N") + ".parquet");
            File.WriteAllBytes(path, parquet.Build());
            return path;
        }

        private static CheckpointDeclinedException Declines(string path, ScanLimits? limits = null)
        {
            var declined = Assert.Throws<CheckpointDeclinedException>(() => CheckpointPageScan.Scan(path, new JobMemory(), limits));
            Assert.Equal(MaintenanceReason.OutOfEnvelope, declined.Reason);
            Assert.Equal(DeclineDependency.Root, declined.Dependency);
            return declined;
        }

        [Theory]
        [InlineData(Compression.Zstd)]
        [InlineData(Compression.Snappy)]
        [InlineData(Compression.Uncompressed)]
        [InlineData(Compression.Gzip)]
        [InlineData(Compression.Lz4)]
        [InlineData(Compression.Lz4Hadoop)]
        public void OwnOutputPassesTheScan(Compression compression)
        {
            var path = WriteOwn(RealisticRecords(20_000), compression: compression);
            var memory = new JobMemory();
            var result = CheckpointPageScan.Scan(path, memory);
            _output.WriteLine($"D {result.DecodeBound}, M {result.MetadataBound}, pool {result.PoolVisible}, groups {result.Footer.RowGroupRows.Count}");

            Assert.Equal(CheckpointSchema.LeafCount, result.Projection.Leaves.Count(x => x.Role == ProjectedRole.Field));
            Assert.Equal(CheckpointSchema.LeafCount, result.Projection.Leaves.Select(x => x.Field).Distinct().Count());
            Assert.Equal(20_005, result.Footer.RowGroupRows.Sum());
            Assert.NotNull(result.Footer.Marker);
            Assert.True(result.DecodeBound <= ScanLimits.Default.DecodeCap);
        }

        [Fact]
        public void OwnOutputAtTheActionCapFitsTheDecodeCap()
        {
            // One action holds at most the action cap, so each carries a single value at the cap in a different string leaf
            var cap = new string('x', (4 << 20) - 1024);
            var map = new Dictionary<string, string>() { [cap] = "v" };
            var vector = new DeletionVector() { StorageType = cap, PathOrInlineDv = "p" };
            var actions = new List<DeltaAction>()
            {
                new DeltaAction() { Protocol = new DeltaProtocolAction() { MinReaderVersion = 1, MinWriterVersion = 2, ReaderFeatures = new List<string>() { cap } } },
                new DeltaAction() { Protocol = new DeltaProtocolAction() { MinReaderVersion = 1, MinWriterVersion = 2, WriterFeatures = new List<string>() { cap } } },
                new DeltaAction() { MetaData = new DeltaMetadataAction() { Id = cap } },
                new DeltaAction() { MetaData = new DeltaMetadataAction() { Name = cap } },
                new DeltaAction() { MetaData = new DeltaMetadataAction() { Description = cap } },
                new DeltaAction() { MetaData = new DeltaMetadataAction() { SchemaString = cap } },
                new DeltaAction() { MetaData = new DeltaMetadataAction() { Format = new DeltaMetadataFormat() { Provider = cap } } },
                new DeltaAction() { MetaData = new DeltaMetadataAction() { Format = new DeltaMetadataFormat() { Options = map } } },
                new DeltaAction() { MetaData = new DeltaMetadataAction() { Format = new DeltaMetadataFormat() { Options = new Dictionary<string, string>() { ["k"] = cap } } } },
                new DeltaAction() { MetaData = new DeltaMetadataAction() { PartitionColumns = new List<string>() { cap } } },
                new DeltaAction() { MetaData = new DeltaMetadataAction() { Configuration = map } },
                new DeltaAction() { MetaData = new DeltaMetadataAction() { Configuration = new Dictionary<string, string>() { ["k"] = cap } } },
                new DeltaAction() { Txn = new DeltaTransactionAction() { AppId = cap } },
                new DeltaAction() { DomainMetadata = new DeltaDomainMetadataAction() { Domain = cap } },
                new DeltaAction() { DomainMetadata = new DeltaDomainMetadataAction() { Domain = "d", Configuration = cap } },
                new DeltaAction() { Add = new DeltaAddAction() { Path = cap } },
                new DeltaAction() { Add = new DeltaAddAction() { Path = "a", Statistics = cap } },
                new DeltaAction() { Add = new DeltaAddAction() { Path = "b", ClusteringProvider = cap } },
                new DeltaAction() { Add = new DeltaAddAction() { Path = "c", PartitionValues = map } },
                new DeltaAction() { Add = new DeltaAddAction() { Path = "d", PartitionValues = new Dictionary<string, string>() { ["k"] = cap } } },
                new DeltaAction() { Add = new DeltaAddAction() { Path = "e", Tags = map } },
                new DeltaAction() { Add = new DeltaAddAction() { Path = "f", Tags = new Dictionary<string, string>() { ["k"] = cap } } },
                new DeltaAction() { Add = new DeltaAddAction() { Path = "g", DeletionVector = vector } },
                new DeltaAction() { Add = new DeltaAddAction() { Path = "h", DeletionVector = new DeletionVector() { StorageType = "u", PathOrInlineDv = cap } } },
                new DeltaAction() { Remove = new DeltaRemoveFileAction() { Path = cap } },
                new DeltaAction() { Remove = new DeltaRemoveFileAction() { Path = "r", PartitionValues = map } },
                new DeltaAction() { Remove = new DeltaRemoveFileAction() { Path = "s", PartitionValues = new Dictionary<string, string>() { ["k"] = cap } } },
                new DeltaAction() { Remove = new DeltaRemoveFileAction() { Path = "t", DeletionVector = vector } },
                new DeltaAction() { Remove = new DeltaRemoveFileAction() { Path = "u", DeletionVector = new DeletionVector() { StorageType = "u", PathOrInlineDv = cap } } }
            };
            var path = WriteOwn(actions.SelectMany(x => TestRecords.FromAction(x)).Concat(RealisticRecords(1000).Skip(2)));
            var result = CheckpointPageScan.Scan(path, new JobMemory());
            _output.WriteLine($"D {result.DecodeBound}, M {result.MetadataBound}, groups {result.Footer.RowGroupRows.Count}");
            Assert.True(result.DecodeBound <= ScanLimits.Default.DecodeCap);
        }

        [Fact]
        public void EveryTestdataRootIsScanned()
        {
            var outcomes = new List<string>();
            foreach (var file in Directory.EnumerateFiles("../../../testdata", "*.checkpoint.parquet", SearchOption.AllDirectories).Order())
            {
                string outcome;
                try
                {
                    var result = CheckpointPageScan.Scan(file, new JobMemory());
                    outcome = $"admitted, D {result.DecodeBound}, M {result.MetadataBound}, leaves {result.Projection.Leaves.Count}";
                }
                catch (CheckpointDeclinedException e)
                {
                    outcome = $"declined: {e.Message}";
                }
                outcomes.Add($"{Path.GetRelativePath("../../../testdata", file)}: {outcome}");
            }
            foreach (var outcome in outcomes)
            {
                _output.WriteLine(outcome);
            }
            Assert.NotEmpty(outcomes);
            Assert.All(outcomes, x => Assert.Contains("admitted", x));
        }

        [Fact]
        public async Task LegacyRootPassesTheScan()
        {
            var storage = Files.Of.LocalDisk(_directory);
            await LegacyTableGenerator.Create(storage, "t", 20_000, 4, 0, true);
            var result = CheckpointPageScan.Scan(Path.Combine(_directory, "t", "_delta_log", $"{0:D20}.checkpoint.parquet"), new JobMemory());
            Assert.Null(result.Footer.Marker);
            Assert.Equal(20_002, result.Footer.RowGroupRows.Sum());
        }

        [Theory]
        [InlineData(false)]
        [InlineData(true)]
        public void DataPageV2FixturesAreAdmitted(bool dictionary)
        {
            var limits = new CheckpointWriterLimits()
            {
                PropertiesForTests = compression =>
                {
                    using var builder = new WriterPropertiesBuilder().Compression(compression).DataPageVersion(ParquetDataPageVersion.V2).DisableStatistics();
                    if (dictionary)
                    {
                        builder.EnableDictionary();
                    }
                    else
                    {
                        builder.DisableDictionary();
                    }
                    return builder.Build();
                }
            };
            var result = CheckpointPageScan.Scan(WriteOwn(RealisticRecords(5_000), limits), new JobMemory());
            Assert.Equal(5_005, result.Footer.RowGroupRows.Sum());
        }

        [Theory]
        [InlineData(ParquetSharp.Encoding.DeltaBinaryPacked, "add.size")]
        [InlineData(ParquetSharp.Encoding.DeltaLengthByteArray, "add.path")]
        [InlineData(ParquetSharp.Encoding.DeltaByteArray, "add.stats")]
        public void DeltaEncodingsDecline(ParquetSharp.Encoding encoding, string column)
        {
            var limits = new CheckpointWriterLimits()
            {
                PropertiesForTests = compression => DeltaTestKit.Properties(b => b.Compression(compression).DisableDictionary().Encoding(column, encoding))
            };
            var declined = Declines(WriteOwn(RealisticRecords(100), limits));
            Assert.Contains("encoding", declined.Message);
        }

        // A file whose schema is root, add, add.path, with the given chunks of add.path
        // A page header built field by field, then a body of the given length
        private static byte[] HandPage(Action<ThriftWriter> fields, int body)
        {
            var header = new ThriftWriter().BeginElement();
            fields(header);
            header.End();
            return header.ToArray().Concat(new byte[body]).ToArray();
        }

        public static TheoryData<string, string> HeaderGuards => new TheoryData<string, string>()
        {
            { "repeated", "Thrift field 3 is repeated" },
            { "wire type", "Expected an i32" },
            { "uncompressed V2", "an invalid V2 page header" },
            { "no sub-header", "without its own sub-header" },
            { "extra sub-header", "without its own sub-header" }
        };

        [Theory]
        [MemberData(nameof(HeaderGuards))]
        public void PageHeaderGuardsDecline(string guard, string message)
        {
            Action<ThriftWriter> data = h => h.BeginStruct(5).I32(1, 10).I32(2, 0).I32(3, 3).I32(4, 3).End();
            var page = guard switch
            {
                "repeated" => HandPage(h => { h.I32(1, 0).I32(2, 8).I32(3, 8).I32(3, 8); data(h); }, 8),
                "wire type" => HandPage(h => { h.I32(1, 0).I64(2, 8).I32(3, 8); data(h); }, 8),
                "uncompressed V2" => HandPage(h => h.I32(1, 3).I32(2, 9).I32(3, 8).BeginStruct(8).I32(1, 10).I32(2, 0).I32(3, 10).I32(4, 0).I32(5, 0).I32(6, 0).Bool(7, false).End(), 8),
                "no sub-header" => HandPage(h => h.I32(1, 0).I32(2, 8).I32(3, 8), 8),
                _ => HandPage(h => { h.I32(1, 0).I32(2, 8).I32(3, 8); data(h); h.BeginStruct(7).I32(1, 10).I32(2, 0).End(); }, 8)
            };
            Assert.Contains(message, Declines(WriteSynthetic(OneLeaf(PathChunk(page)))).Message);
        }

        [Fact]
        public void FieldThatAppearsTwiceDeclines()
        {
            var parquet = new SyntheticParquet();
            parquet.Schema.Add(new SyntheticParquet.Element("schema", Repetition: 0, Children: 1));
            parquet.Schema.Add(new SyntheticParquet.Element("add", Children: 2));
            parquet.Schema.Add(new SyntheticParquet.Element("path", Type: 6));
            parquet.Schema.Add(new SyntheticParquet.Element("path", Type: 6));
            parquet.RowGroups.Add(new List<SyntheticParquet.Chunk>() { PathChunk(SyntheticParquet.DataPage(10, 8)), PathChunk(SyntheticParquet.DataPage(10, 8)) });
            Assert.Contains("appears twice", Declines(WriteSynthetic(parquet)).Message);
        }

        [Fact]
        public void RootWithoutAProjectedLeafDeclines()
        {
            var parquet = new SyntheticParquet();
            parquet.Schema.Add(new SyntheticParquet.Element("schema", Repetition: 0, Children: 1));
            parquet.Schema.Add(new SyntheticParquet.Element("commitInfo", Children: 1));
            parquet.Schema.Add(new SyntheticParquet.Element("timestamp", Type: 2));
            parquet.RowGroups.Add(new List<SyntheticParquet.Chunk>() { new SyntheticParquet.Chunk() { Path = new[] { "commitInfo", "timestamp" }, Type = 2, NumValues = 1, Pages = { SyntheticParquet.DataPage(1, 8) } } });
            Assert.Contains("No leaf of the file is read", Declines(WriteSynthetic(parquet)).Message);
        }

        [Fact]
        public void RowGroupWithoutItsColumnListDeclines()
        {
            var parquet = OneLeaf(PathChunk(SyntheticParquet.DataPage(10, 8)));
            parquet.OmitColumns = true;
            Assert.Contains("has no column chunks", Declines(WriteSynthetic(parquet)).Message);
        }

        private static SyntheticParquet OneLeaf(params SyntheticParquet.Chunk[] chunks)
        {
            var parquet = new SyntheticParquet();
            parquet.Schema.Add(new SyntheticParquet.Element("schema", Repetition: 0, Children: 1));
            parquet.Schema.Add(new SyntheticParquet.Element("add", Children: 1));
            parquet.Schema.Add(new SyntheticParquet.Element("path", Type: 6));
            foreach (var chunk in chunks)
            {
                parquet.RowGroups.Add(new List<SyntheticParquet.Chunk>() { chunk });
            }
            return parquet;
        }

        private static SyntheticParquet.Chunk PathChunk(params byte[][] pages) => new SyntheticParquet.Chunk() { Path = new[] { "add", "path" }, NumValues = 10, Pages = pages.ToList() };

        [Fact]
        public void SyntheticLeafIsAdmittedWithItsExactDecodeBound()
        {
            var page = SyntheticParquet.DataPage(10, 1000);
            var header = SyntheticParquet.HeaderLength(page, 1000);
            var result = CheckpointPageScan.Scan(WriteSynthetic(OneLeaf(PathChunk(page))), new JobMemory());

            // Uncompressed: the stream buffer is capped by the chunk, the body is the page buffer, no separate compressed body
            var stream = page.Length;
            var expected = stream + 1000 + 2 * header + NativeConstants.CodecPerLeaf + Math.Max(stream, 1000) + NativeConstants.ReaderFixed;
            Assert.Equal(expected, result.DecodeBound);
        }

        [Theory]
        [InlineData(3)]
        [InlineData(4)]
        [InlineData(9)]
        public void LzoBrotliAndUnknownCodecsDecline(int codec)
        {
            var chunk = PathChunk(SyntheticParquet.DataPage(10, 100));
            chunk = new SyntheticParquet.Chunk() { Path = chunk.Path, NumValues = 10, Pages = chunk.Pages, Codec = codec };
            Assert.Contains("codec", Declines(WriteSynthetic(OneLeaf(chunk))).Message);
        }

        [Fact]
        public void ValueOverPMaxDeclines()
        {
            var path = WriteSynthetic(OneLeaf(PathChunk(SyntheticParquet.DataPage(10, 5000))));
            Declines(path, new ScanLimits() { PageMax = 4096 });
        }

        [Fact]
        public void ForgedHugePageHeaderDeclines()
        {
            // A header claiming a body far past the chunk
            var header = SyntheticParquet.Header(0, int.MaxValue / 2, 100, h => h.I32(1, 10).I32(2, 0).I32(3, 3).I32(4, 3), 5);
            Declines(WriteSynthetic(OneLeaf(PathChunk(header))));
        }

        [Fact]
        public void HeaderOverHMaxDeclines()
        {
            var statistics = new byte[40_000];
            var page = SyntheticParquet.Page(0, 10, 10, h => h.I32(1, 10).I32(2, 0).I32(3, 3).I32(4, 3).BeginStruct(5).Binary(5, statistics).End(), 5);
            Assert.Contains("page header", Declines(WriteSynthetic(OneLeaf(PathChunk(page)))).Message);
        }

        [Fact]
        public void PageSizesThatDisagreeWithTheFooterDecline()
        {
            var compressed = PathChunk(SyntheticParquet.DataPage(10, 100));
            compressed.TotalCompressed = 90;
            Declines(WriteSynthetic(OneLeaf(compressed)));

            var uncompressed = PathChunk(SyntheticParquet.DataPage(10, 100));
            uncompressed.TotalUncompressed = 1000;
            Declines(WriteSynthetic(OneLeaf(uncompressed)));

            var values = new SyntheticParquet.Chunk() { Path = new[] { "add", "path" }, NumValues = 11, Pages = { SyntheticParquet.DataPage(10, 100) } };
            Assert.Contains("value counts", Declines(WriteSynthetic(OneLeaf(values))).Message);
        }

        [Fact]
        public void ChunkOutsideTheDataRegionDeclines()
        {
            var chunk = PathChunk(SyntheticParquet.DataPage(10, 100));
            chunk.DataPageOffset = 2;
            Assert.Contains("outside", Declines(WriteSynthetic(OneLeaf(chunk))).Message);
        }

        [Fact]
        public void ChunkWhoseExtentOverflowsDeclines()
        {
            // start + total_compressed_size wraps below the data end
            var chunk = PathChunk(SyntheticParquet.DataPage(10, 100));
            chunk.DataPageOffset = long.MaxValue - 10;
            Assert.Contains("outside", Declines(WriteSynthetic(OneLeaf(chunk))).Message);
        }

        [Fact]
        public void DeeplyNestedSchemaDeclines()
        {
            // The projection walks the schema recursively, so its depth must be bounded like the Thrift nesting
            var parquet = new SyntheticParquet();
            parquet.Schema.Add(new SyntheticParquet.Element("schema", Repetition: 0, Children: 1));
            for (int d = 0; d < 100; d++)
            {
                parquet.Schema.Add(new SyntheticParquet.Element($"g{d}", Children: 1));
            }
            parquet.Schema.Add(new SyntheticParquet.Element("leaf", Type: 6));
            parquet.RowGroups.Add(new List<SyntheticParquet.Chunk>() { new SyntheticParquet.Chunk() { Path = new[] { "leaf" }, NumValues = 10, Pages = { SyntheticParquet.DataPage(10, 100) } } });
            Declines(WriteSynthetic(parquet));
        }

        [Fact]
        public void OverlappingChunksDecline()
        {
            var parquet = new SyntheticParquet();
            parquet.Schema.Add(new SyntheticParquet.Element("schema", Repetition: 0, Children: 1));
            parquet.Schema.Add(new SyntheticParquet.Element("add", Children: 2));
            parquet.Schema.Add(new SyntheticParquet.Element("path", Type: 6));
            parquet.Schema.Add(new SyntheticParquet.Element("stats", Type: 6));
            var page = SyntheticParquet.DataPage(10, 100);
            var first = PathChunk(page);
            var second = new SyntheticParquet.Chunk() { Path = new[] { "add", "stats" }, NumValues = 10, Pages = { page }, DataPageOffset = 4 };
            parquet.RowGroups.Add(new List<SyntheticParquet.Chunk>() { first, second });
            Assert.Contains("overlap", Declines(WriteSynthetic(parquet)).Message);
        }

        [Fact]
        public void Int64DictionaryCardinalityIsChecked()
        {
            var parquet = new SyntheticParquet();
            parquet.Schema.Add(new SyntheticParquet.Element("schema", Repetition: 0, Children: 1));
            parquet.Schema.Add(new SyntheticParquet.Element("add", Children: 1));
            parquet.Schema.Add(new SyntheticParquet.Element("size", Type: 2));
            // 100 int64 entries cannot fit 400 bytes
            var dictionary = SyntheticParquet.Page(2, 400, 400, h => h.I32(1, 100).I32(2, 0), 7);
            var data = SyntheticParquet.DataPage(10, 50, encoding: 8);
            parquet.RowGroups.Add(new List<SyntheticParquet.Chunk>() { new SyntheticParquet.Chunk() { Path = new[] { "add", "size" }, Type = 2, NumValues = 10, Pages = { dictionary, data }, Encodings = new[] { 0, 3, 8 } } });
            Assert.Contains("dictionary", Declines(WriteSynthetic(parquet)).Message);
        }

        [Fact]
        public void DictionaryDescriptorsAndOffsetsAreCharged()
        {
            var dictionary = SyntheticParquet.Page(2, 400, 400, h => h.I32(1, 50).I32(2, 0), 7);
            var data = SyntheticParquet.DataPage(10, 50, encoding: 8);
            var chunk = new SyntheticParquet.Chunk() { Path = new[] { "add", "path" }, NumValues = 10, Pages = { dictionary, data }, Encodings = new[] { 0, 3, 8 } };
            var result = CheckpointPageScan.Scan(WriteSynthetic(OneLeaf(chunk)), new JobMemory());

            var stream = dictionary.Length + data.Length;
            var header = Math.Max(SyntheticParquet.HeaderLength(dictionary, 400), SyntheticParquet.HeaderLength(data, 50));
            var dictionaryTerm = 16 * 50 + 400 + 4 * 51 + 4 * 1024;
            var expected = stream + 400 + dictionaryTerm + 2 * header + NativeConstants.CodecPerLeaf + Math.Max(stream, Math.Max(400, dictionaryTerm)) + NativeConstants.ReaderFixed;
            Assert.Equal(expected, result.DecodeBound);
        }

        [Fact]
        public void V2NullsOverValuesDecline()
        {
            var page = SyntheticParquet.Page(3, 100, 100, h => h.I32(1, 10).I32(2, 11).I32(3, 10).I32(4, 0).I32(5, 2).I32(6, 0), 8);
            Assert.Contains("V2", Declines(WriteSynthetic(OneLeaf(PathChunk(page)))).Message);
        }

        [Fact]
        public void FooterOverFMaxDeclines()
        {
            var path = WriteSynthetic(OneLeaf(PathChunk(SyntheticParquet.DataPage(10, 100))));
            Assert.Contains("footer", Declines(path, new ScanLimits() { FooterMax = 16 }).Message);
        }

        [Fact]
        public void TooManyRowGroupsDecline()
        {
            var chunks = Enumerable.Range(0, ParquetFooterModel.MaxRowGroups + 1).Select(_ => PathChunk(SyntheticParquet.DataPage(10, 1))).ToArray();
            Assert.Contains("row groups", Declines(WriteSynthetic(OneLeaf(chunks))).Message);
        }

        [Fact]
        public void ExternalColumnFileDeclines()
        {
            var chunk = new SyntheticParquet.Chunk() { Path = new[] { "add", "path" }, NumValues = 10, Pages = { SyntheticParquet.DataPage(10, 100) }, FilePath = "other.parquet" };
            Assert.Contains("another file", Declines(WriteSynthetic(OneLeaf(chunk))).Message);
        }

        [Fact]
        public void EncryptedMagicDeclines()
        {
            var parquet = OneLeaf(PathChunk(SyntheticParquet.DataPage(10, 100)));
            parquet.Encrypted = true;
            Assert.Contains("encrypted", Declines(WriteSynthetic(parquet)).Message);
        }

        [Fact]
        public void MalformedThriftDeclines()
        {
            var valid = OneLeaf(PathChunk(SyntheticParquet.DataPage(10, 100))).Build();
            var footerLength = BitConverter.ToInt32(valid, valid.Length - 8);
            var footerStart = valid.Length - 8 - footerLength;

            // EOF mid-header: the footer claims fewer bytes than it holds
            var truncated = valid.Take(footerStart).Concat(valid.Skip(footerStart).Take(footerLength / 2)).Concat(BitConverter.GetBytes(footerLength / 2)).Concat("PAR1"u8.ToArray()).ToArray();
            var path = Path.Combine(_directory, "truncated.parquet");
            File.WriteAllBytes(path, truncated);
            Declines(path);

            // An invalid field type as the first field
            var invalid = (byte[])valid.Clone();
            invalid[footerStart] = (byte)((invalid[footerStart] & 0xF0) | 0x0E);
            path = Path.Combine(_directory, "invalid.parquet");
            File.WriteAllBytes(path, invalid);
            Declines(path);

            // A string whose length runs past the footer
            var w = new ThriftWriter().BeginElement().I32(1, 1).Field(6, 8).Varint(1L << 40).End();
            Declines(WriteFooter(w.ToArray()));

            // Nesting deeper than the limit, in a field nobody reads
            var deep = new ThriftWriter().BeginElement().I32(1, 1);
            for (int i = 0; i < 40; i++)
            {
                deep.BeginStruct(20);
            }
            for (int i = 0; i < 40; i++)
            {
                deep.End();
            }
            Assert.Contains("nesting", Declines(WriteFooter(deep.End().ToArray())).Message);
        }

        private string WriteFooter(byte[] footer)
        {
            var path = Path.Combine(_directory, Guid.NewGuid().ToString("N") + ".parquet");
            File.WriteAllBytes(path, "PAR1"u8.ToArray().Concat(footer).Concat(BitConverter.GetBytes(footer.Length)).Concat("PAR1"u8.ToArray()).ToArray());
            return path;
        }

        [Fact]
        public void UnprojectedFooterMetadataIsCharged()
        {
            SyntheticParquet WithUnprojected(int statisticsBytes)
            {
                var parquet = new SyntheticParquet();
                parquet.Schema.Add(new SyntheticParquet.Element("schema", Repetition: 0, Children: 2));
                parquet.Schema.Add(new SyntheticParquet.Element("add", Children: 1));
                parquet.Schema.Add(new SyntheticParquet.Element("path", Type: 6));
                parquet.Schema.Add(new SyntheticParquet.Element("commitInfo", Children: 1));
                parquet.Schema.Add(new SyntheticParquet.Element("operation", Type: 6));
                parquet.RowGroups.Add(new List<SyntheticParquet.Chunk>()
                {
                    PathChunk(SyntheticParquet.DataPage(10, 100)),
                    new SyntheticParquet.Chunk() { Path = new[] { "commitInfo", "operation" }, NumValues = 10, Pages = { SyntheticParquet.DataPage(10, 100) }, StatisticsMax = new byte[statisticsBytes] }
                });
                return parquet;
            }
            var small = CheckpointPageScan.Scan(WriteSynthetic(WithUnprojected(10)), new JobMemory());
            var large = CheckpointPageScan.Scan(WriteSynthetic(WithUnprojected(100_010)), new JobMemory());
            // The footer grows by the bytes, and the native copy of every string counts twice
            var growth = 100_000 + NativeConstants.StringFactor * 100_000;
            Assert.InRange(large.MetadataBound - small.MetadataBound, growth, growth + 64);
        }

        [Fact]
        public void RawFooterIsReleasedWhenParsingEnds()
        {
            var memory = new JobMemory();
            var result = CheckpointPageScan.Scan(WriteOwn(RealisticRecords(1_000)), memory);
            Assert.Equal(result.ModelBytes, memory.Managed);
            Assert.True(memory.ManagedPeak > result.ModelBytes);
        }

        [Fact]
        public void ReaderBufferStaysAboveTheFirstHeaderPeek()
        {
            var path = WriteOwn(RealisticRecords(10));
            Assert.Throws<InvalidOperationException>(() => CheckpointPageScan.Scan(path, new JobMemory(), new ScanLimits() { BufferSize = 16 << 10 }));
            // The reader guards its own buffer too, whatever the scan admitted
            var memory = new JobMemory();
            var scan = CheckpointPageScan.Scan(path, memory);
            Assert.Throws<InvalidOperationException>(() => new BoundedCheckpointReader(path, scan, memory, bufferSize: 16 << 10));
            Assert.Equal(0, memory.Native);
        }

        [Fact]
        public void StreamSimulationFollowsThePinnedGrowthRule()
        {
            // Each header peek that runs past the buffer's end grows it to the peek's end, so small pages grow it toward the whole chunk
            var small = new StreamReplay(1_000_000, 64 << 10);
            for (int i = 0; i < 1000; i++)
            {
                small.Page(20, 980);
            }
            Assert.InRange(small.Peak, 900_000, 1_000_000);

            // A short chunk caps the buffer at its length
            Assert.Equal(5000, new StreamReplay(5000, 64 << 10).Peak);

            // A header longer than the first peek doubles the peek and grows the buffer to the bytes already read past its start
            var large = new StreamReplay(10_000_000, 64 << 10);
            large.Page(20, 30_000);
            large.Page(100_000, 10);
            Assert.True(large.Peak > 100_000 && large.Peak <= 64 << 10 << 2, $"peak {large.Peak}");

            // Bodies above the buffer size are read around it
            var bodies = new StreamReplay(10_000_000, 64 << 10);
            bodies.Page(20, 200_000);
            bodies.Page(20, 200_000);
            Assert.Equal(64 << 10, bodies.Peak);
        }

        [Fact]
        public void StreamSimulatorFlagsTheGh48311Pattern()
        {
            // With a 16 KiB buffer emptied at position 16 KiB, a 16 KiB peek with 20 KiB left in the chunk grows the buffer only to 20 KiB
            var replay = new StreamReplay(36 << 10, 16 << 10);
            replay.Page(20, (16 << 10) - 20);
            var declined = Assert.Throws<CheckpointDeclinedException>(() => replay.Page(20, 1000));
            Assert.Contains("GH-48311", declined.Message);

            // The same sequence with the 64 KiB buffer stays clear
            var safe = new StreamReplay(36 << 10, 64 << 10);
            safe.Page(20, (16 << 10) - 20);
            safe.Page(20, 1000);
        }

        [Fact]
        public void UnrelatedPageSubheaderCannotOverrideTheSelectedEncoding()
        {
            var path = Path.Combine(AppContext.BaseDirectory, $"admission-{Guid.NewGuid():N}.parquet");
            try
            {
                var header = new ThriftWriter().BeginElement().I32(1, 0).I32(2, 8).I32(3, 8);
                header.BeginStruct(5).I32(1, 1).I32(2, 7).I32(3, 3).I32(4, 3).End();
                header.BeginStruct(7).I32(1, 1).I32(2, 0).End().End();
                var parquet = new SyntheticParquet();
                parquet.Schema.Add(new SyntheticParquet.Element("schema", Repetition: 0, Children: 1));
                parquet.Schema.Add(new SyntheticParquet.Element("add", Children: 1));
                parquet.Schema.Add(new SyntheticParquet.Element("path", Type: 6));
                parquet.RowGroups.Add(new List<SyntheticParquet.Chunk>
                {
                    new() { Path = new[] { "add", "path" }, NumValues = 1, Pages = { header.ToArray().Concat(new byte[8]).ToArray() } }
                });
                File.WriteAllBytes(path, parquet.Build());
                Assert.Throws<CheckpointDeclinedException>(() => CheckpointPageScan.Scan(path, new JobMemory()));
            }
            finally
            {
                File.Delete(path);
            }
        }

        [Fact]
        public void KeptMarkerIsChargedByItsLength()
        {
            static long Charged(int length)
            {
                var path = Path.Combine(AppContext.BaseDirectory, $"marker-charge-{Guid.NewGuid():N}.parquet");
                try
                {
                    var parquet = new SyntheticParquet();
                    parquet.Schema.Add(new SyntheticParquet.Element("schema", Repetition: 0, Children: 1));
                    parquet.Schema.Add(new SyntheticParquet.Element("add", Children: 1));
                    parquet.Schema.Add(new SyntheticParquet.Element("path", Type: 6));
                    parquet.RowGroups.Add(new List<SyntheticParquet.Chunk>
                    {
                        new() { Path = new[] { "add", "path" }, NumValues = 1, Pages = { SyntheticParquet.DataPage(1, 8) } }
                    });
                    parquet.KeyValues.Add((ParquetFooterModel.MarkerKey, new string('x', length)));
                    File.WriteAllBytes(path, parquet.Build());
                    var memory = new JobMemory();
                    var scan = CheckpointPageScan.Scan(path, memory);
                    Assert.NotNull(scan.Footer.Marker);
                    return memory.Managed;
                }
                finally
                {
                    File.Delete(path);
                }
            }
            // The base and projection charges alone exceed a 4,000 byte marker's string, so compare against a one byte marker
            var extra = Charged(4000) - Charged(1);
            Assert.True(extra >= 2L * 3999, $"A 4,000 byte marker adds {extra} bytes of charge");
        }

        [Theory]
        [InlineData(4000, true)]
        [InlineData(1 << 20, false)]
        public void RetainedFooterMarkerIsIncludedInManagedCharge(int length, bool kept)
        {
            var path = Path.Combine(AppContext.BaseDirectory, $"model-{Guid.NewGuid():N}.parquet");
            try
            {
                var parquet = new SyntheticParquet();
                parquet.Schema.Add(new SyntheticParquet.Element("schema", Repetition: 0, Children: 1));
                parquet.Schema.Add(new SyntheticParquet.Element("add", Children: 1));
                parquet.Schema.Add(new SyntheticParquet.Element("path", Type: 6));
                parquet.RowGroups.Add(new List<SyntheticParquet.Chunk>
                {
                    new() { Path = new[] { "add", "path" }, NumValues = 1, Pages = { SyntheticParquet.DataPage(1, 8) } }
                });
                parquet.KeyValues.Add((ParquetFooterModel.MarkerKey, new string('x', length)));
                File.WriteAllBytes(path, parquet.Build());
                var memory = new JobMemory();
                var scan = CheckpointPageScan.Scan(path, memory);
                if (kept)
                {
                    Assert.NotNull(scan.Footer.Marker);
                    Assert.True(memory.Managed >= 2L * scan.Footer.Marker.Length,
                        $"Retained marker alone occupies {2L * scan.Footer.Marker.Length} payload bytes, but the complete model is charged {memory.Managed}.");
                }
                else
                {
                    // A marker too long to parse is unmarked without building its string
                    Assert.Null(scan.Footer.Marker);
                    Assert.True(scan.ModelBytes < length, $"A model of {scan.ModelBytes} bytes for a {length} byte marker");
                }
                Assert.Equal(scan.ModelBytes, memory.Managed);
            }
            finally
            {
                File.Delete(path);
            }
        }

        [Fact]
        public void FooterChargeCoversTheRetainedChunksAndEncodingArrays()
        {
            const int leafCount = 16;
            // Two files that differ only in their number of row groups, so the difference is charged per chunk and per group
            var small = ModelBytesOf(leafCount, 500);
            var large = ModelBytesOf(leafCount, 1000);

            var samples = new FooterChunk[1000];
            var before = GC.GetAllocatedBytesForCurrentThread();
            for (int i = 0; i < samples.Length; i++)
            {
                samples[i] = new FooterChunk { Encodings = new int[2] };
            }
            var bytesPerChunk = (GC.GetAllocatedBytesForCurrentThread() - before) / samples.Length;
            GC.KeepAlive(samples);
            var chargedPerChunk = (large - small) / (leafCount * 500);
            Assert.True(chargedPerChunk >= bytesPerChunk,
                $"Each further chunk is charged {chargedPerChunk} bytes with its share of the group, but a retained chunk and its encoding array occupy {bytesPerChunk}");
        }

        private static long ModelBytesOf(int leafCount, int groups)
        {
            var path = Path.Combine(AppContext.BaseDirectory, "footer-charge-" + Guid.NewGuid().ToString("N") + ".parquet");
            try
            {
                var parquet = new SyntheticParquet();
                parquet.Schema.Add(new SyntheticParquet.Element("schema", Repetition: 0, Children: leafCount));
                for (int leaf = 0; leaf < leafCount; leaf++)
                {
                    parquet.Schema.Add(new SyntheticParquet.Element("unknown" + leaf, Type: 6));
                }
                for (int group = 0; group < groups; group++)
                {
                    var chunks = new List<SyntheticParquet.Chunk>();
                    for (int leaf = 0; leaf < leafCount; leaf++)
                    {
                        chunks.Add(new SyntheticParquet.Chunk
                        {
                            Path = new[] { "unknown" + leaf }, NumValues = 1,
                            Pages = { SyntheticParquet.DataPage(1, 6) }
                        });
                    }
                    parquet.RowGroups.Add(chunks);
                }
                File.WriteAllBytes(path, parquet.Build());
                return CheckpointPageScan.Scan(path, new JobMemory()).ModelBytes;
            }
            finally
            {
                File.Delete(path);
            }
        }

        [Fact]
        public void SetTypedSchemaMustBeDeclinedBeforeNativeOpen()
        {
            var path = Path.Combine(AppContext.BaseDirectory, "set-" + Guid.NewGuid().ToString("N") + ".parquet");
            try
            {
                var parquet = new SyntheticParquet();
                parquet.Schema.Add(new SyntheticParquet.Element("schema", Repetition: 0, Children: 1));
                parquet.Schema.Add(new SyntheticParquet.Element("add", Children: 1));
                parquet.Schema.Add(new SyntheticParquet.Element("path", Type: 6));
                parquet.RowGroups.Add(new List<SyntheticParquet.Chunk>
                {
                    new() { Path = new[] { "add", "path" }, NumValues = 1, Pages = { V2(new byte[] { 2, 2, 1, 0, 0, 0, (byte)'a' }, 0, 1) } }
                });
                var bytes = parquet.Build();
                var footerStart = bytes.Length - 8 - BitConverter.ToInt32(bytes, bytes.Length - 8);
                Assert.Equal(0x19, bytes[footerStart + 2]);
                bytes[footerStart + 2] = 0x1a;
                File.WriteAllBytes(path, bytes);

                using (var native = new ParquetSharp.ParquetFileReader(path))
                {
                    Assert.Fail("Expected the native reader to reject the set-typed required schema");
                }
            }
            catch (ParquetSharp.ParquetException)
            {
                var error = Record.Exception(() => CheckpointPageScan.Scan(path, new JobMemory()));
                Assert.True(error is CheckpointDeclinedException,
                    $"Native reader rejected the set-typed schema but managed admission returned {error?.ToString() ?? "success"}");
            }
            finally
            {
                File.Delete(path);
            }
        }

        [Theory]
        [InlineData(false)]
        [InlineData(true)]
        public void V2CountsAreValidatedEvenWhenChunkTotalsAgree(bool mixedVersions)
        {
            var path = Path.Combine(AppContext.BaseDirectory, "v2-" + Guid.NewGuid().ToString("N") + ".parquet");
            try
            {
                var parquet = new SyntheticParquet { Rows = 2 };
                parquet.Schema.Add(new SyntheticParquet.Element("schema", Repetition: 0, Children: 1));
                parquet.Schema.Add(new SyntheticParquet.Element("add", Children: 1));
                parquet.Schema.Add(new SyntheticParquet.Element("path", Type: 6));
                byte[] definedBody = { 2, 2, 1, 0, 0, 0, (byte)'a' };
                byte[] nullBody = { 2, 0 };
                byte[] first;
                byte[] second;
                if (mixedVersions)
                {
                    byte[] v1Body = { 2, 0, 0, 0, 2, 2, 1, 0, 0, 0, (byte)'a' };
                    first = SyntheticParquet.DataPage(1, v1Body.Length);
                    v1Body.CopyTo(first, first.Length - v1Body.Length);
                    second = V2(definedBody, nulls: 0, rows: 0);
                }
                else
                {
                    first = V2(definedBody, nulls: 1, rows: 1);
                    second = V2(nullBody, nulls: 0, rows: 1);
                }
                parquet.RowGroups.Add(new List<SyntheticParquet.Chunk>
                {
                    new() { Path = new[] { "add", "path" }, NumValues = 2, Pages = { first, second } }
                });
                File.WriteAllBytes(path, parquet.Build());
                var memory = new JobMemory();
                var emitted = new List<byte[]>();
                var error = Record.Exception(() =>
                {
                    var scan = CheckpointPageScan.Scan(path, memory);
                    using var reader = new BoundedCheckpointReader(path, scan, memory);
                    reader.Read(record => emitted.Add(record.ToArray()));
                });
                Assert.True(error is CheckpointDeclinedException,
                    $"Accepted invalid per-page V2 counts, mixed={mixedVersions}, emitted={emitted.Count}, error={error}");
            }
            finally
            {
                File.Delete(path);
            }
        }

        private static byte[] V2(byte[] body, int nulls, int rows)
        {
            var page = SyntheticParquet.Page(3, body.Length, body.Length,
                h => h.I32(1, 1).I32(2, nulls).I32(3, rows).I32(4, 0).I32(5, 2).I32(6, 0).Bool(7, false), 8);
            body.CopyTo(page, page.Length - body.Length);
            return page;
        }
    }
}
