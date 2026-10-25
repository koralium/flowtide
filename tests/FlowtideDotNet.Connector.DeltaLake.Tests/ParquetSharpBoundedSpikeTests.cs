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

using Apache.Arrow;
using Apache.Arrow.Types;
using FlowtideDotNet.Connector.DeltaLake.Internal.Delta;
using ParquetSharp;
using ParquetSharp.Arrow;
using Stowage;
using Stowage.Impl;
using System.Diagnostics;
using System.Runtime.InteropServices;
using Xunit.Abstractions;

namespace FlowtideDotNet.Connector.DeltaLake.Tests
{
    // Alone, so its process-wide thread count is not moved by other classes
    [CollectionDefinition(nameof(NativeThreadCountCollection), DisableParallelization = true)]
    public class NativeThreadCountCollection
    {
    }

    [Collection(nameof(NativeThreadCountCollection))]
    public class ParquetSharpBoundedSpikeTests : IDisposable
    {
        private readonly ITestOutputHelper _output;
        private readonly string _directory;

        public ParquetSharpBoundedSpikeTests(ITestOutputHelper output)
        {
            _output = output;
            _directory = Path.Combine(Path.GetTempPath(), "flowtide-spike-" + Guid.NewGuid().ToString("N"));
            Directory.CreateDirectory(_directory);
        }

        public void Dispose()
        {
            Directory.Delete(_directory, true);
        }

        private static string Value(int i) => $"value-{i:D6}-{new string((char)('a' + i % 26), 24)}";

        private static byte[] Copy(ByteArray value)
        {
            var bytes = new byte[value.Length];
            Marshal.Copy(value.Pointer, bytes, 0, value.Length);
            return bytes;
        }

        // Small pages of compressed strings, so the decompression buffer is reused page after page
        private string WriteStrings(int count, Action<WriterPropertiesBuilder>? configure = null)
        {
            var path = Path.Combine(_directory, Guid.NewGuid().ToString("N") + ".parquet");
            using var builder = new WriterPropertiesBuilder();
            builder.Compression(Compression.Zstd).DisableDictionary().DataPagesize(1024).WriteBatchSize(32);
            configure?.Invoke(builder);
            using var properties = builder.Build();
            using (var file = new ParquetFileWriter(path, new ParquetSharp.Column[] { new ParquetSharp.Column<string>("s") }, properties))
            {
                using (var rowGroup = file.AppendRowGroup())
                using (var writer = rowGroup.NextColumn().LogicalWriter<string>())
                {
                    writer.WriteBatch(Enumerable.Range(0, count).Select(Value).ToArray());
                }
                file.Close();
            }
            return path;
        }

        private static ReaderProperties BufferedProperties()
        {
            var properties = ReaderProperties.GetDefaultReaderProperties();
            properties.EnableBufferedStream();
            properties.BufferSize = 64 * 1024;
            return properties;
        }

        [Fact]
        public void ByteArrayViewChangesAfterTheReaderCrossesAPage()
        {
            var path = WriteStrings(2000);
            using var properties = BufferedProperties();
            using var file = new ParquetFileReader(path, properties);
            using var rowGroup = file.RowGroup(0);
            using var column = (ColumnReader<ByteArray>)rowGroup.Column(0);
            var def = new short[256];
            var rep = new short[256];
            var values = new ByteArray[256];

            var levels = column.ReadBatch(4, def, rep, values, out var valuesRead);
            Assert.Equal(4, levels);
            Assert.Equal(4, valuesRead);
            var view = values[0];
            var copied = Copy(view);
            Assert.Equal(Value(0), System.Text.Encoding.UTF8.GetString(copied));

            var read = 4L;
            while (column.HasNext)
            {
                read += column.ReadBatch(256, def, rep, values, out _);
            }
            Assert.Equal(2000, read);
            // The copy keeps the value, the view now shows a later page's bytes
            Assert.NotEqual(copied, Copy(view));
        }

        [Fact]
        public void LocalDiskImplementsTheLocalDiskInterface()
        {
            var storage = Files.Of.LocalDisk(_directory);
            var local = Assert.IsAssignableFrom<ILocalDiskFileStorage>(storage);
            Assert.Equal(Path.Combine(_directory, "a.txt"), local.ToNativeLocalPath("/a.txt"));
        }

        // The Windows build has only the system pool, so pool figures there are process-wide and measured in fresh processes; the Linux build also has mimalloc
        [Fact]
        public void WriterRoutesToTheSystemPool()
        {
            if (OperatingSystem.IsWindows())
            {
                Assert.Throws<ParquetException>(() => MemoryPool.MimallocMemoryPool());
            }
            string jemalloc;
            try
            {
                jemalloc = MemoryPool.JemallocMemoryPool().BackendName;
            }
            catch (ParquetException e)
            {
                jemalloc = e.Message;
            }
            var pool = MemoryPool.SystemMemoryPool();
            WriteStrings(20_000, builder => builder.MemoryPool(pool));
            var defaultPool = MemoryPool.GetDefaultMemoryPool();
            _output.WriteLine($"default {defaultPool.BackendName} max {defaultPool.MaxMemory}, system {pool.BackendName}, jemalloc: {jemalloc}; max {pool.MaxMemory}");

            Assert.Equal("system", pool.BackendName);
            Assert.True(pool.MaxMemory > 0);
        }

        private static Apache.Arrow.Schema MarkedSchema() => new Apache.Arrow.Schema.Builder()
            .Field(f => f.Name("a").DataType(Int64Type.Default).Nullable(true))
            .Metadata("flowtide.checkpoint", "{\"format\":1}")
            .Build();

        private string WriteArrow(Action<ArrowWriterPropertiesBuilder> arrow, Action<WriterPropertiesBuilder>? parquet = null)
        {
            var path = Path.Combine(_directory, Guid.NewGuid().ToString("N") + ".parquet");
            var schema = MarkedSchema();
            using var arrowBuilder = new ArrowWriterPropertiesBuilder();
            arrowBuilder.UseThreads(false);
            arrow(arrowBuilder);
            using var arrowProperties = arrowBuilder.Build();
            using var parquetBuilder = new WriterPropertiesBuilder();
            parquet?.Invoke(parquetBuilder);
            using var properties = parquetBuilder.Build();
            using var batch = new RecordBatch(schema, new IArrowArray[] { new Int64Array.Builder().AppendRange(Enumerable.Range(0, 5000).Select(x => (long)x)).Build() }, 5000);
            using (var stream = File.Create(path))
            using (var writer = new FileWriter(stream, schema, properties, arrowProperties))
            {
                writer.NewBufferedRowGroup();
                writer.WriteBufferedRecordBatch(batch);
                writer.Close();
            }
            return path;
        }

        [Fact]
        public void StoreSchemaWritesTheMarkerIntoTheFooter()
        {
            var stored = WriteArrow(b => b.StoreSchema());
            var plain = WriteArrow(_ => { });
            using var storedFile = new ParquetFileReader(stored);
            using var plainFile = new ParquetFileReader(plain);
            var storedKeys = storedFile.FileMetaData.KeyValueMetadata;
            var plainKeys = plainFile.FileMetaData.KeyValueMetadata;
            _output.WriteLine($"StoreSchema: {string.Join(", ", storedKeys.Keys)}; without: {string.Join(", ", plainKeys.Keys)}");

            Assert.Equal("{\"format\":1}", storedKeys["flowtide.checkpoint"]);
            Assert.Contains("ARROW:schema", storedKeys.Keys);
        }

        [Fact]
        public void DisabledPageIndexLeavesNothingBeforeTheFooter()
        {
            var indexed = WriteArrow(_ => { }, b => b.EnableWritePageIndex());
            var unindexed = WriteArrow(_ => { }, b => b.DisableWritePageIndex());
            _output.WriteLine($"page index on: {ParquetKit.BytesBeforeFooter(indexed)} bytes, off: {ParquetKit.BytesBeforeFooter(unindexed)} bytes");

            Assert.True(ParquetKit.BytesBeforeFooter(indexed) > 0);
            Assert.Equal(0, ParquetKit.BytesBeforeFooter(unindexed));
        }

        [Theory]
        [InlineData(false)]
        [InlineData(true)]
        public void DataPageV2FixturesRoundTrip(bool dictionary)
        {
            var path = WriteStrings(2000, b =>
            {
                b.DataPageVersion(ParquetDataPageVersion.V2);
                if (dictionary)
                {
                    b.EnableDictionary();
                }
            });
            using var file = new ParquetFileReader(path);
            using var rowGroup = file.RowGroup(0);
            var encodings = rowGroup.MetaData.GetColumnChunkMetaData(0).Encodings;
            _output.WriteLine($"encodings: {string.Join(", ", encodings)}");
            using var reader = rowGroup.Column(0).LogicalReader<string>();
            Assert.Equal(Enumerable.Range(0, 2000).Select(Value), reader.ReadAll(2000));
            Assert.Equal(dictionary, encodings.Contains(ParquetSharp.Encoding.RleDictionary));
        }

        [Fact]
        public void ColumnEncodingProducesDeltaFixtures()
        {
            var path = Path.Combine(_directory, "delta.parquet");
            using (var properties = DeltaTestKit.Properties(b => b.DisableDictionary().Encoding("v", ParquetSharp.Encoding.DeltaBinaryPacked)))
            using (var file = new ParquetFileWriter(path, new ParquetSharp.Column[] { new ParquetSharp.Column<long>("v") }, properties))
            {
                using (var rowGroup = file.AppendRowGroup())
                using (var writer = rowGroup.NextColumn().LogicalWriter<long>())
                {
                    writer.WriteBatch(Enumerable.Range(0, 1000).Select(x => (long)x * 3).ToArray());
                }
                file.Close();
            }
            using var reader = new ParquetFileReader(path);
            using var group = reader.RowGroup(0);
            Assert.Contains(ParquetSharp.Encoding.DeltaBinaryPacked, group.MetaData.GetColumnChunkMetaData(0).Encodings);
        }

        [Fact]
        public void WriterAndReaderStartNoNativeThreads()
        {
            var before = Process.GetCurrentProcess().Threads.Count;
            var path = WriteArrow(_ => { });
            using (var properties = BufferedProperties())
            using (var file = new ParquetFileReader(path, properties))
            using (var rowGroup = file.RowGroup(0))
            using (var column = (ColumnReader<long>)rowGroup.Column(0))
            {
                var def = new short[1024];
                var rep = new short[1024];
                var values = new long[1024];
                while (column.HasNext)
                {
                    column.ReadBatch(1024, def, rep, values, out _);
                }
            }
            var after = Process.GetCurrentProcess().Threads.Count;
            _output.WriteLine($"threads {before} -> {after}");
            // Arrow's CPU pool would add one thread per core
            Assert.True(after - before < 4, $"threads grew from {before} to {after}");
        }

        // Pre-sizing a builder fails: Reserve sizes offsets and validity only, the value bytes still grow by doubling
        [Fact]
        public void BuilderValueBytesCannotBePreSized()
        {
            const int rows = 4096;
            var value = System.Text.Encoding.UTF8.GetBytes(new string('x', 100));
            var builder = new StringArray.Builder();
            builder.Reserve(rows);

            var before = GC.GetAllocatedBytesForCurrentThread();
            for (int i = 0; i < rows; i++)
            {
                builder.Append((ReadOnlySpan<byte>)value);
            }
            var appended = GC.GetAllocatedBytesForCurrentThread() - before;
            before = GC.GetAllocatedBytesForCurrentThread();
            using var strings = builder.Build();
            var built = GC.GetAllocatedBytesForCurrentThread() - before;
            _output.WriteLine($"append allocated {appended} bytes for {rows * value.Length} value bytes, build allocated {built} bytes");

            Assert.True(appended > rows * value.Length);
        }

        // The fallback for builders that cannot be pre-sized: a batch's buffers assembled in memory the ledger already holds, wrapped without copying
        [Fact]
        public void ArraysOverPreparedBuffersAllocateOnlyTheirObjects()
        {
            const int rows = 4096;
            var data = new byte[rows * 100];
            var offsets = new byte[(rows + 1) * sizeof(int)];
            var validity = new byte[(rows + 7) / 8];
            var offsetValues = System.Runtime.InteropServices.MemoryMarshal.Cast<byte, int>(offsets.AsSpan());
            var nulls = 0;
            for (int i = 0; i < rows; i++)
            {
                var present = i % 7 != 0;
                offsetValues[i + 1] = offsetValues[i] + (present ? 100 : 0);
                if (present)
                {
                    validity[i / 8] |= (byte)(1 << (i % 8));
                }
                else
                {
                    nulls++;
                }
            }

            var before = GC.GetAllocatedBytesForCurrentThread();
            using var strings = new StringArray(rows, new ArrowBuffer(offsets), new ArrowBuffer(data), new ArrowBuffer(validity), nulls);
            var allocated = GC.GetAllocatedBytesForCurrentThread() - before;
            _output.WriteLine($"wrapping {rows} values allocated {allocated} bytes");

            Assert.True(allocated < 4096, $"{allocated} bytes");
            Assert.Equal(nulls, strings.NullCount);
            Assert.Equal(100, strings.GetBytes(1).Length);
        }

        [Fact]
        public async Task LegacyCheckpointHasNoArrowSchemaKey()
        {
            var storage = Files.Of.LocalDisk(_directory);
            await LegacyTableGenerator.Create(storage, "t", 10, 2, 0, false);
            using var file = new ParquetFileReader(Path.Combine(_directory, "t", "_delta_log", $"{0:D20}.checkpoint.parquet"));
            Assert.DoesNotContain("ARROW:schema", file.FileMetaData.KeyValueMetadata.Keys);
        }
    }
}
