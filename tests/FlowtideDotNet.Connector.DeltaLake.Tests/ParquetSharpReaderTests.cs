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
using FlowtideDotNet.Connector.DeltaLake.Internal.Delta.DeletionVectors;
using FlowtideDotNet.Connector.DeltaLake.Internal.Delta.ParquetFormat;
using FlowtideDotNet.Storage.Memory;
using Stowage;
using Apache.Arrow;
using Apache.Arrow.Types;
using System.Buffers;
using System.Collections.Concurrent;

namespace FlowtideDotNet.Connector.DeltaLake.Tests
{
    // Alone, so the process-wide Arrow pool is not moved by other classes
    [CollectionDefinition(nameof(ArrowPoolCollection), DisableParallelization = true)]
    public class ArrowPoolCollection
    {
    }

    [Collection(nameof(ArrowPoolCollection))]
    public class ParquetSharpReaderTests
    {
        [Fact]
        public async Task TestReadAddRemovedDataFile()
        {
            var storageLoc = Files.Of.LocalDisk("../../../testdata");
            var table = await DeltaTransactionReader.ReadTable(storageLoc, "/table_with_deletion_logs");
            Assert.NotNull(table);

            var reader = new ParquetSharpReader();
            reader.Initialize(table, new List<string> { "company", "age", "name" });

            var file = table.AddFiles[0];

            var changedIndices = new List<(long id, int weight)>
            {
                (5, 1),
                (12, -1)
            };

            var allocator = FlowtideDotNet.Storage.Memory.GlobalMemoryManager.Instance;
            var resultList = new List<BatchResultWithWeights>();
            await foreach (var result in reader.ReadAddRemovedDataFile(storageLoc, "/table_with_deletion_logs", file.Path!, changedIndices, file.PartitionValues, allocator))
            {
                resultList.Add(result);
            }

            Assert.Single(resultList);
            var batch = resultList[0];
            Assert.Equal(2, batch.count);
            Assert.Equal(1, batch.weights[0]);
            Assert.Equal(-1, batch.weights[1]);

            Assert.Equal(3, batch.data.Columns.Count);

            batch.data.Dispose();
            batch.weights.Dispose();
        }

        [Fact]
        public async Task TestReadAddRemovedDataFile_IndexOutOfBounds()
        {
            var storageLoc = Files.Of.LocalDisk("../../../testdata");
            var table = await DeltaTransactionReader.ReadTable(storageLoc, "/table_with_deletion_logs");
            Assert.NotNull(table);

            var reader = new ParquetSharpReader();
            reader.Initialize(table, new List<string> { "company", "age", "name" });

            var file = table.AddFiles[0];

            var changedIndices = new List<(long id, int weight)>
            {
                (100, 1)
            };

            var allocator = FlowtideDotNet.Storage.Memory.GlobalMemoryManager.Instance;
            var resultList = new List<BatchResultWithWeights>();
            await foreach (var result in reader.ReadAddRemovedDataFile(storageLoc, "/table_with_deletion_logs", file.Path!, changedIndices, file.PartitionValues, allocator))
            {
                resultList.Add(result);
            }

            Assert.Empty(resultList);
        }

        [Fact]
        public async Task TestReadAddRemovedDataFile_MultiBatch()
        {
            var realStorage = Files.Of.LocalDisk("../../../testdata");
            var table = await DeltaTransactionReader.ReadTable(realStorage, "/table_with_deletion_logs");
            Assert.NotNull(table);

            var reader = new ParquetSharpReader();
            reader.Initialize(table, new List<string> { "company", "age", "name" });

            var arrowSchema = new Apache.Arrow.Schema.Builder()
                .Field(new Field("company", new StringType(), true))
                .Field(new Field("age", new DoubleType(), true))
                .Field(new Field("name", new StringType(), true))
                .Build();

            RecordBatch CreateBatch(int startIndex, int count)
            {
                var companyBuilder = new StringArray.Builder();
                var ageBuilder = new DoubleArray.Builder();
                var nameBuilder = new StringArray.Builder();

                for (int i = 0; i < count; i++)
                {
                    companyBuilder.Append($"company_{startIndex + i}");
                    ageBuilder.Append(startIndex + i);
                    nameBuilder.Append($"name_{startIndex + i}");
                }

                return new RecordBatch(
                    arrowSchema,
                    [
                        companyBuilder.Build(),
                        ageBuilder.Build(),
                        nameBuilder.Build()
                    ],
                    count);
            }

            var batch1 = CreateBatch(0, 30000);
            var batch2 = CreateBatch(30000, 30000);
            var batch3 = CreateBatch(60000, 30000);

            var storageLoc = Files.Of.InternalMemory("./test_multibatch");
            var filePath = "multi_batch.parquet";
            
            using (var stream = await storageLoc.OpenWrite(filePath))
            {
                using (var writer = new ParquetSharp.Arrow.FileWriter(stream, arrowSchema))
                {
                    writer.WriteRecordBatch(batch1);
                    writer.WriteRecordBatch(batch2);
                    writer.WriteRecordBatch(batch3);
                    writer.Close();
                }
            }

            var changedIndices = new List<(long id, int weight)>
            {
                (5000, 1),
                (65536, 1),
                (75000, -1)
            };

            var allocator = FlowtideDotNet.Storage.Memory.GlobalMemoryManager.Instance;
            var resultList = new List<BatchResultWithWeights>();
            await foreach (var result in reader.ReadAddRemovedDataFile(storageLoc, "/", filePath, changedIndices, null, allocator))
            {
                resultList.Add(result);
            }

            try
            {
                Assert.Equal(2, resultList.Count);

                Assert.Equal(1, resultList[0].count);
                Assert.Equal(1, resultList[0].weights[0]);
                var (array1, _) = resultList[0].data.Columns[0].ToArrowArray();
                var companyCol1 = (Apache.Arrow.StringArray)array1;
                Assert.Equal("company_5000", companyCol1.GetString(0));

                Assert.Equal(2, resultList[1].count);
                Assert.Equal(1, resultList[1].weights[0]);
                Assert.Equal(-1, resultList[1].weights[1]);

                var (array2, _) = resultList[1].data.Columns[0].ToArrowArray();
                var companyCol2 = (Apache.Arrow.StringArray)array2;
                Assert.Equal("company_65536", companyCol2.GetString(0));
                Assert.Equal("company_75000", companyCol2.GetString(1));
            }
            finally
            {
                foreach (var r in resultList)
                {
                    r.data.Dispose();
                    r.weights.Dispose();
                }
            }
        }

        [Fact]
        public async Task ReadsRunOnTheCallingThread()
        {
            // Reads from Arrow's own threads crashed the test host on Linux
            var schema = new Apache.Arrow.Schema.Builder()
                .Field(new Field("id", Int64Type.Default, true))
                .Field(new Field("name", StringType.Default, true))
                .Build();
            var ids = new Int64Array.Builder();
            var names = new StringArray.Builder();
            for (int i = 0; i < 1000; i++)
            {
                ids.Append(i);
                names.Append($"name_{i}");
            }
            var file = new MemoryStream();
            using (var writer = new ParquetSharp.Arrow.FileWriter(file, schema))
            {
                using var batch = new RecordBatch(schema, [ids.Build(), names.Build()], 1000);
                writer.WriteRecordBatch(batch);
                writer.Close();
            }

            var stream = new ThreadRecordingStream(file.ToArray());
            stream.Caller = Environment.CurrentManagedThreadId;
            using var reader = ParquetFileReaders.Open(stream);
            using var batches = reader.GetRecordBatchReader();
            long rows = 0;
            while (true)
            {
                stream.Caller = Environment.CurrentManagedThreadId;
                var read = await batches.ReadNextRecordBatchAsync();
                if (read == null)
                {
                    break;
                }
                rows += read.Length;
                read.Dispose();
            }

            Assert.Equal(1000, rows);
            Assert.True(stream.Reads > 0);
            Assert.Equal(0, stream.ForeignReads);
        }

        [Theory]
        [InlineData("complete")]
        [InlineData("break")]
        [InlineData("throw")]
        public async Task CdcReadDisposesItsStream(string end)
        {
            var testData = Files.Of.LocalDisk("../../../testdata");
            var table = await DeltaTransactionReader.ReadTable(testData, "/simple_table_with_cdc");
            Assert.NotNull(table);
            var commit = await DeltaTransactionReader.ReadVersionCommit(testData, "/simple_table_with_cdc", 2);
            Assert.NotNull(commit);
            var file = Assert.Single(commit.CdcFiles);

            var reader = new ParquetSharpReader();
            reader.Initialize(table, new List<string> { "id", "name" });
            var storage = new DisposeTrackingStorage(testData);

            var received = new List<CdcBatchResult>();
            try
            {
                await foreach (var batch in reader.ReadCdcFile(storage, "/simple_table_with_cdc", file.Path!, file.PartitionValues, FlowtideDotNet.Storage.Memory.GlobalMemoryManager.Instance))
                {
                    received.Add(batch);
                    if (end == "break")
                    {
                        break;
                    }
                    if (end == "throw")
                    {
                        throw new InvalidOperationException("consumer failed");
                    }
                }
            }
            catch (InvalidOperationException) when (end == "throw")
            {
            }
            finally
            {
                DisposeAll(received);
            }

            Assert.NotEmpty(received);
            Assert.True(Assert.Single(storage.Opened).Disposed);
        }

        [Fact]
        public async Task CdcReadDisposesItsStreamWhenALaterBatchFails()
        {
            // The bad row is past the first batch, so the read fails after a yield
            var changeTypes = Enumerable.Repeat("insert", 100_000).ToList();
            changeTypes[^1] = "bogus";
            var (reader, storage) = await SyntheticCdcFile(changeTypes);

            var received = new List<CdcBatchResult>();
            try
            {
                await Assert.ThrowsAsync<InvalidOperationException>(async () =>
                {
                    await foreach (var batch in reader.ReadCdcFile(storage, "/", "cdc.parquet", null, FlowtideDotNet.Storage.Memory.GlobalMemoryManager.Instance))
                    {
                        received.Add(batch);
                    }
                });
            }
            finally
            {
                DisposeAll(received);
            }

            Assert.NotEmpty(received);
            Assert.True(Assert.Single(storage.Opened).Disposed);
        }

        [Fact]
        public async Task CdcReadDisposesItsStreamWhenTheChangeTypeIsMissing()
        {
            var (reader, storage) = await SyntheticCdcFile(null);

            await Assert.ThrowsAsync<InvalidOperationException>(async () =>
            {
                await foreach (var batch in reader.ReadCdcFile(storage, "/", "cdc.parquet", null, FlowtideDotNet.Storage.Memory.GlobalMemoryManager.Instance))
                {
                    batch.data.Dispose();
                    batch.weights.Dispose();
                }
            });

            Assert.True(Assert.Single(storage.Opened).Disposed);
        }

        // Rows for the cdc test table, no _change_type column when changeTypes is null
        private static async Task<(ParquetSharpReader reader, DisposeTrackingStorage storage)> SyntheticCdcFile(List<string>? changeTypes)
        {
            var table = await DeltaTransactionReader.ReadTable(Files.Of.LocalDisk("../../../testdata"), "/simple_table_with_cdc");
            Assert.NotNull(table);
            var reader = new ParquetSharpReader();
            reader.Initialize(table, new List<string> { "id", "name" });

            int rows = changeTypes?.Count ?? 10;
            var schemaBuilder = new Apache.Arrow.Schema.Builder()
                .Field(new Field("id", Int32Type.Default, true))
                .Field(new Field("name", StringType.Default, true));
            if (changeTypes != null)
            {
                schemaBuilder.Field(new Field("_change_type", StringType.Default, true));
            }
            var schema = schemaBuilder.Build();

            var ids = new Int32Array.Builder();
            var names = new StringArray.Builder();
            for (int i = 0; i < rows; i++)
            {
                ids.Append(i);
                names.Append($"name_{i}");
            }
            var columns = new List<IArrowArray> { ids.Build(), names.Build() };
            if (changeTypes != null)
            {
                columns.Add(new StringArray.Builder().AppendRange(changeTypes).Build());
            }

            var inner = Files.Of.InternalMemory($"./cdc-{Guid.NewGuid():N}");
            using (var stream = await inner.OpenWrite("cdc.parquet"))
            using (var writer = new ParquetSharp.Arrow.FileWriter(stream, schema))
            {
                using var batch = new RecordBatch(schema, columns, rows);
                writer.WriteRecordBatch(batch);
                writer.Close();
            }
            return (reader, new DisposeTrackingStorage(inner));
        }

        [Theory]
        [InlineData("data")]
        [InlineData("addRemoved")]
        [InlineData("arrow")]
        [InlineData("cdc")]
        public async Task EarlyExitReleasesTheBatchReader(string method)
        {
            var (reader, storage) = await SyntheticCdcFile(Enumerable.Repeat("insert", 300_000).ToList());
            var allocator = GlobalMemoryManager.Instance;

            long released = method switch
            {
                "data" => await ReleasedOnEarlyExit(reader.ReadDataFile(storage, "/", "cdc.parquet", EmptyDeleteVector.Instance, null, allocator), b => b.data.Dispose()),
                "addRemoved" => await ReleasedOnEarlyExit(reader.ReadAddRemovedDataFile(storage, "/", "cdc.parquet", [(0, 1), (299_999, 1)], null, allocator), b =>
                {
                    b.data.Dispose();
                    b.weights.Dispose();
                }),
                "arrow" => await ReleasedOnEarlyExit(reader.ReadDataFileArrowFormat(storage, "/", "cdc.parquet"), b => b.Dispose()),
                _ => await ReleasedOnEarlyExit(reader.ReadCdcFile(storage, "/", "cdc.parquet", null, allocator), b =>
                {
                    b.data.Dispose();
                    b.weights.Dispose();
                }),
            };

            // A batch reader left open keeps its decode buffers in the Arrow pool
            Assert.True(released > 1_000_000, $"Released {released} bytes");
            Assert.True(Assert.Single(storage.Opened).Disposed);
        }

        [Fact]
        public async Task ArrowFormatBatchesOutliveTheReader()
        {
            var (reader, storage) = await SyntheticCdcFile(Enumerable.Repeat("insert", 100_000).ToList());

            var batches = new List<RecordBatch>();
            try
            {
                await foreach (var batch in reader.ReadDataFileArrowFormat(storage, "/", "cdc.parquet"))
                {
                    batches.Add(batch);
                }
                Assert.True(Assert.Single(storage.Opened).Disposed);

                int expected = 0;
                foreach (var batch in batches)
                {
                    var ids = (Int32Array)batch.Column(0);
                    var names = (StringArray)batch.Column(1);
                    for (int i = 0; i < batch.Length; i++, expected++)
                    {
                        Assert.Equal(expected, ids.GetValue(i));
                        Assert.Equal($"name_{expected}", names.GetString(i));
                    }
                }
                Assert.Equal(100_000, expected);
            }
            finally
            {
                foreach (var batch in batches)
                {
                    batch.Dispose();
                }
            }
        }

        [Fact]
        public async Task CdcReadFreesTheBatchItFailedOn()
        {
            var changeTypes = Enumerable.Repeat("insert", 100_000).ToList();
            changeTypes[^1] = "bogus";
            var (reader, storage) = await SyntheticCdcFile(changeTypes);
            var allocator = new CountingAllocator();

            var received = new List<CdcBatchResult>();
            try
            {
                await Assert.ThrowsAsync<InvalidOperationException>(async () =>
                {
                    await foreach (var batch in reader.ReadCdcFile(storage, "/", "cdc.parquet", null, allocator))
                    {
                        received.Add(batch);
                    }
                });
            }
            finally
            {
                DisposeAll(received);
            }

            Assert.NotEmpty(received);
            Assert.Equal(0, allocator.Outstanding);
        }

        // Arrow pool bytes freed by disposing the reader after its first batch
        private static async Task<long> ReleasedOnEarlyExit<T>(IAsyncEnumerable<T> batches, Action<T> dispose)
        {
            var pool = ParquetSharp.MemoryPool.GetDefaultMemoryPool();
            var enumerator = batches.GetAsyncEnumerator();
            Assert.True(await enumerator.MoveNextAsync());
            dispose(enumerator.Current);
            var held = pool.BytesAllocated;
            await enumerator.DisposeAsync();
            return held - pool.BytesAllocated;
        }

        private static void DisposeAll(List<CdcBatchResult> batches)
        {
            foreach (var batch in batches)
            {
                batch.data.Dispose();
                batch.weights.Dispose();
            }
        }

        // Bytes taken through the block paths and not yet freed
        private sealed class CountingAllocator : IMemoryAllocator
        {
            private long _outstanding;

            public long Outstanding => Interlocked.Read(ref _outstanding);

            public IMemoryOwner<byte> Allocate(int size, int alignment)
            {
                return GlobalMemoryManager.Instance.Allocate(size, alignment);
            }

            public IMemoryOwner<byte> Realloc(IMemoryOwner<byte> memory, int size, int alignment)
            {
                return GlobalMemoryManager.Instance.Realloc(memory, size, alignment);
            }

            public void RegisterAllocationToMetrics(int size)
            {
                Interlocked.Add(ref _outstanding, size);
            }

            public void RegisterFreeToMetrics(int size)
            {
                Interlocked.Add(ref _outstanding, -size);
            }
        }

        // Read streams record their disposal
        private sealed class DisposeTrackingStorage : HookFileStorage, IFileStorage
        {
            public DisposeTrackingStorage(IFileStorage inner) : base(inner)
            {
            }

            public ConcurrentQueue<DisposeTrackingStream> Opened { get; } = new ConcurrentQueue<DisposeTrackingStream>();

            async Task<Stream?> IFileStorage.OpenRead(IOPath path, CancellationToken cancellationToken)
            {
                var stream = await base.OpenRead(path, cancellationToken);
                if (stream is not MemoryStream memory)
                {
                    return stream;
                }
                var tracked = new DisposeTrackingStream(memory.ToArray());
                await memory.DisposeAsync();
                Opened.Enqueue(tracked);
                return tracked;
            }
        }

        private sealed class DisposeTrackingStream : MemoryStream
        {
            public DisposeTrackingStream(byte[] bytes) : base(bytes, false)
            {
            }

            public volatile bool Disposed;

            protected override void Dispose(bool disposing)
            {
                Disposed = true;
                base.Dispose(disposing);
            }
        }

        private sealed class ThreadRecordingStream : MemoryStream
        {
            public ThreadRecordingStream(byte[] bytes) : base(bytes)
            {
            }

            public volatile int Caller;

            public int Reads;

            public int ForeignReads;

            public override int Read(byte[] buffer, int offset, int count)
            {
                Record();
                return base.Read(buffer, offset, count);
            }

            public override int Read(Span<byte> buffer)
            {
                Record();
                return base.Read(buffer);
            }

            private void Record()
            {
                Interlocked.Increment(ref Reads);
                if (Environment.CurrentManagedThreadId != Caller)
                {
                    Interlocked.Increment(ref ForeignReads);
                }
            }
        }
    }
}
