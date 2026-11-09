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
using FlowtideDotNet.Connector.DeltaLake.Internal.Delta.ParquetFormat;
using ParquetSharp;

namespace FlowtideDotNet.Connector.DeltaLake.Tests
{
    public class ParquetLocalReaderTests
    {
        [Fact]
        public async Task RowGroupBatchesNeverCrossAGroup()
        {
            var path = Path.Join(Path.GetTempPath(), "local-reader-" + Guid.NewGuid().ToString("N") + ".parquet");
            try
            {
                var groups = WriteGroups(path, new[] { 25, 3, 40 });
                using var file = new ParquetDataFile(path, null, ParquetFileReaders.OpenLocal(path, 7, new FileInfo(path).Length));

                for (int group = 0; group < groups.Length; group++)
                {
                    var sizes = new List<int>();
                    var values = new List<long>();
                    await foreach (var batch in file.ReadRowGroup(group, new[] { 0 }))
                    {
                        sizes.Add(batch.Length);
                        values.AddRange(((Int64Array)batch.Column(0)).Values.ToArray());
                        batch.Dispose();
                    }

                    Assert.All(sizes, x => Assert.InRange(x, 1, 7));
                    Assert.Equal(groups[group], values);
                }
            }
            finally
            {
                File.Delete(path);
            }
        }

        [Fact]
        public async Task AnEmptyLeafListIsRefusedRatherThanReadingEveryColumn()
        {
            var path = Path.Join(Path.GetTempPath(), "local-reader-" + Guid.NewGuid().ToString("N") + ".parquet");
            try
            {
                WriteGroups(path, new[] { 10 });
                using var file = new ParquetDataFile(path, null, ParquetFileReaders.OpenLocal(path, 7, new FileInfo(path).Length));

                var error = await Assert.ThrowsAsync<InvalidOperationException>(async () => await file.ReadRowGroup(0, System.Array.Empty<int>()).GetAsyncEnumerator().MoveNextAsync());

                Assert.Contains("needs at least one leaf", error.Message);
            }
            finally
            {
                File.Delete(path);
            }
        }

        [Fact]
        public async Task PageStatisticsLongerThanTheFooterStillRead()
        {
            var path = Path.Join(Path.GetTempPath(), "local-reader-" + Guid.NewGuid().ToString("N") + ".parquet");
            try
            {
                var schema = new Schema.Builder().Field(new Field("s", Apache.Arrow.Types.StringType.Default, true)).Build();
                var values = new[] { "a" }.Concat(Enumerable.Repeat("b", 102)).Append("b" + new string('z', 3_999)).Concat(Enumerable.Repeat("c", 100)).ToArray();
                using (var builder = new WriterPropertiesBuilder())
                using (var properties = builder.DisableDictionary().EnableStatistics().DisableWritePageIndex().DataPagesize(1_024).WriteBatchSize(4).Build())
                using (var writer = new ParquetSharp.Arrow.FileWriter(path, schema, properties))
                using (var batch = new RecordBatch(schema, new[] { new StringArray.Builder().AppendRange(values).Build() }, values.Length))
                {
                    writer.WriteRecordBatch(batch);
                    writer.Close();
                }
                var bytes = File.ReadAllBytes(path);
                var footerLength = System.Buffers.Binary.BinaryPrimitives.ReadInt32LittleEndian(bytes.AsSpan(bytes.Length - 8, 4));
                using var file = new ParquetDataFile(path, null, ParquetFileReaders.OpenLocal(path, 1_024, footerLength));

                var rows = 0;
                await foreach (var batch in file.ReadRowGroup(0, new[] { 0 }))
                {
                    rows += batch.Length;
                    batch.Dispose();
                }

                Assert.True(footerLength < 4_000);
                Assert.Equal(values.Length, rows);
            }
            finally
            {
                File.Delete(path);
            }
        }

        [Fact]
        public void AFailedOpenReleasesTheReaderAndStream()
        {
            var path = Path.Join(Path.GetTempPath(), "local-reader-" + Guid.NewGuid().ToString("N") + ".parquet");
            try
            {
                WriteGroups(path, new[] { 10 });
                var bytes = File.ReadAllBytes(path);
                var footerLength = System.Buffers.Binary.BinaryPrimitives.ReadInt32LittleEndian(bytes.AsSpan(bytes.Length - 8, 4));
                for (int i = bytes.Length - 9; i >= bytes.Length - 8 - footerLength; i--)
                {
                    if (bytes[i] == 0x14 && bytes[i - 1] == 0x16)
                    {
                        bytes[i] = 0x16;
                        break;
                    }
                }
                File.WriteAllBytes(path, bytes);
                var reader = ParquetFileReaders.OpenLocal(path, 7, footerLength);
                var stream = new TrackingStream();

                Assert.Throws<InvalidDataException>(() => new ParquetDataFile(path, stream, reader));
                Assert.True(stream.Disposed);
                Assert.Throws<NullReferenceException>(() => reader.Schema);
            }
            finally
            {
                File.Delete(path);
            }
        }

        [Fact]
        public void ACorruptFooterFailsToOpen()
        {
            var path = Path.Join(Path.GetTempPath(), "local-reader-" + Guid.NewGuid().ToString("N") + ".parquet");
            try
            {
                WriteGroups(path, new[] { 10 });
                var bytes = File.ReadAllBytes(path);
                var footerLength = System.Buffers.Binary.BinaryPrimitives.ReadInt32LittleEndian(bytes.AsSpan(bytes.Length - 8, 4));
                bytes.AsSpan(bytes.Length - 8 - footerLength, footerLength).Fill(0xFF);
                File.WriteAllBytes(path, bytes);

                Assert.Throws<ParquetException>(() => ParquetFileReaders.OpenLocal(path, 7, footerLength));
            }
            finally
            {
                File.Delete(path);
            }
        }

        private sealed class TrackingStream : MemoryStream
        {
            public bool Disposed { get; private set; }

            protected override void Dispose(bool disposing)
            {
                Disposed = true;
                base.Dispose(disposing);
            }
        }

        private static List<long>[] WriteGroups(string path, int[] rows)
        {
            var schema = new Schema.Builder().Field(new Field("id", Apache.Arrow.Types.Int64Type.Default, false)).Build();
            using var properties = new WriterPropertiesBuilder().Build();
            using var writer = new ParquetSharp.Arrow.FileWriter(path, schema, properties);
            var groups = new List<long>[rows.Length];
            long next = 0;
            for (int group = 0; group < rows.Length; group++)
            {
                groups[group] = Enumerable.Range(0, rows[group]).Select(_ => next++).ToList();
                var builder = new Int64Array.Builder();
                builder.AppendRange(groups[group]);
                using var batch = new RecordBatch(schema, new[] { builder.Build() }, rows[group]);
                writer.WriteRecordBatch(batch, rows[group]);
            }
            writer.Close();
            return groups;
        }
    }
}
