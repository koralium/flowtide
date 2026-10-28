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
using Apache.Arrow.Ipc;
using ParquetSharp;
using ParquetSharp.Arrow;

namespace FlowtideDotNet.Connector.DeltaLake.Internal.Delta.ParquetFormat
{
    // An open data file, owning its stream and readers
    internal sealed class ParquetDataFile : IDisposable
    {
        private readonly Stream _stream;
        private readonly FileReader _reader;
        private readonly List<IArrowArrayStream> _batchReaders = new List<IArrowArrayStream>();

        public ParquetDataFile(string path, Stream stream, FileReader reader)
        {
            Path = path;
            _stream = stream;
            _reader = reader;
            // Each access makes a new reader, so keep one
            LowLevel = reader.ParquetReader;
            try
            {
                var metadata = LowLevel.FileMetaData;
                NumRows = metadata.NumRows;
                RowGroupStarts = new long[metadata.NumRowGroups];
                long start = 0;
                for (int i = 0; i < RowGroupStarts.Length; i++)
                {
                    RowGroupStarts[i] = start;
                    using var rowGroup = LowLevel.RowGroup(i);
                    start += rowGroup.MetaData.NumRows;
                }
                if (start != NumRows)
                {
                    throw new InvalidDataException($"Delta file {path} cannot be decoded: the footer has {NumRows} rows and its row groups {start}");
                }
            }
            catch
            {
                LowLevel.Dispose();
                throw;
            }
        }

        // The path in the table, for messages
        public string Path { get; }

        public Apache.Arrow.Schema Schema => _reader.Schema;

        // Borrowed, the file reader owns it
        public SchemaManifest Manifest => _reader.SchemaManifest;

        // Rows in the footer, deleted rows included
        public long NumRows { get; }

        // First row of each row group
        public long[] RowGroupStarts { get; }

        // Borrowed, the file disposes it and its footer
        public ParquetFileReader LowLevel { get; }

        // All columns when no leaves are given
        public async IAsyncEnumerable<RecordBatch> ReadBatches(int[]? leafColumns)
        {
            var batchReader = leafColumns == null ? _reader.GetRecordBatchReader() : _reader.GetRecordBatchReader(columns: leafColumns);
            _batchReaders.Add(batchReader);
            try
            {
                RecordBatch batch;
                while ((batch = await batchReader.ReadNextRecordBatchAsync()) != null)
                {
                    yield return batch;
                }
            }
            finally
            {
                _batchReaders.Remove(batchReader);
                batchReader.Dispose();
            }
        }

        // Batch readers, the footer, the file reader, then the stream
        public void Dispose()
        {
            foreach (var batchReader in _batchReaders)
            {
                batchReader.Dispose();
            }
            _batchReaders.Clear();
            LowLevel.Dispose();
            _reader.Dispose();
            _stream.Dispose();
        }
    }
}
