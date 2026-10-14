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

using FlowtideDotNet.Connector.DeltaLake.Internal.Delta.ParquetFormat;
using ParquetSharp;
using ParquetSharp.Arrow;

namespace FlowtideDotNet.Connector.DeltaLake.Internal.Delta.Checkpoints
{
    /// <summary>
    /// The checkpoint writer's Parquet and Arrow settings.
    /// </summary>
    internal static class CheckpointWriterSettings
    {
        public const long DataPageSize = 256L << 10;
        public const long WriteBatchSize = 1024;

        public static WriterProperties Build(Compression compression)
        {
            using var builder = new WriterPropertiesBuilder();
            builder.Compression(compression);
            builder.CompressionLevel(DeltaParquetCompression.CompressionLevel(compression));
            builder.DataPageVersion(ParquetDataPageVersion.V1);
            builder.DataPagesize(DataPageSize);
            builder.WriteBatchSize(WriteBatchSize);
            builder.DisableDictionary();
            builder.DisableStatistics();
            builder.DisableWritePageIndex();
            // Groups are closed by the writer's own projection, never by a row count
            builder.MaxRowGroupLength(1L << 40);
            builder.MemoryPool(MemoryPool.SystemMemoryPool());
            return builder.Build();
        }

        public static ArrowWriterProperties Arrow()
        {
            using var builder = new ArrowWriterPropertiesBuilder();
            builder.StoreSchema();
            builder.UseThreads(false);
            return builder.Build();
        }
    }
}
