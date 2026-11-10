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

using ParquetSharp;

namespace FlowtideDotNet.Connector.DeltaLake.Internal.Delta.ParquetFormat
{
    internal readonly record struct DeltaParquetCodec(Compression Compression, string FileExtension)
    {
        public WriterProperties WriterProperties => DeltaParquetCompression.GetWriterProperties(Compression);
    }

    internal static class DeltaParquetCompression
    {
        public const string CodecProperty = "delta.parquet.compression.codec";

        // Compaction outputs pin what their writer terms assume
        public const long OutputPageSize = 1L << 20;
        public const long OutputDictionaryPageSize = 1L << 20;
        public const long OutputWriteBatch = 1024;

        private static readonly DeltaParquetCodec Uncompressed = new DeltaParquetCodec(Compression.Uncompressed, ".parquet");
        private static readonly DeltaParquetCodec Snappy = new DeltaParquetCodec(Compression.Snappy, ".snappy.parquet");
        private static readonly DeltaParquetCodec Gzip = new DeltaParquetCodec(Compression.Gzip, ".gz.parquet");
        // Arrow writes Lz4Hadoop as the deprecated Parquet LZ4 and Lz4 as LZ4_RAW
        private static readonly DeltaParquetCodec Lz4 = new DeltaParquetCodec(Compression.Lz4Hadoop, ".lz4.parquet");
        private static readonly DeltaParquetCodec Lz4Raw = new DeltaParquetCodec(Compression.Lz4, ".lz4raw.parquet");
        private static readonly DeltaParquetCodec Zstd = new DeltaParquetCodec(Compression.Zstd, ".zstd.parquet");

        private static readonly object s_lock = new object();
        private static WriterProperties?[] s_writerProperties = new WriterProperties?[Enum.GetValues<Compression>().Length];
        private static WriterProperties?[] s_outputProperties = new WriterProperties?[Enum.GetValues<Compression>().Length];

        /// <summary>
        /// The codec for new files, the option wins over the table property and zstd is the default (spec).
        /// An unknown table value also gives zstd and is returned so the caller can warn.
        /// </summary>
        public static DeltaParquetCodec Resolve(IReadOnlyDictionary<string, string>? configuration, DeltaCompressionCodec? option, out string? unknownValue)
        {
            unknownValue = null;
            if (option.HasValue)
            {
                return option.Value switch
                {
                    DeltaCompressionCodec.Uncompressed => Uncompressed,
                    DeltaCompressionCodec.Snappy => Snappy,
                    DeltaCompressionCodec.Gzip => Gzip,
                    DeltaCompressionCodec.Lz4Raw => Lz4Raw,
                    _ => Zstd
                };
            }
            if (configuration == null || !configuration.TryGetValue(CodecProperty, out var value) || value == null)
            {
                return Zstd;
            }
            switch (value.Trim().ToLowerInvariant())
            {
                case "uncompressed":
                case "none":
                    return Uncompressed;
                case "snappy":
                    return Snappy;
                case "gzip":
                    return Gzip;
                case "lz4":
                    return Lz4;
                case "lz4_raw":
                    return Lz4Raw;
                case "zstd":
                    return Zstd;
                default:
                    unknownValue = value;
                    return Zstd;
            }
        }

        // Shared and never disposed, the native writer copies them
        public static WriterProperties GetWriterProperties(Compression compression)
        {
            var properties = Volatile.Read(ref s_writerProperties)[(int)compression];
            if (properties != null)
            {
                return properties;
            }
            lock (s_lock)
            {
                return s_writerProperties[(int)compression] ??= Build(compression, output: false);
            }
        }

        // Compaction outputs: the sink's settings, the copier ends each group
        public static WriterProperties GetOutputWriterProperties(Compression compression)
        {
            var properties = Volatile.Read(ref s_outputProperties)[(int)compression];
            if (properties != null)
            {
                return properties;
            }
            lock (s_lock)
            {
                return s_outputProperties[(int)compression] ??= Build(compression, output: true);
            }
        }

        // The settings Delta files rely on are set explicitly
        private static WriterProperties Build(Compression compression, bool output)
        {
            using var builder = new WriterPropertiesBuilder();
            builder.Compression(compression);
            builder.CompressionLevel(CompressionLevel(compression));
            builder.DataPageVersion(ParquetDataPageVersion.V1);
            builder.EnableDictionary();
            builder.EnableStatistics();
            // Compaction admission bounds byte arrays with these
            builder.SetSizeStatisticsLevel(SizeStatisticsLevel.PageAndColumnChunk);
            if (output)
            {
                // ParquetSharp's defaults, pinned so host settings can't move the terms
                builder.MemoryPool(MemoryPool.SystemMemoryPool());
                builder.MaxRowGroupLength(1L << 40);
                builder.DataPagesize(OutputPageSize);
                builder.DictionaryPagesizeLimit(OutputDictionaryPageSize);
                builder.WriteBatchSize(OutputWriteBatch);
                builder.Encoding(Encoding.Plain);
                builder.EnableWritePageIndex();
                builder.Version(ParquetVersion.PARQUET_2_6);
            }
            return builder.Build();
        }

        // Arrow rejects a level for codecs without one, int.MinValue is its default level
        public static int CompressionLevel(Compression compression) => compression switch
        {
            Compression.Zstd => 1,
            Compression.Gzip => 6,
            Compression.Lz4 => 1,
            _ => int.MinValue
        };

        internal static void ResetCacheForTests()
        {
            lock (s_lock)
            {
                Volatile.Write(ref s_writerProperties, new WriterProperties?[s_writerProperties.Length]);
                Volatile.Write(ref s_outputProperties, new WriterProperties?[s_outputProperties.Length]);
            }
        }
    }
}
