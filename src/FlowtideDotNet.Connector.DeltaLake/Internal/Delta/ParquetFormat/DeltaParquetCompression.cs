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

        private static readonly DeltaParquetCodec Uncompressed = new DeltaParquetCodec(Compression.Uncompressed, ".parquet");
        private static readonly DeltaParquetCodec Snappy = new DeltaParquetCodec(Compression.Snappy, ".snappy.parquet");
        private static readonly DeltaParquetCodec Gzip = new DeltaParquetCodec(Compression.Gzip, ".gz.parquet");
        // Arrow writes Lz4Hadoop as the deprecated Parquet LZ4 and Lz4 as LZ4_RAW
        private static readonly DeltaParquetCodec Lz4 = new DeltaParquetCodec(Compression.Lz4Hadoop, ".lz4.parquet");
        private static readonly DeltaParquetCodec Lz4Raw = new DeltaParquetCodec(Compression.Lz4, ".lz4raw.parquet");
        private static readonly DeltaParquetCodec Zstd = new DeltaParquetCodec(Compression.Zstd, ".zstd.parquet");

        private static readonly object s_lock = new object();
        private static WriterProperties?[] s_writerProperties = new WriterProperties?[Enum.GetValues<Compression>().Length];

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
                return s_writerProperties[(int)compression] ??= Build(compression);
            }
        }

        // Every setting is explicit, so host wide ParquetSharp defaults cannot change Delta files
        private static WriterProperties Build(Compression compression)
        {
            using var builder = new WriterPropertiesBuilder();
            builder.Compression(compression);
            // Arrow rejects a level for codecs without one, int.MinValue is its default level
            builder.CompressionLevel(compression switch
            {
                Compression.Zstd => 1,
                Compression.Gzip => 6,
                Compression.Lz4 => 1,
                _ => int.MinValue
            });
            builder.DataPageVersion(ParquetDataPageVersion.V1);
            builder.EnableDictionary();
            builder.EnableStatistics();
            return builder.Build();
        }

        internal static void ResetCacheForTests()
        {
            lock (s_lock)
            {
                Volatile.Write(ref s_writerProperties, new WriterProperties?[s_writerProperties.Length]);
            }
        }
    }
}
