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

namespace FlowtideDotNet.Storage
{
    /// <summary>
    /// The algorithm used to compress state pages before they are written to the file cache or persistent storage.
    /// </summary>
    public enum CompressionType
    {
        /// <summary>
        /// Pages are written uncompressed.
        /// </summary>
        None = 0,
        /// <summary>
        /// Pages are compressed with Zstandard.
        /// </summary>
        Zstd = 1
    };

    /// <summary>
    /// The granularity at which compression is applied to state pages.
    /// </summary>
    public enum CompressionMethod
    {
        /// <summary>
        /// Compresses the entire page
        /// </summary>
        Page = 0,
    }

    /// <summary>
    /// Controls how state pages are serialized to the file cache and persistent storage, mainly how they are compressed.
    /// An instance left at its defaults applies no compression. Configure it through the storage builder, for example
    /// with <c>ZstdPageCompression</c> or <c>NoCompression</c>.
    /// </summary>
    public class StateSerializeOptions
    {
        /// <summary>
        /// The compression algorithm to use. Defaults to <see cref="CompressionType.None"/>.
        /// Compression is only applied when this is set to <see cref="CompressionType.Zstd"/> together with
        /// <see cref="CompressionMethod.Page"/>.
        /// </summary>
        public CompressionType CompressionType { get; set; }

        /// <summary>
        /// The granularity at which compression is applied. Defaults to <see cref="CompressionMethod.Page"/>.
        /// </summary>
        public CompressionMethod CompressionMethod { get; set; }

        /// <summary>
        /// The compression level passed to the compressor. When null a default level of 3 is used.
        /// Only used when <see cref="CompressionType"/> enables compression.
        /// </summary>
        public int? CompressionLevel { get; set; }
    }
}
