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

namespace FlowtideDotNet.Core.ColumnStore.Serialization
{
    internal enum CompressionType : sbyte
    {
        LZ4_FRAME = 0,
        ZSTD = 1,
    };

    /// Provided for forward compatibility in case we need to support different
    /// strategies for compressing the IPC message body (like whole-body
    /// compression rather than buffer-level) in the future
    internal enum BodyCompressionMethod : sbyte
    {
        /// Each constituent buffer is first compressed with the indicated
        /// compressor, and then written with the uncompressed length in the first 8
        /// bytes as a 64-bit little-endian signed integer followed by the compressed
        /// buffer bytes (and then padding as required by the protocol). The
        /// uncompressed length may be set to -1 to indicate that the data that
        /// follows is not compressed, which can be useful for cases where
        /// compression does not yield appreciable savings.
        BUFFER = 0,
    };

#pragma warning disable CS0282 // There is no defined ordering between fields in multiple declarations of partial struct
    internal ref partial struct ArrowSerializer
#pragma warning restore CS0282 // There is no defined ordering between fields in multiple declarations of partial struct
    {
        public int CreateBodyCompression(CompressionType codec,
      BodyCompressionMethod method = BodyCompressionMethod.BUFFER)
        {
            StartTable(2);
            BodyCompressionAddMethod(method);
            BodyCompressionAddCodec(codec);
            return EndTable();
        }

        void BodyCompressionAddMethod(BodyCompressionMethod method)
        {
            AddSbyte(1, (sbyte)method, 0);
        }

        void BodyCompressionAddCodec(CompressionType codec)
        {
            AddSbyte(0, (sbyte)codec, 0);
        }
    }
}
