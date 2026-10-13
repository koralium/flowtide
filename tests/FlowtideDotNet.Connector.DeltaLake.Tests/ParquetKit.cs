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

namespace FlowtideDotNet.Connector.DeltaLake.Tests
{
    internal static class ParquetKit
    {
        // Bytes between the last column chunk and the footer, where a page index is written
        public static long BytesBeforeFooter(string path)
        {
            using var file = new ParquetFileReader(path);
            long data = 4;
            for (int g = 0; g < file.FileMetaData.NumRowGroups; g++)
            {
                using var rowGroup = file.RowGroup(g);
                for (int c = 0; c < file.FileMetaData.NumColumns; c++)
                {
                    data += rowGroup.MetaData.GetColumnChunkMetaData(c).TotalCompressedSize;
                }
            }
            var bytes = File.ReadAllBytes(path);
            var footer = BitConverter.ToInt32(bytes, bytes.Length - 8);
            return bytes.Length - 8 - footer - data;
        }
    }
}
