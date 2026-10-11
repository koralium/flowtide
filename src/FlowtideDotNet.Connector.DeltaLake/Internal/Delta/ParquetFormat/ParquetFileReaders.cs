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

using ParquetSharp.Arrow;

namespace FlowtideDotNet.Connector.DeltaLake.Internal.Delta.ParquetFormat
{
    internal static class ParquetFileReaders
    {
        // Shared by every reader and never disposed
        private static readonly ArrowReaderProperties s_properties = CreateProperties();

        public static FileReader Open(Stream stream)
        {
            return new FileReader(stream, null, s_properties);
        }

        private static ArrowReaderProperties CreateProperties()
        {
            var properties = ArrowReaderProperties.GetDefault();
            // Pre-buffered reads call the managed stream from Arrow's own threads, which crashed the process on Linux
            properties.PreBuffer = false;
            return properties;
        }
    }
}
