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
        private static readonly ArrowReaderProperties s_properties = CreateProperties(forDeletes: false);
        private static readonly ArrowReaderProperties s_deleteProperties = CreateProperties(forDeletes: true);

        public static FileReader Open(Stream stream)
        {
            return new FileReader(stream, null, s_properties);
        }

        // Matching and rewriting read INT96 as microseconds
        public static FileReader OpenForDeletes(Stream stream)
        {
            return new FileReader(stream, null, s_deleteProperties);
        }

        private static ArrowReaderProperties CreateProperties(bool forDeletes)
        {
            var properties = ArrowReaderProperties.GetDefault();
            // Pre-buffered reads call the managed stream from Arrow's own threads, which crashed the process on Linux
            properties.PreBuffer = false;
            if (forDeletes)
            {
                properties.CoerceInt96TimestampUnit = Apache.Arrow.Types.TimeUnit.Microsecond;
            }
            return properties;
        }
    }
}
