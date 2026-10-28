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

namespace FlowtideDotNet.Connector.DeltaLake.Internal.Delta.ParquetFormat.Comparers
{
    // Arrow allocates on every Fields read, keep the last two
    internal sealed class FieldsCache
    {
        private Entry? _first;
        private Entry? _second;

        public IReadOnlyList<IArrowArray> Get(StructArray array)
        {
            var first = _first;
            if (first != null && ReferenceEquals(first.Array, array))
            {
                return first.Fields;
            }
            var second = _second;
            if (second != null && ReferenceEquals(second.Array, array))
            {
                return second.Fields;
            }
            var entry = new Entry(array, array.Fields);
            _second = first;
            _first = entry;
            return entry.Fields;
        }

        private sealed class Entry
        {
            public Entry(StructArray array, IReadOnlyList<IArrowArray> fields)
            {
                Array = array;
                Fields = fields;
            }

            public StructArray Array { get; }

            public IReadOnlyList<IArrowArray> Fields { get; }
        }
    }
}
