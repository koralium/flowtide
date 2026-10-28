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

using System.Collections;

namespace FlowtideDotNet.Connector.DeltaLake.Internal.Delta.DeletionVectors
{
    /// <summary>
    /// One batch's deleted rows as an array, so each column's lookup is an index.
    /// </summary>
    internal sealed class BatchDeleteMask : IDeleteVector
    {
        private bool[] _deleted = System.Array.Empty<bool>();
        private long _start;
        private int _length;
        private int _count;

        public long Cardinality => _count;

        // Covers the rows from start, none deleted
        public void Reset(long start, int length)
        {
            if (_deleted.Length < length)
            {
                _deleted = new bool[length];
            }
            else
            {
                System.Array.Clear(_deleted, 0, length);
            }
            _start = start;
            _length = length;
            _count = 0;
        }

        public void Add(long index)
        {
            _deleted[index - _start] = true;
            _count++;
        }

        public bool Contains(long index) => _deleted[index - _start];

        public IEnumerator<long> GetEnumerator()
        {
            for (int i = 0; i < _length; i++)
            {
                if (_deleted[i])
                {
                    yield return _start + i;
                }
            }
        }

        IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();
    }
}
