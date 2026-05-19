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

using System;
using System.Buffers;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using static SqlParser.Ast.DataType;
using static SqlParser.Ast.MatchRecognizeSymbol;

namespace FlowtideDotNet.Connector.Files.Internal.CsvFiles.Parser
{
    internal class PipeSegment : ReadOnlySequenceSegment<byte>
    {
        internal PipeSegment? _next;
        private int _end;
        private readonly byte[] _data;
        private readonly ArrayPool<byte> _pool;

        public PipeSegment(ArrayPool<byte> pool, int bufferSize)
        {
            _data = pool.Rent(bufferSize);
            AvailableMemory = _data;
            _end = 0;
            this.Memory = _data;
            this._pool = pool;
        }

        public Memory<byte> AvailableMemory { get; private set; }

        public int Length => End;

        public int End
        {
            get => _end;
            set
            {
                Debug.Assert(value <= _data.Length);

                _end = value;
                Memory = new Memory<byte>(_data, 0, value);
            }
        }

        public void Advance(int bytesWritten)
        {
            End += bytesWritten;
        }

        public void SetNext(PipeSegment segment)
        {
            Debug.Assert(segment != null);
            Debug.Assert(Next == null);

            _next = segment;
            Next = _next;

            // FIX: We set the NEXT segment's running index based on the current one
            segment.RunningIndex = this.RunningIndex + this.Length;
        }

        public void Reset()
        {
            Next = null;
            Memory = Memory<byte>.Empty;
            _end = 0;
            RunningIndex = 0;
        }

        public void ReturnToPool()
        {
            _pool.Return(_data);
        }
    }
}
