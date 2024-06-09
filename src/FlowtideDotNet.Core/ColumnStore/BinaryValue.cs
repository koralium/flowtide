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

using FlowtideDotNet.Core.Flexbuffer;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FlowtideDotNet.Core.ColumnStore
{
    public struct BinaryValue : IDataValue
    {
        private readonly byte[] _bytes;
        private readonly int _start;
        private readonly int _end;

        public BinaryValue(byte[] bytes, int start, int end)
        {
            this._bytes = bytes;
            this._start = start;
            this._end = end;
        }
        public ArrowTypeId Type => ArrowTypeId.Binary;

        public long AsLong => throw new NotImplementedException();

        public FlxString AsString => throw new NotImplementedException();

        public bool AsBool => throw new NotImplementedException();

        public double AsDouble => throw new NotImplementedException();

        public ListValue AsList => throw new NotImplementedException();

        public Span<byte> AsBinary => _bytes.AsSpan(_start, _end - _start);

        public IMapValue AsMap => throw new NotImplementedException();

        public decimal AsDecimal => throw new NotImplementedException();
    }
}
