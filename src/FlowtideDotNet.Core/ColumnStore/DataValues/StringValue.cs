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
using System.Reflection.Metadata.Ecma335;
using System.Text;
using System.Threading.Tasks;

namespace FlowtideDotNet.Core.ColumnStore
{
    public struct StringValue : IDataValue
    {
        private byte[] _utf8;
        private readonly int start;
        private readonly int end;

        public ArrowTypeId Type => ArrowTypeId.String;

        public long AsLong => throw new NotImplementedException();

        public FlxString AsString => new FlxString(_utf8.AsSpan(start, end - start));

        public bool AsBool => throw new NotImplementedException();

        public double AsDouble => throw new NotImplementedException();

        public ListValue AsList => throw new NotImplementedException();

        public Span<byte> AsBinary => _utf8.AsSpan(start, end - start);

        public IMapValue AsMap => throw new NotImplementedException();

        public decimal AsDecimal => throw new NotImplementedException();

        public StringValue(byte[] utf8, int start, int end)
        {
            _utf8 = utf8;
            this.start = start;
            this.end = end;
        }

        public StringValue(byte[] utf8)
        {
            _utf8 = utf8;
            this.start = 0;
            this.end = utf8.Length;
        }

        public StringValue(string value)
        {
            _utf8 = Encoding.UTF8.GetBytes(value);
            start = 0;
            end = _utf8.Length;
        }
    }
}
