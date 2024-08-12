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

using FlowtideDotNet.Core.ColumnStore.DataValues;
using FlowtideDotNet.Core.Flexbuffer;
using System;
using System.Collections;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FlowtideDotNet.Core.ColumnStore
{
    internal struct MapValue : IMapValue
    {
        private readonly IEnumerable<KeyValuePair<IDataValue, IDataValue>> keyValuePairs;

        public MapValue(IEnumerable<KeyValuePair<IDataValue, IDataValue>> keyValuePairs)
        {
            this.keyValuePairs = keyValuePairs;
        }

        public ArrowTypeId Type => ArrowTypeId.Map;

        public long AsLong => throw new NotImplementedException();

        public FlxString AsString => throw new NotImplementedException();

        public bool AsBool => throw new NotImplementedException();

        public double AsDouble => throw new NotImplementedException();

        public IListValue AsList => throw new NotImplementedException();

        public Span<byte> AsBinary => throw new NotImplementedException();

        public IMapValue AsMap => this;

        public decimal AsDecimal => throw new NotImplementedException();

        public IEnumerator<KeyValuePair<IDataValue, IDataValue>> GetEnumerator()
        {
            return keyValuePairs.GetEnumerator();
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return keyValuePairs.GetEnumerator();
        }
    }
}
