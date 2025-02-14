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
using System.Collections.Generic;
using System.IO.Hashing;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FlowtideDotNet.Core.ColumnStore
{
    public interface IDataValue
    {
        ArrowTypeId Type { get; }

        long AsLong { get; }

        FlxString AsString { get; }

        bool AsBool { get; }

        double AsDouble { get; }

        IListValue AsList { get; }

        ReadOnlySpan<byte> AsBinary { get; }

        IMapValue AsMap { get; }

        decimal AsDecimal { get; }

        bool IsNull { get; }

        TimestampTzValue AsTimestamp { get; }

        void CopyToContainer(DataValueContainer container);

        void AddToHash(NonCryptographicHashAlgorithm hashAlgorithm);
    }
}
