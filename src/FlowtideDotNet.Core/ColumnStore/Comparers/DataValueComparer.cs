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
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FlowtideDotNet.Core.ColumnStore.Comparers
{
    internal class DataValueComparer : IComparer<IDataValue>
    {
        public int Compare(IDataValue? x, IDataValue? y)
        {
            if (x!.Type != y!.Type)
            {
                return x.Type - y.Type;
            }

            switch (x.Type)
            {
                case ArrowTypeId.Null:
                    return 0;
                case ArrowTypeId.Boolean:
                    return x.AsBool.CompareTo(y.AsBool);
                case ArrowTypeId.Int64:
                    return x.AsLong.CompareTo(y.AsLong);
                case ArrowTypeId.Double:
                    return x.AsDouble.CompareTo(y.AsDouble);
                case ArrowTypeId.String:
                    return x.AsString.CompareTo(y.AsString);
                case ArrowTypeId.Binary:
                    return x.AsBinary.SequenceCompareTo(y.AsBinary);
                case ArrowTypeId.Decimal128:
                    return x.AsDecimal.CompareTo(y.AsDecimal);
                default:
                    throw new NotImplementedException();
            }
        }
    }
}
