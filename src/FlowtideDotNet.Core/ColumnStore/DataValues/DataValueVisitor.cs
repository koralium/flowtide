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

namespace FlowtideDotNet.Core.ColumnStore.DataValues
{
    public class DataValueVisitor
    {
        public virtual void Visit<T>(ref readonly T value) where T : IDataValue
        {
            var visitor = this;
            value.Accept(in visitor);
        }

        public virtual void VisitBinaryValue(ref readonly BinaryValue binaryValue)
        {
            throw new NotImplementedException();
        }

        public virtual void VisitBoolValue(ref readonly BoolValue boolValue)
        {
            throw new NotImplementedException();
        }

        public virtual void VisitDecimalValue(ref readonly DecimalValue decimalValue)
        {
            throw new NotImplementedException();
        }

        public virtual void VisitDoubleValue(ref readonly DoubleValue doubleValue)
        {
            throw new NotImplementedException();
        }

        public virtual void VisitInt64Value(ref readonly Int64Value int64Value)
        {
            throw new NotImplementedException();
        }

        public virtual void VisitListValue(ref readonly ListValue listValue)
        {
            throw new NotImplementedException();
        }

        public virtual void VisitMapValue(ref readonly MapValue mapValue)
        {
            throw new NotImplementedException();
        }

        public virtual void VisitNullValue(ref readonly NullValue nullValue)
        {
            throw new NotImplementedException();
        }

        public virtual void VisitReferenceListValue(ref readonly ReferenceListValue referenceListValue)
        {
            throw new NotImplementedException();
        }

        public virtual void VisitReferenceMapValue(ref readonly ReferenceMapValue referenceMapValue)
        {
            throw new NotImplementedException();
        }

        public virtual void VisitStringValue(ref readonly StringValue stringValue)
        {
            throw new NotImplementedException();
        }

        public virtual void VisitTimestampTzValue(ref readonly TimestampTzValue timestampTzValue)
        {
            throw new NotImplementedException();
        }

        public virtual void VisitStructValue(ref readonly StructValue structValue)
        {
            throw new NotImplementedException();
        }

        public virtual void VisitReferenceStructValue(ref readonly ReferenceStructValue referenceStructValue)
        {
            throw new NotImplementedException();
        }
    }
}
