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

using FlowtideDotNet.Substrait;
using Orleans.Serialization;
using Orleans.Serialization.Buffers;
using Orleans.Serialization.Cloning;
using Orleans.Serialization.Serializers;
using Orleans.Serialization.WireProtocol;
using System;
using System.Buffers;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FlowtideDotNet.Orleans.Internal
{
    public class OrleansPlanSerializer : IGeneralizedCodec, IGeneralizedCopier, ITypeFilter
    {
        public object DeepCopy(object input, CopyContext context)
        {
            throw new NotImplementedException();
        }

        public bool IsSupportedType(Type type)
        {
            if (type == typeof(Plan))
                return true;
            return false;
        }

        public bool? IsTypeAllowed(Type type)
        {
            return IsSupportedType(type);
        }

        public object ReadValue<TInput>(ref Reader<TInput> reader, Field field)
        {
            throw new NotImplementedException();
        }

        public void WriteField<TBufferWriter>(ref Writer<TBufferWriter> writer, uint fieldIdDelta, Type expectedType, object value) where TBufferWriter : IBufferWriter<byte>
        {
            throw new NotImplementedException();
        }
    }
}
