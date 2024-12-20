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

namespace FlowtideDotNet.Core.ColumnStore.Serialization
{
    internal class EventBatchSerializer
    {
        public EventBatchSerializer()
        {
            
        }

        public Memory<byte> SerializeSchema(EventBatchData eventBatchData)
        {
            int schemaFieldCount = 0;
            for (int i = 0; i <  eventBatchData.Columns.Count; i++)
            {
                schemaFieldCount = eventBatchData.Columns[i].SchemaFieldCountEstimate();
            }
            var overhead = 200 + (schemaFieldCount * 100); //100 bytes per field as an big overestimate

            var schemaMemory = new byte[overhead];
            var vtable = new int[100];
            var vtables = new int[100];
            var fieldsarray = new int[100];
            ArrowSerializer arrowSerializer = new ArrowSerializer(schemaMemory, vtable, vtables);

            var emptyStringOffset = arrowSerializer.CreateEmptyString();

            arrowSerializer.SchemaCreateFieldsVector(fieldsarray);
            
            return new byte[1];
        }
    }
}
