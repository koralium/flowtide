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

using Apache.Arrow.Ipc;
using FlowtideDotNet.Core.ColumnStore.Serialization;
using FlowtideDotNet.Core.ColumnStore.TreeStorage;
using FlowtideDotNet.Storage.Tree;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FlowtideDotNet.Core.Operators.Normalization
{
    internal class NormalizeValueSerializer : IBplusTreeValueSerializer<ColumnRowReference, NormalizeValueStorage>
    {
        private readonly List<int> _columnsToStore;

        public NormalizeValueSerializer(List<int> columnsToStore)
        {
            _columnsToStore = columnsToStore;
        }

        public NormalizeValueStorage CreateEmpty()
        {
            return new NormalizeValueStorage(_columnsToStore);    
        }

        public NormalizeValueStorage Deserialize(in BinaryReader reader)
        {
            using var arrowReader = new ArrowStreamReader(reader.BaseStream, new Apache.Arrow.Memory.NativeMemoryAllocator(), true);
            var recordBatch = arrowReader.ReadNextRecordBatch();

            var eventBatch = EventArrowSerializer.ArrowToBatch(recordBatch);

            return new NormalizeValueStorage(_columnsToStore, eventBatch, recordBatch.Length);
        }

        public void Serialize(in BinaryWriter writer, in NormalizeValueStorage values)
        {
            var recordBatch = EventArrowSerializer.BatchToArrow(values._data);
            var batchWriter = new ArrowStreamWriter(writer.BaseStream, recordBatch.Schema, true);
            batchWriter.WriteRecordBatch(recordBatch);
        }
    }
}
