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
using FlowtideDotNet.Storage.Tree;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FlowtideDotNet.Core.ColumnStore.TreeStorage
{
    internal class ColumnStoreSerializer : IBPlusTreeKeySerializer<ColumnRowReference, ColumnKeyStorageContainer>
    {
        private readonly int columnCount;

        public ColumnStoreSerializer(int columnCount)
        {
            this.columnCount = columnCount;
        }
        public ColumnKeyStorageContainer CreateEmpty()
        {
            return new ColumnKeyStorageContainer(columnCount);
        }

        public ColumnKeyStorageContainer Deserialize(in BinaryReader reader)
        {
            using var arrowReader = new ArrowStreamReader(reader.BaseStream, new Apache.Arrow.Memory.NativeMemoryAllocator(), true);
            var recordBatch = arrowReader.ReadNextRecordBatch();
            
            List<IColumn> columns = new List<IColumn>();
            var visitor = new ArrowToInternalVisitor();
            for (int i = 0; i < recordBatch.ColumnCount; i++)
            {
                recordBatch.Column(i).Accept(visitor);
                columns.Add(visitor.Column!);
            }
            recordBatch.Dispose();

            return new ColumnKeyStorageContainer(recordBatch.ColumnCount, new EventBatchData(columns));
        }

        public void Serialize(in BinaryWriter writer, in ColumnKeyStorageContainer values)
        {
            
            var recordBatch = EventBatchToArrow.BatchToArrow(values._data);
            var batchWriter = new ArrowStreamWriter(writer.BaseStream, recordBatch.Schema, true);
            batchWriter.WriteRecordBatch(recordBatch);
            //batchWriter.WriteEnd();
            //MemoryStream memoryStream = new MemoryStream();
            //var batchWriter = new ArrowStreamWriter(memoryStream, recordBatch.Schema, true);
            //batchWriter.WriteRecordBatch(recordBatch);
            //batchWriter.WriteEnd();
            //var arr = memoryStream.ToArray();
            //writer.Write(arr.Length);
            //writer.Write(arr);
        }
    }
}
