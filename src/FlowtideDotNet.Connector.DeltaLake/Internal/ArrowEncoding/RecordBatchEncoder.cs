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

using Apache.Arrow;
using Apache.Arrow.Memory;
using FlowtideDotNet.Core.ColumnStore;
using FlowtideDotNet.Core.Operators.Write.Column;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FlowtideDotNet.Connector.DeltaLake.Internal.ArrowEncoding
{
    internal class RecordBatchEncoder
    {
        private readonly Schema m_schema;
        private List<IArrowColumnEncoder> m_encoders;
        private DataValueContainer m_dataValueContainer;
        private BooleanArray.Builder? m_deletedArray;
        private int m_count;

        private RecordBatchEncoder(Schema schema, List<IArrowColumnEncoder> encoders)
        {
            this.m_schema = schema;
            m_encoders = encoders;
            m_dataValueContainer = new DataValueContainer();
            NewBatch();
        }

        public void NewBatch()
        {
            m_count = 0;
            foreach (var encoder in m_encoders)
            {
                encoder.NewBatch();
            }
            m_deletedArray = new BooleanArray.Builder();
        }

        public void AddRow(ColumnWriteOperation columnWriteOperation)
        {
            Debug.Assert(m_deletedArray != null);
            m_count++;
            for (int i = 0; i < m_encoders.Count; i++)
            {
                columnWriteOperation.EventBatchData.Columns[i].GetValueAt(columnWriteOperation.Index, m_dataValueContainer, default);
                m_encoders[i].AddValue(m_dataValueContainer);
            }
            m_deletedArray.Append(!columnWriteOperation.IsDeleted);
        }

        public RecordBatch RecordBatch(MemoryAllocator memoryAllocator, bool includeDeletedColumn)
        {
            Debug.Assert(m_deletedArray != null);
            List<IArrowArray> arrays = new List<IArrowArray>();
            foreach (var encoder in m_encoders)
            {
                arrays.Add(encoder.BuildArray(memoryAllocator));
            }
            if (includeDeletedColumn)
            {
                arrays.Add(m_deletedArray.Build());
            }
            
            return new RecordBatch(m_schema, arrays, m_count);
        }

        public static RecordBatchEncoder Create(Schema schema)
        {
            List<IArrowColumnEncoder> encoders = new List<IArrowColumnEncoder>();
            var visitor = new EventBatchToArrowVisitor();
            for (int i = 0; i < schema.FieldsList.Count; i++)
            {
                schema.GetFieldByIndex(i).DataType.Accept(visitor);
                encoders.Add(visitor.Encoder ?? throw new InvalidOperationException("No encoder found"));
            }
            return new RecordBatchEncoder(schema, encoders);
        }
    }
}
