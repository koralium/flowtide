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

using FlowtideDotNet.Core.ColumnStore;
using FlowtideDotNet.Core.ColumnStore.DataValues;
using FlowtideDotNet.Core.ColumnStore.ObjectConverter;
using Parquet;
using Parquet.Data;
using Parquet.Schema;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FlowtideDotNet.Connector.DeltaLake.Internal.Delta.ParquetFormat.Encoders
{
    internal class ArrayEncoder : IParquetEncoder
    {
        private readonly IParquetEncoder inner;
        private readonly int repititionLevel;
        private readonly int definitionLevel;
        private DataColumn? _column;
        private int innerIndex;

        public ArrayEncoder(IParquetEncoder inner, int repititionLevel, int definitionLevel)
        {
            this.inner = inner;
            this.repititionLevel = repititionLevel;
            this.definitionLevel = definitionLevel;
        }

        public void FinishBatch()
        {
        }

        public void NewBatch(ParquetSchema schema, Dictionary<string, string>? partitionValues)
        {
            inner.NewBatch(schema, partitionValues);
        }

        public Task NewRowGroup(IParquetRowGroupReader rowGroupReader)
        {
            innerIndex = 0;
            return inner.NewRowGroup(rowGroupReader);
        }

        public void ReadNextData(ref AddToColumnParquet addToColumnFunc)
        {
            int innerRepLevel = 0;
            int innerNextRepLevel = 0;
            var innerDefinitionLevel = 0;

            List<IDataValue> values = new List<IDataValue>();

            do
            {
                (innerRepLevel, innerNextRepLevel, innerDefinitionLevel) = inner.ReadNextRepitionAndDefinition();
            } while (true);
            //List<IDataValue> values = new List<IDataValue>();
            //int nextLevel = 0;
            //do
            //{
            //    int definitionLevel = 0;
            //    AddToColumnParquet func = new AddToColumnParquet();
            //    (nextLevel, definitionLevel) = inner.Read(innerIndex, ref func);
            //    values.Add(func.BoxedValue!);
            //    innerIndex++;
            //} while (nextLevel == repititionLevel);

            //addToColumnFunc.AddValue(new ListValue(values), 0);

            //return (0, 0);
        }

        public (int repititionLevel, int nextRepititionLevel, int definitionLevel) ReadNextRepitionAndDefinition()
        {
            throw new NotImplementedException();
        }

        public void SkipNextIndex()
        {
            throw new NotImplementedException();
        }
    }
}
