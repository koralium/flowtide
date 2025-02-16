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
    internal class DoubleEncoder : IParquetEncoder
    {
        private readonly FieldPath path;
        private DataField? _dataField;
        private DataColumn? _dataColumn;
        private double?[]? _arr;
        private int _dataIndex;
        private int _repititionIndex;

        public DoubleEncoder(FieldPath path)
        {
            this.path = path;
        }

        public void FinishBatch()
        {
            _arr = null;
            _dataColumn = null;
            _dataField = null;
        }

        public void NewBatch(ParquetSchema schema, Dictionary<string, string>? partitionValues)
        {
            _dataField = schema.DataFields.First(x => x.Path.Equals(path));
        }

        public async Task NewRowGroup(IParquetRowGroupReader rowGroupReader)
        {
            if (_dataField == null)
            {
                throw new InvalidOperationException("NewBatch must be called before NewRowGroup");
            }
            _dataColumn = await rowGroupReader.ReadColumnAsync(_dataField);
            _arr = (double?[])_dataColumn.Data;
            _repititionIndex = 0;
            _dataIndex = 0;
        }

        public void ReadNextData(ref AddToColumnParquet addToColumnFunc)
        {
            var val = _arr![_dataIndex];
            _dataIndex++;

            if (val.HasValue)
            {
                addToColumnFunc.AddValue(new DoubleValue(val.Value));
            }
            else
            {
                addToColumnFunc.AddValue(NullValue.Instance);
            }
        }

        public (int repititionLevel, int nextRepititionLevel, int definitionLevel) ReadNextRepitionAndDefinition()
        {
            int nextLevel = 0;
            int repititionLevel = 0;
            int definitionLevel = 0;

            if (_dataColumn!.RepetitionLevels != null)
            {
                repititionLevel = _dataColumn.RepetitionLevels[_repititionIndex];
                if (_dataColumn.RepetitionLevels.Length > _repititionIndex + 1)
                {
                    nextLevel = _dataColumn.RepetitionLevels[_repititionIndex + 1];
                }
            }
            if (_dataColumn!.DefinitionLevels != null)
            {
                definitionLevel = _dataColumn.DefinitionLevels[_repititionIndex];
            }
            _repititionIndex++;

            return (repititionLevel, nextLevel, definitionLevel);
        }

        public void SkipNextIndex()
        {
            throw new NotImplementedException();
        }
    }
}
