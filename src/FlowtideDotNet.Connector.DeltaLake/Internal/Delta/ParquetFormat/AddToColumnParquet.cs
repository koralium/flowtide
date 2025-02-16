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
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FlowtideDotNet.Connector.DeltaLake.Internal.Delta.ParquetFormat
{
    internal struct AddToColumnParquet
    {
        private readonly IColumn? column;
        private IDataValue? boxedValue;
        private int repetitionLevel;

        public AddToColumnParquet(IColumn column)
        {
            this.column = column;
            boxedValue = null;
        }

        public AddToColumnParquet()
        {
            column = null;
        }

        /// <summary>
        /// If column was not set, this will contain the boxed value
        /// </summary>
        public IDataValue? BoxedValue => boxedValue;

        public int RepetitionLevel => repetitionLevel;

        public void AddValue<T>(T value)
            where T : IDataValue
        {
            if (column != null)
            {
                column.Add(value);
            }
            else
            {
                boxedValue = value;
            }
        }
    }
}
