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
using FlowtideDotNet.Connector.DeltaLake.Internal.Delta.DeletionVectors;
using FlowtideDotNet.Connector.DeltaLake.Internal.Delta.Schema.Types;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FlowtideDotNet.Connector.DeltaLake.Internal.Delta.ParquetFormat.Comparers
{
    internal class RecordBatchComparer
    {
        private readonly IArrowComparer[] _comparers;

        public RecordBatchComparer(IArrowComparer[] comparers)
        {
            _comparers = comparers;
        }

        public int FindOccurance(int toFindIndex, RecordBatch toFindFrom, RecordBatch searchIn, int globalOffset, IDeleteVector deleteVector)
        {
            if (_comparers.Length == 0)
            {
                return 0;
            }

            int startIndex = 0;

            while (true)
            {
                startIndex = _comparers[0].FindOccurance(toFindIndex, toFindFrom.Column(0), startIndex, searchIn.Length - startIndex, searchIn.Column(0), globalOffset, deleteVector);

                if (startIndex == -1)
                {
                    return -1;
                }

                bool found = true;
                for (int i = 1; i < toFindFrom.ColumnCount; i++)
                {
                    if (!_comparers[i].IsEqual(toFindIndex, toFindFrom.Column(i), startIndex, searchIn.Column(i)))
                    {
                        found = false;
                        break;
                    }
                }

                if (found)
                {
                    return startIndex;
                }
            }
        }

        /// <summary>
        /// Create a record batch comparer for the given schema.
        /// This allows for searching a specific row in a record batch
        /// </summary>
        /// <param name="schema"></param>
        /// <returns></returns>
        public static RecordBatchComparer Create(StructType schema)
        {
            IArrowComparer[] comparers = new IArrowComparer[schema.Fields.Count];

            var visitor = new ArrowComparerVisitor();

            for (int i = 0; i < schema.Fields.Count; i++)
            {
                comparers[i] = schema.Fields[i].Type.Accept(visitor);
            }

            return new RecordBatchComparer(comparers);
        }
    }
}
