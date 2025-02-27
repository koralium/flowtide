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

using FlowtideDotNet.Connector.DeltaLake.Internal.Delta.Actions;
using FlowtideDotNet.Connector.DeltaLake.Internal.Delta.Stats;
using FlowtideDotNet.Core.ColumnStore.TreeStorage;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FlowtideDotNet.Connector.DeltaLake.Internal.Delta
{
    internal class DeltaFile
    {
        public DeltaAddAction Action { get; set; }

        public DeltaStatistics Statistics { get; set; }

        public DeltaFile(DeltaAddAction action, DeltaStatistics statistics)
        {
            Action = action;
            Statistics = statistics;
        }

        public bool CanBeInFile(ColumnRowReference rowReference, List<string> columnNames)
        {
            if (Statistics.ValueComparers != null)
            {
                for (int i = 0; i < columnNames.Count; i++)
                {
                    if (Statistics.ValueComparers.TryGetValue(columnNames[i], out var comparer))
                    {
                        if (!comparer.IsInBetween(rowReference.referenceBatch.Columns[i].GetValueAt(rowReference.RowIndex, default)))
                        {
                            return false;
                        }
                    }
                }
            }
            return true;
        }
    }
}
