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
using FlowtideDotNet.Core.ColumnStore.Comparers;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FlowtideDotNet.AcceptanceTests.Internal
{
    internal static class EventBatchAssertion
    {
        public static void Equal(EventBatchData expected, EventBatchData actual)
        {
            if (expected.Count != actual.Count)
            {
                Assert.Fail($"EventBatchData count mismatch: {expected.Count} != {actual.Count}");
            }

            if (expected.Columns.Count != actual.Columns.Count)
            {
                Assert.Fail($"EventBatchData column count mismatch: {expected.Columns.Count} != {actual.Columns.Count}");
            }

            var comparer = new DataValueComparer();

            for (int i= 0; i < expected.Count; i++)
            {
                for (int c = 0; c < expected.Columns.Count; c++)
                {
                    if (expected.Columns[c].GetTypeAt(i, default) != actual.Columns[c].GetTypeAt(i, default))
                    {
                        Assert.Fail($"EventBatchData column type mismatch at index {i}, expected: {expected.Columns[c].GetTypeAt(i, default)}, actual: {actual.Columns[c].GetTypeAt(i, default)}");
                    }

                    var compareVal = comparer.Compare(expected.Columns[c].GetValueAt(i, default), actual.Columns[c].GetValueAt(i, default));

                    if (compareVal != 0)
                    {
                        Assert.Fail($"EventBatchData value mismatch at index {i}: {expected.Columns[c].GetValueAt(i, default)} != {actual.Columns[c].GetValueAt(i, default)}");
                    }
                }
            }
        }
    }
}
