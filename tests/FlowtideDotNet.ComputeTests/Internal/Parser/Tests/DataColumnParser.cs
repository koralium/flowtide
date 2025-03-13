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
using System.Text;

namespace FlowtideDotNet.ComputeTests.SourceGenerator.Internal.Tests
{
    internal static class DataColumnParser
    {
        public static EventBatchData ParseDataColumn(FuncTestCaseParser.DataColumnContext context)
        {
            var dataType = context.dataType();

            var col = new ColumnValuesVisitor(dataType.GetText()).Visit(context.columnValues());

            var batch = new EventBatchData([col]);

            return batch;
        }
    }
}
