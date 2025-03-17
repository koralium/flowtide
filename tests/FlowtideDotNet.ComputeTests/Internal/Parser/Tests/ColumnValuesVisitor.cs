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

using Antlr4.Runtime.Misc;
using FlowtideDotNet.Core.ColumnStore;
using FlowtideDotNet.Storage.Memory;

namespace FlowtideDotNet.ComputeTests.SourceGenerator.Internal.Tests
{
    internal class ColumnValuesVisitor : FuncTestCaseParserBaseVisitor<Column>
    {
        private readonly string dataType;

        public ColumnValuesVisitor(string dataType)
        {
            this.dataType = dataType;
        }

        public override Column VisitColumnValues([NotNull] FuncTestCaseParser.ColumnValuesContext context)
        {
            var visitor = new ColumnValueVisitor(dataType);

            
            Column result = new Column(GlobalMemoryManager.Instance);
            foreach(var val in context.literal())
            {
                result.Add(visitor.Visit(val));
            }
            return result;
        }
    }

    internal class ColumnValueVisitor : FuncTestCaseParserBaseVisitor<IDataValue>
    {
        private readonly string dataType;

        public ColumnValueVisitor(string dataType)
        {
            this.dataType = dataType;
        }

        public override IDataValue VisitLiteral([NotNull] FuncTestCaseParser.LiteralContext context)
        {
            return DataTypeValueWriter.WriteDataValue(context.GetText(), dataType);
        }
    }
}
