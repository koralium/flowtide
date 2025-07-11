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

using SqlParser;
using SqlParser.Ast;

namespace FlowtideDotNet.Substrait.Sql.Internal
{
    internal static class TableOptionExtractor
    {
        public static IReadOnlyDictionary<string, string> ReadOptions(Sequence<Expression> expressions)
        {

            var options = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
            foreach (var expr in expressions)
            {
                if (expr is Expression.BinaryOp binaryOp)
                {
                    var left = binaryOp.Left.ToSql();
                    var right = binaryOp.Right.ToSql();
                    options[left] = right;
                }
            }

            return options;
        }
    }
}
