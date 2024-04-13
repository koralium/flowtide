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

using FlowtideDotNet.Core.Engine;
using FlowtideDotNet.Substrait.Type;
using System.Text.RegularExpressions;

namespace FlowtideDotNet.AcceptanceTests.Internal
{
    public static class ReadWriteFactoryExtensions
    {
        public static ReadWriteFactory AddMockSource(this ReadWriteFactory readWriteFactory, string regexPattern, MockDatabase mockDatabase)
        {
            if (regexPattern == "*")
            {
                regexPattern = ".*";
            }

            return readWriteFactory.AddReadResolver((readRel, functionsRegister, opt) =>
            {
                var regexResult = Regex.Match(readRel.NamedTable.DotSeperated, regexPattern, RegexOptions.IgnoreCase);
                if (!regexResult.Success)
                {
                    return null;
                }

                var table = mockDatabase.GetTable(readRel.NamedTable.DotSeperated);

                List<int> pks = new List<int>();
                foreach (var primaryKeyIndex in table.PrimaryKeyIndices)
                {
                    var pkIndex = readRel.BaseSchema.Names.IndexOf(table.Columns[primaryKeyIndex]);
                    if (pkIndex >= 0)
                    {
                        pks.Add(pkIndex);
                    }
                    else
                    {
                        readRel.BaseSchema.Names.Add(table.Columns[primaryKeyIndex]);
                        readRel.BaseSchema.Struct.Types.Add(new AnyType());
                        pks.Add(readRel.BaseSchema.Names.Count - 1);
                    }
                }

                return new ReadOperatorInfo(new MockDataSourceOperator(readRel, mockDatabase, opt), new Substrait.Relations.NormalizationRelation()
                {
                    Emit = readRel.Emit,
                    Filter = readRel.Filter,
                    Input = readRel,
                    KeyIndex = pks
                });
            });
        }
    }
}
