﻿// Licensed under the Apache License, Version 2.0 (the "License")
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

using FlowtideDotNet.SqlServer.SqlServer;
using FlowtideDotNet.Substrait.Relations;
using FlowtideDotNet.Substrait.Tests.SqlServer;
using FlowtideDotNet.Substrait.Type;
using System.Text.RegularExpressions;

namespace FlowtideDotNet.Core.Engine
{
    public static class ReadWriteFactoryExtensions
    {
        public static ReadWriteFactory AddSqlServerSource(this ReadWriteFactory readWriteFactory, string regexPattern, Func<string> connectionStringFunc, Action<ReadRelation>? transform = null)
        {
            if (regexPattern == "*")
            {
                regexPattern = ".*";
            }
            readWriteFactory.AddReadResolver((relation, dataflowopt) =>
            {
                var regexResult = Regex.Match(relation.NamedTable.DotSeperated, regexPattern, RegexOptions.IgnoreCase);
                if (!regexResult.Success)
                {
                    return null;
                }
                transform?.Invoke(relation);

                var source = new SqlServerDataSource(connectionStringFunc, relation, dataflowopt);
                var primaryKeys = source.GetPrimaryKeys();

                List<int> pkIndices = new List<int>();
                foreach(var pk in primaryKeys)
                {
                    var pkIndex = relation.BaseSchema.Names.FindIndex((s) => s.Equals(pk, StringComparison.OrdinalIgnoreCase));
                    if (pkIndex == -1)
                    {
                        relation.BaseSchema.Names.Add(pk);
                        relation.BaseSchema.Struct.Types.Add(new AnyType() { Nullable = false });
                        pkIndices.Add(relation.BaseSchema.Names.Count - 1);
                    }
                    else
                    {
                        pkIndices.Add(pkIndex);
                    }
                }

                return new ReadOperatorInfo(source, new NormalizationRelation()
                {
                    Input = relation,
                    Filter = relation.Filter,
                    KeyIndex = pkIndices,
                    Emit  = relation.Emit
                });
            });
            return readWriteFactory;
        }

        public static ReadWriteFactory AddSqlServerSink(this ReadWriteFactory readWriteFactory, string regexPattern, Func<string> connectionStringFunc, Action<WriteRelation>? transform = null)
        {
            if (regexPattern == "*")
            {
                regexPattern = ".*";
            }

            readWriteFactory.AddWriteResolver((relation, dataflowopts) =>
            {
                var regexResult = Regex.Match(relation.NamedObject.DotSeperated, regexPattern, RegexOptions.IgnoreCase);
                if (!regexResult.Success)
                {
                    return null;
                }
                transform?.Invoke(relation);
                return new SqlServerSink(connectionStringFunc, relation, dataflowopts);
            });
            return readWriteFactory;
        }
    }
}
