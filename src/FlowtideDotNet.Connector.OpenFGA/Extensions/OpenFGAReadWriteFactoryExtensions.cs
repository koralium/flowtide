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

using FlowtideDotNet.Connector.OpenFGA.Internal;
using FlowtideDotNet.Core.Engine;
using FlowtideDotNet.Core.Operators.Write;
using FlowtideDotNet.Substrait.Relations;
using FlowtideDotNet.Substrait.Type;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading.Tasks;

namespace FlowtideDotNet.Connector.OpenFGA.Extensions
{
    public static class OpenFGAReadWriteFactoryExtensions
    {
        public static ReadWriteFactory AddOpenFGASink(
            this ReadWriteFactory factory,
            string regexPattern,
            OpenFGASinkOptions options,
            Action<WriteRelation>? transform = null)
        {
            if (regexPattern == "*")
            {
                regexPattern = ".*";
            }
            factory.AddWriteResolver((writeRel, opt) =>
            {
                var regexResult = Regex.Match(writeRel.NamedObject.DotSeperated, regexPattern, RegexOptions.IgnoreCase, TimeSpan.FromSeconds(5));
                if (!regexResult.Success)
                {
                    return null;
                }
                transform?.Invoke(writeRel);

                var sink = new FlowtideOpenFGASink(options, writeRel, ExecutionMode.OnCheckpoint, opt);
                return sink;
            });
            return factory;
        }

        public static ReadWriteFactory AddOpenFGASource(this ReadWriteFactory readWriteFactory, string regexPattern, OpenFGASourceOptions options)
        {
            if (regexPattern == "*")
            {
                regexPattern = ".*";
            }

            readWriteFactory.AddReadResolver((readRelation, opt) =>
            {
                var regexResult = Regex.Match(readRelation.NamedTable.DotSeperated, regexPattern, RegexOptions.IgnoreCase, TimeSpan.FromSeconds(5));
                if (!regexResult.Success)
                {
                    return null;
                }

                // Make sure that all primary key columns are in the query, if not add them.
                List<int> indices = new List<int>();
                var userIndex = readRelation.BaseSchema.Names.FindIndex(x => x.Equals("user", StringComparison.OrdinalIgnoreCase));
                if (userIndex < 0)
                {
                    readRelation.BaseSchema.Names.Add("user");
                    readRelation.BaseSchema.Struct!.Types.Add(new AnyType() { Nullable = false });
                    userIndex = readRelation.BaseSchema.Names.Count - 1;
                }
                indices.Add(userIndex);

                var relationIndex = readRelation.BaseSchema.Names.FindIndex(x => x.Equals("relation", StringComparison.OrdinalIgnoreCase));
                if (relationIndex < 0)
                {
                    readRelation.BaseSchema.Names.Add("relation");
                    readRelation.BaseSchema.Struct!.Types.Add(new AnyType() { Nullable = false });
                    relationIndex = readRelation.BaseSchema.Names.Count - 1;
                }
                indices.Add(relationIndex);

                var objectIndex = readRelation.BaseSchema.Names.FindIndex(x => x.Equals("object", StringComparison.OrdinalIgnoreCase));
                if (objectIndex < 0)
                {
                    readRelation.BaseSchema.Names.Add("object");
                    readRelation.BaseSchema.Struct!.Types.Add(new AnyType() { Nullable = false });
                    objectIndex = readRelation.BaseSchema.Names.Count - 1;
                }
                indices.Add(objectIndex);

                return new ReadOperatorInfo(new FlowtideOpenFGASource(options, readRelation, opt), new NormalizationRelation()
                {
                    Input = readRelation,
                    Filter = readRelation.Filter,
                    KeyIndex = indices,
                    Emit = readRelation.Emit
                });
            });
            return readWriteFactory;
        }
    }
}
