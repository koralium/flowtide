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

using FlowtideDotNet.Core.Operators.Write;
using FlowtideDotNet.Core.Sources.Generic;
using FlowtideDotNet.Core.Sources.Generic.Internal;
using FlowtideDotNet.Substrait.Relations;
using System.Text.RegularExpressions;

namespace FlowtideDotNet.Core.Engine
{
    public static class GenericReadWriteFactoryExtensions
    {
        public static ReadWriteFactory AddGenericDataSource<T>(this ReadWriteFactory readWriteFactory, string regexPattern, Func<ReadRelation, GenericDataSourceAsync<T>> dataSource)
            where T: class
        {
            if (regexPattern == "*")
            {
                regexPattern = ".*";
            }

            readWriteFactory.AddReadResolver((readRelation, functionsRegister, opt) =>
            {
                var regexResult = Regex.Match(readRelation.NamedTable.DotSeperated, regexPattern, RegexOptions.IgnoreCase, TimeSpan.FromSeconds(5));
                if (!regexResult.Success)
                {
                    return null;
                }

                return new ReadOperatorInfo(new GenericReadOperator<T>(readRelation, dataSource(readRelation), functionsRegister, opt));
            });

            return readWriteFactory;
        }

        public static ReadWriteFactory AddGenericDataSink<T>(this ReadWriteFactory readWriteFactory, string regexPattern, ExecutionMode executionMode, Func<WriteRelation, GenericDataSink<T>> dataSink)
            where T: class
        {
            if (regexPattern == "*")
            {
                regexPattern = ".*";
            }

            readWriteFactory.AddWriteResolver((writeRelation, opt) =>
            {
                var regexResult = Regex.Match(writeRelation.NamedObject.DotSeperated, regexPattern, RegexOptions.IgnoreCase, TimeSpan.FromSeconds(5));
                if (!regexResult.Success)
                {
                    return null;
                }

                return new GenericWriteOperator<T>(dataSink(writeRelation), writeRelation, executionMode, opt);
            });

            return readWriteFactory;
        }
    }
}
