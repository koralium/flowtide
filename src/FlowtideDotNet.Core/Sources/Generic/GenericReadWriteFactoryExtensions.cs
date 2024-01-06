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

using FlowtideDotNet.Core.Sources.Generic;
using FlowtideDotNet.Core.Sources.Generic.Internal;
using System.Text.RegularExpressions;

namespace FlowtideDotNet.Core.Engine
{
    public static class GenericReadWriteFactoryExtensions
    {
        public static ReadWriteFactory AddGenericDataSource<T>(this ReadWriteFactory readWriteFactory, string regexPattern, GenericDataSourceAsync<T> dataSource)
            where T: class
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

                return new ReadOperatorInfo(new GenericReadOperator<T>(readRelation, dataSource, opt));
            });

            return readWriteFactory;
        }
    }
}
