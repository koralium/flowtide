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

using FlowtideDotNet.Substrait.Relations;
using FlowtideDotNet.Core.Sinks;
using System.Text.RegularExpressions;
using FlowtideDotNet.Core.Sinks.Blackhole;

namespace FlowtideDotNet.Core.Engine
{
    public static class ReadWriteFactoryExtensions
    {
        public static ReadWriteFactory AddConsoleSink(this ReadWriteFactory readWriteFactory, string regexPattern, Action<WriteRelation>? transform = null)
        {
            readWriteFactory.AddWriteResolver((relation, opt) =>
            {
                var regexResult = Regex.Match(relation.NamedObject.DotSeperated, regexPattern, RegexOptions.IgnoreCase, TimeSpan.FromSeconds(5));
                if (!regexResult.Success)
                {
                    return null;
                }

                return new ConsoleSink(relation, opt);
            });

            return readWriteFactory;
        }

        public static ReadWriteFactory AddBlackholeSink(this ReadWriteFactory readWriteFactory, string regexPattern)
        {
            if (regexPattern == "*")
            {
                regexPattern = ".*";
            }
            readWriteFactory.AddWriteResolver((relation, opt) =>
            {
                var regexResult = Regex.Match(relation.NamedObject.DotSeperated, regexPattern, RegexOptions.IgnoreCase, TimeSpan.FromSeconds(5));
                if (!regexResult.Success)
                {
                    return null;
                }

                return new BlackholeSink(opt);
            });

            return readWriteFactory;
        }
    }
}
