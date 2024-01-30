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

using FlowtideDotNet.Connector.SpiceDB.Internal;
using FlowtideDotNet.Core.Engine;
using FlowtideDotNet.Substrait.Relations;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading.Tasks;

namespace FlowtideDotNet.Connector.SpiceDB.Extensions
{
    public static class SpiceDbReadWriteFactoryExtensions
    {
        public static ReadWriteFactory AddSpiceDbSink(this ReadWriteFactory factory, string regexPattern, SpiceDbSinkOptions spiceDbSinkOptions, Action<WriteRelation>? transform = null)
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
                return new SpiceDbSink(spiceDbSinkOptions, writeRel, Core.Operators.Write.ExecutionMode.OnCheckpoint, opt);
            });
            return factory;
        }
    }
}
