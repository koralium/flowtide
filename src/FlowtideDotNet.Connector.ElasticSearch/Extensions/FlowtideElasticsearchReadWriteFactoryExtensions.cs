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

using FlowtideDotNet.Connector.ElasticSearch;
using FlowtideDotNet.Connector.ElasticSearch.Internal;
using FlowtideDotNet.Substrait.Relations;
using Nest;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading.Tasks;

namespace FlowtideDotNet.Core.Engine
{
    public static class FlowtideElasticsearchReadWriteFactoryExtensions
    {
        public static ReadWriteFactory AddElasticsearchSink(
            this ReadWriteFactory factory, 
            string regexPattern, 
            ConnectionSettings options, 
            Action<IProperties>? customMappings = null, 
            Action<WriteRelation>? transform = null)
        {
            if (regexPattern == "*")
            {
                regexPattern = ".*";
            }
            factory.AddWriteResolver((writeRel, opt) =>
            {
                var regexResult = Regex.Match(writeRel.NamedObject.DotSeperated, regexPattern, RegexOptions.IgnoreCase);
                if (!regexResult.Success)
                {
                    return null;
                }
                transform?.Invoke(writeRel);

                FlowtideElasticsearchOptions flowtideElasticsearchOptions = new FlowtideElasticsearchOptions()
                {
                    ConnectionSettings = options,
                    CustomMappings = customMappings
                };

                return new ElasticSearchSink(writeRel, flowtideElasticsearchOptions, Operators.Write.ExecutionMode.OnCheckpoint, opt);
            });
            return factory;
        }
    }
}
