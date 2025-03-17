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
using System.Text.RegularExpressions;

namespace FlowtideDotNet.Core.Engine
{
    public static class FlowtideElasticsearchReadWriteFactoryExtensions
    {
        public static ReadWriteFactory AddElasticsearchSink(
            this ReadWriteFactory factory, 
            string regexPattern, 
            FlowtideElasticsearchOptions options, 
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

                var sink = new ColumnElasticSearchSink(options, options.ExecutionMode, writeRel, opt);
                sink.CreateIndexAndMappings().GetAwaiter().GetResult();
                return sink;
            });
            return factory;
        }
    }
}
