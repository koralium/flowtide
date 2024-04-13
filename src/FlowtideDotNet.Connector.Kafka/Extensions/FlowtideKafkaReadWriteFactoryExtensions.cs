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

using FlowtideDotNet.Connector.Kafka;
using FlowtideDotNet.Connector.Kafka.Internal;
using FlowtideDotNet.Core.Operators.Write;
using FlowtideDotNet.Substrait.Relations;
using FlowtideDotNet.Substrait.Type;
using System.Text.RegularExpressions;

namespace FlowtideDotNet.Core.Engine
{
    public static class FlowtideKafkaReadWriteFactoryExtensions
    {
        /// <summary>
        /// Add a kafka source
        /// </summary>
        /// <param name="readWriteFactory"></param>
        /// <param name="regexPattern">Pattern to match on table names</param>
        /// <param name="options">Options for this source</param>
        /// <returns></returns>
        public static ReadWriteFactory AddKafkaSource(this ReadWriteFactory readWriteFactory, string regexPattern, FlowtideKafkaSourceOptions options)
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
                int keyIndex = -1;
                for (int i = 0; i < readRelation.BaseSchema.Names.Count; i++)
                {
                    if (readRelation.BaseSchema.Names[i] == "_key")
                    {
                        keyIndex = i;
                        break;
                    }
                }

                if (keyIndex == -1)
                {
                    readRelation.BaseSchema.Names.Add("_key");
                    readRelation.BaseSchema.Struct.Types.Add(new AnyType() { Nullable = false });
                    keyIndex = readRelation.BaseSchema.Names.Count - 1;
                }

                return new ReadOperatorInfo(new KafkaDataSource(readRelation, options, opt), new NormalizationRelation()
                {
                    Input = readRelation,
                    Filter = readRelation.Filter,
                    KeyIndex = new List<int>() { keyIndex },
                    Emit = readRelation.Emit
                });
            });
            return readWriteFactory;
        }

        public static ReadWriteFactory AddKafkaSink(this ReadWriteFactory readWriteFactory, string regexPattern, FlowtideKafkaSinkOptions options, ExecutionMode executionMode = ExecutionMode.OnCheckpoint)
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
                return new KafkaSink(writeRelation, options, executionMode, opt);
            });
            return readWriteFactory;
        }
    }
}
