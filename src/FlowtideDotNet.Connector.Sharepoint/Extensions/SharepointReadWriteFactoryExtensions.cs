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

using FlowtideDotNet.Connector;
using FlowtideDotNet.Connector.Sharepoint;
using FlowtideDotNet.Connector.Sharepoint.Internal;
using FlowtideDotNet.Substrait.Relations;
using System.Text.RegularExpressions;

namespace FlowtideDotNet.Core.Engine
{
    public static class SharepointReadWriteFactoryExtensions
    {
        public static ReadWriteFactory AddSharepointListSink(this ReadWriteFactory factory, string regexPattern, Func<WriteRelation, SharepointSinkOptions> optionsFactory, Action<WriteRelation>? transform = null)
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
                var options = optionsFactory(writeRel);
                return new SharepointSink(options, writeRel, Core.Operators.Write.ExecutionMode.OnCheckpoint, opt);
            });
            return factory;
        }

        public static ReadWriteFactory AddSharepointListSource(this ReadWriteFactory factory, string regexPattern, Func<ReadRelation, SharepointSourceOptions> optionsFactory, Action<ReadRelation>? transform = null)
        {
            if (regexPattern == "*")
            {
                regexPattern = ".*";
            }
            factory.AddReadResolver((readRel, functionRegister, opt) =>
            {
                var regexResult = Regex.Match(readRel.NamedTable.DotSeperated, regexPattern, RegexOptions.IgnoreCase, TimeSpan.FromSeconds(5));
                if (!regexResult.Success)
                {
                    return null;
                }
                transform?.Invoke(readRel);
                var options = optionsFactory(readRel);
                return new ReadOperatorInfo(new SharepointSource(options, readRel, opt));
            });
            return factory;
        }
    }
}
