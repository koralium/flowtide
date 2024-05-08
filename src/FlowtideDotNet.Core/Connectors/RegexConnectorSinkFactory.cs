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

using FlowtideDotNet.Base.Vertices.Egress;
using FlowtideDotNet.Core.Compute;
using FlowtideDotNet.Substrait.Relations;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;

namespace FlowtideDotNet.Core.Connectors
{
    public abstract class RegexConnectorSinkFactory : AbstractConnectorSinkFactory
    {
        private readonly string _regexPattern;
        protected RegexConnectorSinkFactory(string regexPattern)
        {
            if (regexPattern == "*")
            {
                regexPattern = ".*";
            }
            _regexPattern = regexPattern;
        }

        public override bool CanHandle(WriteRelation writeRelation)
        {
            var regexResult = Regex.Match(writeRelation.NamedObject.DotSeperated, _regexPattern, RegexOptions.IgnoreCase, TimeSpan.FromSeconds(5));
            return regexResult.Success;
        }
    }
}
