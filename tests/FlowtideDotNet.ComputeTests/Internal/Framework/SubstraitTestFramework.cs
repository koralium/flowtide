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

using System.Reflection;
using Xunit.v3;

namespace FlowtideDotNet.ComputeTests.Internal.Framework
{
    internal class SubstraitTestFramework : TestFramework
    {
        public override string TestFrameworkDisplayName => "Substrait Compute Framework";

        protected override ITestFrameworkDiscoverer CreateDiscoverer(Assembly assembly)
        {
            return new SubstraitDiscoverer(new SubstraitTestAssembly(assembly));
        }

        protected override ITestFrameworkExecutor CreateExecutor(Assembly assembly)
        {
            return new SubstraitTestExecutor(new SubstraitTestAssembly(assembly));
        }
    }
}
