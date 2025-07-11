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

using Xunit.Sdk;
using Xunit.v3;

namespace FlowtideDotNet.ComputeTests.Internal.Framework
{
    internal class SubstraitTestCollection : ITestCollection
    {
        public SubstraitTestCollection(SubstraitTestAssembly testAssembly, string displayName)
        {
            TestAssembly = testAssembly;
            TestCollectionDisplayName = displayName;
            Traits = ExtensibilityPointFactory.GetCollectionTraits(testCollectionDefinition: null, TestAssembly.Traits);
            UniqueID = UniqueIDGenerator.ForTestCollection(testAssembly.UniqueID, displayName, collectionDefinitionClassName: null);
        }

        public SubstraitTestAssembly TestAssembly { get; }

        ITestAssembly ITestCollection.TestAssembly =>
            TestAssembly;

        public string? TestCollectionClassName =>
            null;

        public string TestCollectionDisplayName { get; }

        public IReadOnlyDictionary<string, IReadOnlyCollection<string>> Traits { get; }

        public string UniqueID { get; }
    }
}
