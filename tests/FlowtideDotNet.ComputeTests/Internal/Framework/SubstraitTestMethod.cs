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

using System.ComponentModel;
using Xunit.Internal;
using Xunit.Sdk;

namespace FlowtideDotNet.ComputeTests.Internal.Framework
{
    internal class SubstraitTestMethod : ITestMethod, IXunitSerializable
    {
        SubstraitTestClass? testClass;
        readonly Lazy<string> uniqueID;
        private string? methodName;

        [EditorBrowsable(EditorBrowsableState.Never)]
        [Obsolete("Called by the de-serializer; should only be called by deriving classes for de-serialization purposes")]
        public SubstraitTestMethod()
        {
            uniqueID = new(() => UniqueIDGenerator.ForTestMethod(TestClass.UniqueID, MethodName));
        }

#pragma warning disable CS0618
        public SubstraitTestMethod(
            SubstraitTestClass testClass,
            string methodName) :
                this()
#pragma warning restore CS0618
        {
            this.testClass = Guard.ArgumentNotNull(testClass);
            this.methodName = methodName;
        }

        public string DisplayName =>
            methodName!;

        public string MethodName =>
            methodName!;

        public SubstraitTestClass TestClass =>
            testClass ?? throw new InvalidOperationException($"Attempted to retrieve an uninitialized {nameof(SubstraitTestMethod)}.{nameof(TestClass)}");

        ITestClass ITestMethod.TestClass =>
            TestClass;

        public IReadOnlyDictionary<string, IReadOnlyCollection<string>> Traits =>
            new Dictionary<string, IReadOnlyCollection<string>>();

        public string UniqueID =>
            uniqueID.Value;

        public void Deserialize(IXunitSerializationInfo info)
        {
            testClass = Guard.NotNull("Could not retrieve TestClass from serialization", info.GetValue<SubstraitTestClass>("c"));
            methodName = Guard.NotNull("Could not retrieve MethodName from serialization", info.GetValue<string>("n"));
        }

        public void Serialize(IXunitSerializationInfo info)
        {
            info.AddValue("c", TestClass);
            info.AddValue("n", methodName);
        }
    }
}
