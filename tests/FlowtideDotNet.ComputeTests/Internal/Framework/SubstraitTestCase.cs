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
    internal class SubstraitTestCase : ITestCase, IXunitSerializable
    {
        SubstraitTestMethod? testMethod;
        private int lineNumber;
        private bool single;

        [EditorBrowsable(EditorBrowsableState.Never)]
        [Obsolete("Called by the de-serializer; should only be called by deriving classes for de-serialization purposes")]
        public SubstraitTestCase()
        { }

        /// <summary>
        /// Initializes a new instance of the <see cref="XunitTestCase"/> class.
        /// </summary>
        /// <param name="testMethod">The test method this test case belongs to.</param>
        /// <param name="order">The value from <see cref="ObservationAttribute.Order"/>.</param>
        public SubstraitTestCase(
            SubstraitTestMethod testMethod,
            string text,
            int lineNumber,
            int order,
            bool single)
        {
            this.testMethod = Guard.ArgumentNotNull(testMethod);
            Text = text;
            this.lineNumber = lineNumber;
            Order = order;
            this.single = single;
        }

        bool ITestCaseMetadata.Explicit =>
            false;

        public string? Text { get; private set; }
        public int Order { get; private set; }

        string? ITestCaseMetadata.SkipReason =>
            null;

        string? ITestCaseMetadata.SourceFilePath =>
            TestClass.FilePath;

        int? ITestCaseMetadata.SourceLineNumber =>
            lineNumber;

        public string TestCaseDisplayName
        {
            get
            {
                if (single)
                {
                    return TestMethod.DisplayName;
                }
                return Text!;
            }
        }

        public SubstraitTestClass TestClass =>
            TestMethod.TestClass;

        ITestClass? ITestCase.TestClass =>
            TestClass;

        int? ITestCaseMetadata.TestClassMetadataToken =>
            default;

        string? ITestCaseMetadata.TestClassName =>
            $"{TestClass.TestClassNamespace}.{TestClass.DisplayName}";

        string? ITestCaseMetadata.TestClassNamespace =>
            TestClass.TestClassNamespace;

        string? ITestCaseMetadata.TestClassSimpleName =>
            TestClass.TestClassSimpleName;

        public SubstraitTestCollection TestCollection =>
            TestMethod.TestClass.TestCollection;

        ITestCollection ITestCase.TestCollection =>
            TestCollection;

        public SubstraitTestMethod TestMethod =>
            testMethod ?? throw new InvalidOperationException($"Attempted to retrieve an uninitialized {nameof(SubstraitTestCase)}.{nameof(TestMethod)}");

        ITestMethod? ITestCase.TestMethod =>
            TestMethod;

        int? ITestCaseMetadata.TestMethodMetadataToken =>
            default;

        string? ITestCaseMetadata.TestMethodName =>
            TestMethod.MethodName;

        string[]? ITestCaseMetadata.TestMethodParameterTypesVSTest =>
            null;

        string? ITestCaseMetadata.TestMethodReturnTypeVSTest =>
            null;

        public IReadOnlyDictionary<string, IReadOnlyCollection<string>> Traits =>
            TestMethod.Traits;

        public string UniqueID =>
            UniqueIDGenerator.ForTestCase(TestMethod.UniqueID + Order, testMethodGenericTypes: null, testMethodArguments: null);

        public void Deserialize(IXunitSerializationInfo info)
        {
            testMethod = Guard.NotNull("Could not retrieve TestMethod from serialization", info.GetValue<SubstraitTestMethod>("tm"));
            Order = info.GetValue<int>("o");
            Text = info.GetValue<string>("text")!;
            lineNumber = info.GetValue<int>("lineNumber");
            single = info.GetValue<bool>("single");
        }

        public void Serialize(IXunitSerializationInfo info)
        {
            info.AddValue("o", Order);
            info.AddValue("tm", TestMethod);
            info.AddValue("text", Text);
            info.AddValue("lineNumber", lineNumber);
            info.AddValue("single", single);
        }
    }
}
