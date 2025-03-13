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

using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;
using Xunit.Internal;
using Xunit.Sdk;
using Xunit.v3;

namespace FlowtideDotNet.ComputeTests.Internal.Framework
{
    internal class SubstraitTestClass : ITestClass, IXunitSerializable
    {
        internal static BindingFlags MethodBindingFlags = BindingFlags.Instance | BindingFlags.Static | BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.FlattenHierarchy;

        SubstraitTestAssembly? testAssembly;
        readonly Lazy<SubstraitTestCollection> testCollection;
        readonly Lazy<string> uniqueID;
        private string? @namespace;
        private string? className;

        [EditorBrowsable(EditorBrowsableState.Never)]
        [Obsolete("Called by the de-serializer; should only be called by deriving classes for de-serialization purposes")]
        public SubstraitTestClass()
        {
            testCollection = new(() => new SubstraitTestCollection(TestAssembly, @namespace!));
            uniqueID = new(() => UniqueIDGenerator.ForTestClass(TestCollection.UniqueID, TestClassName));
        }

#pragma warning disable CS0618
        public SubstraitTestClass(
            SubstraitTestAssembly testAssembly,
            string @namespace,
            string className,
            string filePath,
            bool isScalar,
            string[] includeList) :
                this()
#pragma warning restore CS0618
        {
            this.testAssembly = Guard.ArgumentNotNull(testAssembly);
            this.@namespace = @namespace;
            this.className = className;
            FilePath = filePath;
            IsScalar = isScalar;
            IncludeList = includeList;
        }

        public string DisplayName =>
            className!;

        SubstraitTestAssembly TestAssembly =>
            testAssembly ?? throw new InvalidOperationException($"Attempted to retrieve an uninitialized {nameof(SubstraitTestClass)}.{nameof(TestAssembly)}");

        public string TestClassName =>
            className!;

        public string? TestClassNamespace =>
            @namespace;

        public string TestClassSimpleName =>
            className!;

        public SubstraitTestCollection TestCollection =>
            testCollection.Value;

        ITestCollection ITestClass.TestCollection =>
            testCollection.Value;

        public IReadOnlyDictionary<string, IReadOnlyCollection<string>> Traits =>
            new Dictionary<string, IReadOnlyCollection<string>>();

        public string UniqueID =>
            uniqueID.Value;

        public string? FilePath { get; private set; }
        public bool IsScalar { get; private set; }
        public string[]? IncludeList { get; private set; }

        public void Deserialize(IXunitSerializationInfo info)
        {
            testAssembly = Guard.NotNull("Could not retrieve TestAssembly from serialization", info.GetValue<SubstraitTestAssembly>("a"));
            var typeName = Guard.NotNull("Could not retrieve TestClassName from serialization", info.GetValue<string>("c"));
            className = Guard.NotNull("Could not retrieve className", info.GetValue<string>("c"));
            @namespace = Guard.NotNull("Could not retrieve namespace", info.GetValue<string>("n"));
            FilePath = Guard.NotNull("Could not retrieve FilePath", info.GetValue<string>("f"));
            IsScalar = Guard.NotNull<bool>("Could not retrieve isScalar", info.GetValue<bool>("sc"));
            IncludeList = Guard.NotNull("Could not retrieve includeList", info.GetValue<string[]>("il"));
        }

        public void Serialize(IXunitSerializationInfo info)
        {
            info.AddValue("a", TestAssembly);
            info.AddValue("c", className);
            info.AddValue("n", @namespace);
            info.AddValue("f", FilePath);
            info.AddValue("sc", IsScalar);
            info.AddValue("il", IncludeList!.ToArray());
        }
    }
}
