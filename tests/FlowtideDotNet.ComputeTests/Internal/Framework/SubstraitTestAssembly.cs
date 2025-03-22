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
using Xunit.Internal;
using Xunit.Sdk;
using Xunit.v3;

namespace FlowtideDotNet.ComputeTests.Internal.Framework
{
    internal class SubstraitTestAssembly : ITestAssembly, IXunitSerializable
    {
        Assembly? assembly;
        readonly Lazy<string> assemblyName;
        readonly Lazy<IReadOnlyDictionary<string, IReadOnlyCollection<string>>> traits;
        readonly Lazy<string> uniqueID;

        [Obsolete("Called by the de-serializer; should only be called by deriving classes for de-serialization purposes")]
        public SubstraitTestAssembly()
        {
            assemblyName = new(() => Assembly.GetName().FullName);
            traits = new(() => ExtensibilityPointFactory.GetAssemblyTraits(Assembly));
            uniqueID = new(() => UniqueIDGenerator.ForAssembly(Assembly.Location, ConfigFilePath));
        }

#pragma warning disable CS0618
        public SubstraitTestAssembly(
            Assembly assembly,
            string? configFilePath = null) :
                this()
#pragma warning restore CS0618
        {
            Guard.ArgumentNotNull(assembly);

            this.assembly = assembly;
            ConfigFilePath = configFilePath;
        }

        public Assembly Assembly =>
        assembly ?? throw new InvalidOperationException($"Attempted to retrieve an uninitialized {nameof(SubstraitTestAssembly)}.{nameof(Assembly)}");

        public string AssemblyName =>
            assemblyName.Value;

        public string AssemblyPath =>
            Assembly.Location;

        public string? ConfigFilePath { get; private set; }

        public Guid ModuleVersionID =>
            Assembly.Modules.First().ModuleVersionId;

        public IReadOnlyDictionary<string, IReadOnlyCollection<string>> Traits =>
            traits.Value;

        public string UniqueID =>
             uniqueID.Value;

        public void Deserialize(IXunitSerializationInfo info)
        {
            ConfigFilePath = info.GetValue<string>("c");

            var assemblyPath = Guard.NotNull("Could not retrieve AssemblyPath from serialization", info.GetValue<string>("a"));
            assembly = Guard.NotNull(() => $"Could not load assembly {assemblyPath}", Assembly.LoadFrom(assemblyPath));
        }

        public void Serialize(IXunitSerializationInfo info)
        {
            info.AddValue("a", AssemblyPath);
            info.AddValue("c", ConfigFilePath);
        }
    }
}
