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

using FlowtideDotNet.ComputeTests;
using Xunit.Sdk;
using Xunit.v3;

namespace FlowtideDotNet.ComputeTests.Internal.Framework
{
    internal class SubstraitDiscoverer : TestFrameworkDiscoverer<SubstraitTestClass>
    {
        public SubstraitDiscoverer(SubstraitTestAssembly testAssembly) : base(testAssembly)
        {
            TestAssembly = testAssembly;
        }

        public new SubstraitTestAssembly TestAssembly { get; }

        /// <summary>
        /// Tries and remove debug or release folder information in the file location
        /// </summary>
        /// <param name="fileLocation"></param>
        /// <returns></returns>
        private string FixFileLocation(string fileLocation)
        {
            if (fileLocation.Contains("bin/Debug/net"))
            {
                // Remove bin/debug/net{framework} from the file location, keep the start of the path as well
                var debugStart = fileLocation.IndexOf("bin/Debug/net");
                var debugEnd = fileLocation.IndexOf("/", debugStart + "bin\\Debug\\net".Length);
                if (debugEnd == -1)
                {
                    debugEnd = fileLocation.IndexOf("\\", debugStart + "bin\\Debug\\net".Length);
                }

                fileLocation = fileLocation.Substring(0, debugStart) + fileLocation.Substring(debugEnd + 1);
            }
            else if (fileLocation.Contains("bin\\Debug\\net"))
            {
                // Remove bin/debug/net{framework} from the file location, keep the start of the path as well
                var debugStart = fileLocation.IndexOf("bin\\Debug\\net");
                var debugEnd = fileLocation.IndexOf("\\", debugStart + "bin\\Debug\\net".Length);
                if (debugEnd == -1)
                {
                    debugEnd = fileLocation.IndexOf("/", debugStart + "bin\\Debug\\net".Length);
                }

                fileLocation = fileLocation.Substring(0, debugStart) + fileLocation.Substring(debugEnd + 1);
            }
            else if (fileLocation.Contains("bin/Release/net"))
            {
                // Remove bin/Release/net{framework} from the file location, keep the start of the path as well
                var debugStart = fileLocation.IndexOf("bin/Release/net");
                var debugEnd = fileLocation.IndexOf("/", debugStart + "bin\\Release\\net".Length);
                if (debugEnd == -1)
                {
                    debugEnd = fileLocation.IndexOf("\\", debugStart + "bin\\Release\\net".Length);
                }

                fileLocation = fileLocation.Substring(0, debugStart) + fileLocation.Substring(debugEnd + 1);
            }
            else if (fileLocation.Contains("bin\\Release\\net"))
            {
                // Remove bin/Release/net{framework} from the file location, keep the start of the path as well
                var debugStart = fileLocation.IndexOf("bin\\Release\\net");
                var debugEnd = fileLocation.IndexOf("\\", debugStart + "bin\\Release\\net".Length);
                if (debugEnd == -1)
                {
                    debugEnd = fileLocation.IndexOf("/", debugStart + "bin\\Release\\net".Length);
                }

                fileLocation = fileLocation.Substring(0, debugStart) + fileLocation.Substring(debugEnd + 1);
            }
            return fileLocation;
        }

        public override async ValueTask Find(Func<ITestCase, ValueTask<bool>> callback, ITestFrameworkDiscoveryOptions discoveryOptions, Type[]? types = null, CancellationToken? cancellationToken = null)
        {
            var resourceNames = TestAssembly.Assembly.GetManifestResourceNames();

            foreach(var embeddedResourceName in resourceNames)
            {
                if (!embeddedResourceName.EndsWith(".test"))
                {
                    continue;
                }
                var withoutTestName = embeddedResourceName.Substring(0, embeddedResourceName.Length - 5);
                var fileNameStart = withoutTestName.LastIndexOf('.') + 1;
                var fileName = withoutTestName.Substring(fileNameStart) + ".test";
                var embeddedFolderName = withoutTestName.Substring(0, fileNameStart - 1).Replace("FlowtideDotNet.ComputeTests.", "").Replace(".", "/");
                using var stream = TestAssembly.Assembly.GetManifestResourceStream(embeddedResourceName);
                using var streamReader = new StreamReader(stream!);
                var txt = streamReader.ReadToEnd();
                var className = Path.GetFileNameWithoutExtension(fileName);

                // Get the last folder name that the file is in
                var folderName = Path.GetFileName(Path.GetDirectoryName(embeddedFolderName + "/"));

                
                var doc = new TestCaseParser().Parse(txt);

                var fullPathBase = Path.GetDirectoryName(TestAssembly.AssemblyPath!);
                
                // remove bin debug, release info from the file location
                var fileLocation = FixFileLocation($"{fullPathBase}/{embeddedFolderName}/{fileName}");

                var testClass = new SubstraitTestClass(TestAssembly, folderName!, className, fileLocation, doc.IsScalar, doc.Include!.ToArray());
                foreach (var d in doc.TestGroups!)
                {

                    var method = new SubstraitTestMethod(
                        testClass, d.Description);

                    int order = 0;
                    foreach (var c in d.TestCases)
                    {
                        var testCase = new SubstraitTestCase(method, c.Text!, c.LineNumber, order++, d.TestCases.Count == 1);
                        await callback(testCase);
                    }
                }
            }
        }

        protected override ValueTask<SubstraitTestClass> CreateTestClass(Type @class)
        {
            return new(new SubstraitTestClass(TestAssembly, "test", "test", "", true, []));
        }

        protected override ValueTask<bool> FindTestsForType(SubstraitTestClass testClass, ITestFrameworkDiscoveryOptions discoveryOptions, Func<ITestCase, ValueTask<bool>> discoveryCallback)
        {
            throw new NotImplementedException();
        }

        protected override Type[] GetExportedTypes()
        {
            return TestAssembly.Assembly.ExportedTypes.ToArray();
        }
    }
}
