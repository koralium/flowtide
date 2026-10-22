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

using FlowtideDotNet.Connector.DeltaLake.Internal.Maintenance;
using System.Globalization;

namespace FlowtideDotNet.Connector.DeltaLake.Tests
{
    // The test host never runs this; ScratchDirectoryTests start the test assembly as a process that holds a scratch folder, and manual fixtures run through it
    public static class Program
    {
        public static async Task<int> Main(string[] args)
        {
            if (args.Length == 2 && args[0] == "hold-scratch")
            {
                var directory = new ScratchDirectory(args[1]);
                Console.WriteLine(directory.ProcessPath);
                Console.Out.Flush();
                // Holds the folder's lock until the parent closes standard input
                Console.In.ReadToEnd();
                GC.KeepAlive(directory);
                return 0;
            }
            // The first long advance of an upgraded table with more than 100,000 versions before adoption
            if (args.Length >= 2 && args[0] == "advance-fixture")
            {
                var versions = args.Length >= 3 && !args[2].StartsWith("--", StringComparison.Ordinal) ? long.Parse(args[2], CultureInfo.InvariantCulture) : 100_050;
                var describe = new Dictionary<string, string>()
                {
                    ["scratch"] = args[1],
                    ["profile"] = "local disk, unmetered, fake clock",
                    ["workload"] = $"seeded history of {versions} versions, adoption 40 below the head, one long plan",
                    ["offered-load"] = "one table"
                };
                return await ManualRun.RunAsync(args, describe, output => AdvancePlannerKit.RunFixtureAsync(args[1], versions, output), Console.Out);
            }
            Console.Error.WriteLine("usage: hold-scratch <directory> | advance-fixture <directory> [versions] [--commit <sha>]");
            return ManualRun.HarnessError;
        }
    }
}
