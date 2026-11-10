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
            // Compaction fixtures and measured runs, one per process
            if (args.Length >= 1 && (args[0] == "compaction-fixture" || args[0] == "compaction-probe"))
            {
                var options = new Dictionary<string, string>(StringComparer.Ordinal);
                for (int i = 1; i + 1 < args.Length; i += 2)
                {
                    options[args[i].TrimStart('-')] = args[i + 1];
                }
                var describe = new Dictionary<string, string>()
                {
                    ["scratch"] = args[0] == "compaction-fixture" ? options.GetValueOrDefault("root", "") : options.GetValueOrDefault("scratch", Path.GetTempPath()),
                    ["profile"] = options.GetValueOrDefault("store", "local") + ", probe clock"
                };
                // Knobs that change a probe table, kept with the run
                foreach (var name in new[] { "FLOWTIDE_PROBE_READAHEAD", "FLOWTIDE_PROBE_PREFILTER", "FLOWTIDE_PROBE_THRESHOLD", "FLOWTIDE_PROBE_PRUNING" })
                {
                    describe[name] = Environment.GetEnvironmentVariable(name) ?? "unset";
                }
                return await ManualRun.RunAsync(args, describe, output => args[0] == "compaction-fixture" ? CompactionProbe.BuildAsync(options, output) : CompactionProbe.RunAsync(options, output), Console.Out);
            }
            // A10's group-at-a-time read audit, one process per run
            if (args.Length >= 1 && args[0] == "admission-audit")
            {
                var options = new Dictionary<string, string>(StringComparer.Ordinal);
                for (int i = 1; i + 1 < args.Length; i += 2)
                {
                    options[args[i].TrimStart('-')] = args[i + 1];
                }
                var describe = new Dictionary<string, string>() { ["scratch"] = options.GetValueOrDefault("scratch", Path.GetTempPath()), ["profile"] = "local disk, system pool" };
                return await ManualRun.RunAsync(args, describe, output => AdmissionAudit.RunAsync(options, output), Console.Out);
            }
            // P-admit: bins copied in a fresh process, memory and copy time
            if (args.Length >= 1 && args[0] == "compaction-audit")
            {
                var options = new Dictionary<string, string>(StringComparer.Ordinal);
                for (int i = 1; i + 1 < args.Length; i += 2)
                {
                    options[args[i].TrimStart('-')] = args[i + 1];
                }
                var describe = new Dictionary<string, string>() { ["scratch"] = options["scratch"], ["profile"] = "local disk, system pool" };
                return await ManualRun.RunAsync(args, describe, output => CompactionAudit.RunAsync(options, output), Console.Out);
            }
            // Pass 1's admission over fixture files, for the positive gate
            if (args.Length >= 1 && args[0] == "admission-gate")
            {
                var options = new Dictionary<string, string>(StringComparer.Ordinal);
                for (int i = 1; i + 1 < args.Length; i += 2)
                {
                    options[args[i].TrimStart('-')] = args[i + 1];
                }
                var describe = new Dictionary<string, string>() { ["scratch"] = options.GetValueOrDefault("scratch", Path.GetTempPath()), ["profile"] = "managed admission only" };
                return await ManualRun.RunAsync(args, describe, output => AdmissionAudit.GateAsync(options, output), Console.Out);
            }
            Console.Error.WriteLine("usage: hold-scratch <directory> | advance-fixture <directory> [versions] [--commit <sha>] | compaction-fixture --root <dir> --fixture <name> | compaction-probe --root <dir> --fixture <name> [--commits n] [--discard n] [--pace s] [--variant off] [--rep n] [--store local|azure-like] [--source on|off] [--scratch <dir>] | admission-audit (--path <dir|file> [--largest n] | --groups n --rows r --scratch <dir>) [--batch rows] | compaction-audit --scratch <dir> (--table <fixture dir> [--dvs on|off] | --shape <name>) [--target MiB] [--codec zstd|snappy] [--skip n] [--bins n] [--keep <dir>]");
            return ManualRun.HarnessError;
        }
    }
}
