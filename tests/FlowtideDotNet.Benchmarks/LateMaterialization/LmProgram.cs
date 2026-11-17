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

using System.Diagnostics;
using System.Globalization;
using System.Text;
using System.Text.Json;

namespace FlowtideDotNet.Benchmarks.LateMaterialization
{
    /// <summary>
    /// Baseline runs for late materialization, started with --late-materialization.
    /// </summary>
    internal static class LmProgram
    {
        public const string ArmVariable = "FLOWTIDE_LM_ARM";

        private const string Usage = "Usage: --late-materialization selfcheck|describe|run|census|paired [--workload LM-J5] [--scale 0.001] [--rounds 3] [--narrow] [--shuffled] [--cache-pages 10000] [--state-dir path] [--memory-state] [--seed 123] [--pairs 15] [--timeout 3600]";

        private static readonly string[] OptionNames = ["workload", "scale", "rounds", "narrow", "shuffled", "cache-pages", "state-dir", "memory-state", "seed", "pairs", "timeout"];

        // Options that may come without a value
        private static readonly string[] FlagNames = ["narrow", "shuffled", "memory-state"];

        private static readonly string[] Metrics = ["LoadRowsPerSecond", "ChurnRowsPerSecond", "LoadCheckpointMs", "CheckpointMeanMs", "CheckpointMaxMs", "PeakWorkingSetMb"];

        // Two sided 95 percent t values by degrees of freedom
        private static readonly double[] TValues =
        [
            12.706, 4.303, 3.182, 2.776, 2.571, 2.447, 2.365, 2.306, 2.262, 2.228,
            2.201, 2.179, 2.160, 2.145, 2.131, 2.120, 2.110, 2.101, 2.093, 2.086,
            2.080, 2.074, 2.069, 2.064, 2.060, 2.056, 2.052, 2.048, 2.045, 2.042
        ];

        public static async Task<int> Run(string[] args)
        {
            LmOptions options;
            try
            {
                options = LmOptions.Parse(args, OptionNames, FlagNames);
            }
            catch (ArgumentException e)
            {
                Console.Error.WriteLine(e.Message);
                Console.Error.WriteLine(Usage);
                return 2;
            }
            switch (options.Command)
            {
                case "selfcheck":
                    return await LmSelfCheck.Run(options.Workload("all"));
                case "describe":
                    Console.WriteLine(Describe(LmWorkload.Create(options.Workload("LM-J5"), options.Scale)));
                    return 0;
                case "run":
                    var result = await LmRun.Run(options.ForRun(options.Workload("LM-J5"), !options.Narrow, false, true));
                    Console.WriteLine(JsonSerializer.Serialize(result));
                    return 0;
                case "census":
                    return await Census(options);
                case "paired":
                    return Paired(args, options);
                default:
                    Console.Error.WriteLine($"Unknown command {options.Command}");
                    Console.Error.WriteLine(Usage);
                    return 2;
            }
        }

        private static string Describe(LmWorkload workload)
        {
            var text = new StringBuilder();
            for (int i = 0; i < workload.Tables.Count; i++)
            {
                var table = workload.Tables[i];
                text.AppendLine($"{table.Name}: {table.Rows} rows, {table.RowBytes} bytes per row");
                for (int c = 0; c < table.Columns.Count; c++)
                {
                    var column = table.Columns[c];
                    text.AppendLine($"  {column.Name} {column.Kind} size {column.Size} modulus {column.Modulus}{(column.Payload ? " payload" : string.Empty)}");
                }
            }
            text.AppendLine("wide:   " + workload.WideSql);
            text.AppendLine("narrow: " + workload.NarrowSql);
            text.AppendLine("kept classes: " + string.Join(", ", workload.KeptClasses));
            if (workload.KeptOnceClass != null)
            {
                text.AppendLine("one copy kept in: " + workload.KeptOnceClass);
            }
            return text.ToString();
        }

        private static async Task<int> Census(LmOptions options)
        {
            string selected = options.Workload("all");
            var workloads = selected == "all" ? LmWorkload.Names : [selected];
            int problems = 0;
            for (int i = 0; i < workloads.Length; i++)
            {
                var wide = await LmRun.Run(options.ForRun(workloads[i], true, true, false));
                var narrow = await LmRun.Run(options.ForRun(workloads[i], false, true, false));
                Console.WriteLine(CensusReport(LmWorkload.Create(workloads[i], options.Scale), wide, narrow));
                Console.WriteLine(JsonSerializer.Serialize(new { wide, narrow }));
                problems += wide.CensusProblems!.Count + narrow.CensusProblems!.Count;
            }
            return problems == 0 ? 0 : 1;
        }

        /// <summary>
        /// The narrow run is the same query without its wide columns.
        /// </summary>
        public static string CensusReport(LmWorkload workload, LmRunResult wide, LmRunResult narrow)
        {
            var wideBytes = LmCensus.ByClass(wide.Census!);
            var narrowBytes = LmCensus.ByClass(narrow.Census!);
            var report = new StringBuilder();
            report.AppendLine($"{wide.Workload} scale {wide.Scale.ToString(CultureInfo.InvariantCulture)}, {wide.InputRows} input rows");
            report.AppendLine($"{"class",-20}{"wide MB",12}{"narrow MB",12}{"wide data MB",14}  stays");
            long total = 0;
            long removable = 0;
            string? keptTree = null;
            for (int i = 0; i < LmCensus.Classes.Length; i++)
            {
                string name = LmCensus.Classes[i];
                long difference = wideBytes[name] - narrowBytes[name];
                total += wideBytes[name];
                bool kept = Array.IndexOf(workload.KeptClasses, name) >= 0;
                string stays = kept ? "yes" : string.Empty;
                if (name == workload.KeptOnceClass)
                {
                    // Keeping the smallest copy keeps this an upper bound
                    long keptBytes = SmallestTreeDifference(wide.Census!, narrow.Census!, name, out keptTree);
                    removable += Math.Max(0, difference - keptBytes);
                    stays = "one copy";
                }
                else if (!kept && difference > 0)
                {
                    removable += difference;
                }
                report.AppendLine($"{name,-20}{Megabytes(wideBytes[name]),12}{Megabytes(narrowBytes[name]),12}{Megabytes(difference),14}  {stays}");
            }
            report.AppendLine($"{"total",-20}{Megabytes(total),12}");
            if (workload.KeptOnceClass != null)
            {
                report.AppendLine($"the copy that stays is sized as {keptTree ?? "no tree, nothing differs"}");
            }
            double share = total == 0 ? 0 : 100.0 * removable / total;
            report.AppendLine($"removable upper bound {Megabytes(removable)} MB, {share.ToString("F1", CultureInfo.InvariantCulture)}% of wide state");
            report.AppendLine($"counters wide: {Counters(wide)}");
            for (int i = 0; i < wide.CensusProblems!.Count; i++)
            {
                report.AppendLine($"CENSUS PROBLEM wide: {wide.CensusProblems[i]}");
            }
            for (int i = 0; i < narrow.CensusProblems!.Count; i++)
            {
                report.AppendLine($"CENSUS PROBLEM narrow: {narrow.CensusProblems[i]}");
            }
            return report.ToString();
        }

        /// <summary>
        /// The smallest wide data any tree of a class holds.
        /// </summary>
        public static long SmallestTreeDifference(List<LmCensusRow> wide, List<LmCensusRow> narrow, string stateClass, out string? tree)
        {
            long smallest = 0;
            tree = null;
            for (int i = 0; i < wide.Count; i++)
            {
                if (wide[i].StateClass != stateClass)
                {
                    continue;
                }
                // Both forms name their trees the same
                long difference = wide[i].Bytes;
                for (int n = 0; n < narrow.Count; n++)
                {
                    if (narrow[n].Operator == wide[i].Operator && narrow[n].Tree == wide[i].Tree)
                    {
                        difference -= narrow[n].Bytes;
                        break;
                    }
                }
                if (difference > 0 && (tree == null || difference < smallest))
                {
                    smallest = difference;
                    tree = $"{wide[i].DisplayName} {wide[i].Operator} tree {wide[i].Tree}, {Megabytes(difference)} MB";
                }
            }
            return smallest;
        }

        private static string Counters(LmRunResult result)
        {
            var text = new StringBuilder();
            foreach (var counter in result.Counters)
            {
                text.Append(counter.Key).Append('=').Append(counter.Value.ToString("F0", CultureInfo.InvariantCulture)).Append(' ');
            }
            return text.ToString();
        }

        private static string Megabytes(long bytes)
        {
            return (bytes / (1024.0 * 1024.0)).ToString("F2", CultureInfo.InvariantCulture);
        }

        private static int Paired(string[] args, LmOptions options)
        {
            int pairs = options.Pairs;
            // Half of the pairs start with each arm, in a seeded order
            var offFirst = new bool[pairs];
            for (int i = 0; i < pairs; i++)
            {
                offFirst[i] = i % 2 == 0;
            }
            var random = new Random(unchecked((int)LmData.Mix((ulong)options.Seed)));
            random.Shuffle(offFirst);

            var childArguments = options.ChildArguments(args);
            var off = new List<LmRunResult>();
            var placebo = new List<LmRunResult>();
            for (int pair = 0; pair < pairs; pair++)
            {
                var first = RunChild(offFirst[pair] ? "Off" : "Placebo", childArguments, options.TimeoutSeconds);
                var second = RunChild(offFirst[pair] ? "Placebo" : "Off", childArguments, options.TimeoutSeconds);
                off.Add(offFirst[pair] ? first : second);
                placebo.Add(offFirst[pair] ? second : first);
                Console.Error.WriteLine($"pair {pair + 1}/{pairs} done");
            }
            Console.WriteLine(PairedReport(off, placebo));
            return 0;
        }

        private static LmRunResult RunChild(string arm, List<string> arguments, double timeoutSeconds)
        {
            var start = new ProcessStartInfo()
            {
                FileName = Environment.ProcessPath!,
                RedirectStandardOutput = true,
                RedirectStandardError = true,
                UseShellExecute = false
            };
            // Under the dotnet host the assembly comes first
            if (Path.GetFileNameWithoutExtension(start.FileName).Equals("dotnet", StringComparison.OrdinalIgnoreCase))
            {
                start.ArgumentList.Add(Environment.GetCommandLineArgs()[0]);
            }
            for (int i = 0; i < arguments.Count; i++)
            {
                start.ArgumentList.Add(arguments[i]);
            }
            start.Environment[ArmVariable] = arm;
            using var process = Process.Start(start)!;
            var output = process.StandardOutput.ReadToEndAsync();
            var errors = process.StandardError.ReadToEndAsync();
            if (!process.WaitForExit(TimeSpan.FromSeconds(timeoutSeconds + 300)))
            {
                process.Kill(true);
                throw new TimeoutException($"The {arm} run did not finish in time");
            }
            if (process.ExitCode != 0)
            {
                throw new InvalidOperationException($"The {arm} run failed with exit code {process.ExitCode}: {errors.Result}");
            }
            var lines = output.Result.Split('\n', StringSplitOptions.RemoveEmptyEntries);
            return JsonSerializer.Deserialize<LmRunResult>(lines[lines.Length - 1])!;
        }

        public static string PairedReport(List<LmRunResult> off, List<LmRunResult> placebo)
        {
            var report = new StringBuilder();
            report.AppendLine($"{off[0].Workload} scale {off[0].Scale.ToString(CultureInfo.InvariantCulture)}, {off.Count} pairs");
            if (off.Count < 15)
            {
                report.AppendLine("Fewer than 15 pairs, too few to decide a gate.");
            }
            // The arms are only comparable when they did the same work
            for (int i = 0; i < off.Count; i++)
            {
                if (off[i].ChurnRows != off[0].ChurnRows || placebo[i].ChurnRows != off[0].ChurnRows || off[i].InputRows != placebo[i].InputRows)
                {
                    report.AppendLine($"WORK MISMATCH in pair {i + 1}: Off sent {off[i].ChurnRows} changed rows, Placebo {placebo[i].ChurnRows}, the first run {off[0].ChurnRows}");
                }
            }
            report.AppendLine($"{"metric",-22}{"Off mean",14}{"cv %",8}{"Placebo mean",14}{"cv %",8}{"drift %",10}{"95% +/-",10}");
            for (int m = 0; m < Metrics.Length; m++)
            {
                var offValues = new double[off.Count];
                var placeboValues = new double[off.Count];
                var drift = new double[off.Count];
                for (int i = 0; i < off.Count; i++)
                {
                    offValues[i] = Metric(off[i], Metrics[m]);
                    placeboValues[i] = Metric(placebo[i], Metrics[m]);
                    // A log ratio treats a gain and a loss alike
                    drift[i] = offValues[i] > 0 && placeboValues[i] > 0 ? 100.0 * Math.Log(placeboValues[i] / offValues[i]) : 0;
                }
                double interval = TValue(drift.Length - 1) * Deviation(drift) / Math.Sqrt(drift.Length);
                report.AppendLine(string.Format(CultureInfo.InvariantCulture, "{0,-22}{1,14:F1}{2,8:F2}{3,14:F1}{4,8:F2}{5,10:F2}{6,10:F2}",
                    Metrics[m], Mean(offValues), Variation(offValues), Mean(placeboValues), Variation(placeboValues), Mean(drift), interval));
            }
            report.AppendLine($"counters, first Off run: {Counters(off[0])}");
            return report.ToString();
        }

        private static double TValue(int degrees)
        {
            if (degrees < 1)
            {
                return double.NaN;
            }
            return degrees <= TValues.Length ? TValues[degrees - 1] : 1.96;
        }

        private static double Metric(LmRunResult result, string name)
        {
            switch (name)
            {
                case "LoadRowsPerSecond":
                    return result.LoadRowsPerSecond;
                case "ChurnRowsPerSecond":
                    return result.ChurnRowsPerSecond;
                case "LoadCheckpointMs":
                    return result.LoadCheckpointMs;
                case "CheckpointMeanMs":
                    return result.CheckpointMeanMs;
                case "CheckpointMaxMs":
                    return result.CheckpointMaxMs;
                default:
                    return result.PeakWorkingSetMb;
            }
        }

        private static double Mean(double[] values)
        {
            double sum = 0;
            for (int i = 0; i < values.Length; i++)
            {
                sum += values[i];
            }
            return sum / values.Length;
        }

        private static double Deviation(double[] values)
        {
            if (values.Length < 2)
            {
                return 0;
            }
            double mean = Mean(values);
            double sum = 0;
            for (int i = 0; i < values.Length; i++)
            {
                sum += (values[i] - mean) * (values[i] - mean);
            }
            return Math.Sqrt(sum / (values.Length - 1));
        }

        private static double Variation(double[] values)
        {
            double mean = Mean(values);
            return mean == 0 ? 0 : 100.0 * Deviation(values) / mean;
        }
    }

    /// <summary>
    /// The command line of a run, checked before anything starts.
    /// </summary>
    internal sealed class LmOptions
    {
        private readonly Dictionary<string, string> _values = new Dictionary<string, string>();

        public string Command { get; private set; } = string.Empty;

        public double Scale { get; private set; } = 0.001;

        public int Rounds { get; private set; } = 3;

        public long Seed { get; private set; } = 123;

        public int CachePages { get; private set; } = 10000;

        public int Pairs { get; private set; } = 15;

        public double TimeoutSeconds { get; private set; } = 3600;

        public bool Narrow { get; private set; }

        public bool Shuffled { get; private set; }

        public bool MemoryState { get; private set; }

        public string Workload(string fallback)
        {
            return _values.TryGetValue("workload", out var value) ? value : fallback;
        }

        public static LmOptions Parse(string[] args, string[] names, string[] flags)
        {
            if (args.Length < 2)
            {
                throw new ArgumentException("A command is required.");
            }
            var options = new LmOptions() { Command = args[1] };
            for (int i = 2; i < args.Length; i++)
            {
                if (!args[i].StartsWith("--", StringComparison.Ordinal))
                {
                    throw new ArgumentException($"Unexpected argument {args[i]}");
                }
                string name = args[i].Substring(2);
                string? value = null;
                int equals = name.IndexOf('=');
                if (equals >= 0)
                {
                    value = name.Substring(equals + 1);
                    name = name.Substring(0, equals);
                }
                else if (i + 1 < args.Length && !args[i + 1].StartsWith("--", StringComparison.Ordinal))
                {
                    value = args[i + 1];
                    i++;
                }
                if (Array.IndexOf(names, name) < 0)
                {
                    throw new ArgumentException($"Unknown option --{name}");
                }
                if (value == null && Array.IndexOf(flags, name) >= 0)
                {
                    value = "true";
                }
                if (string.IsNullOrEmpty(value))
                {
                    throw new ArgumentException($"--{name} needs a value");
                }
                options._values[name] = value;
            }
            options.Scale = options.Number("scale", options.Scale, 1e-7, 100);
            options.Rounds = (int)options.Whole("rounds", options.Rounds, 0, 1000);
            options.CachePages = (int)options.Whole("cache-pages", options.CachePages, 16, int.MaxValue);
            options.Pairs = (int)options.Whole("pairs", options.Pairs, 2, 10000);
            options.TimeoutSeconds = options.Number("timeout", options.TimeoutSeconds, 1, 1e7);
            options.Seed = options.Whole("seed", options.Seed, long.MinValue, long.MaxValue);
            options.Narrow = options.Flag("narrow");
            options.Shuffled = options.Flag("shuffled");
            options.MemoryState = options.Flag("memory-state");
            if (options._values.TryGetValue("workload", out var workload))
            {
                if (workload != "all")
                {
                    // Throws for a name that is not a workload
                    LmWorkload.Create(workload, options.Scale);
                }
                else if (options.Command != "census" && options.Command != "selfcheck")
                {
                    throw new ArgumentException("--workload all is only for census and selfcheck");
                }
            }
            return options;
        }

        private double Number(string name, double fallback, double lowest, double highest)
        {
            if (!_values.TryGetValue(name, out var text))
            {
                return fallback;
            }
            if (!double.TryParse(text, NumberStyles.Float, CultureInfo.InvariantCulture, out var value) || double.IsNaN(value) || value < lowest || value > highest)
            {
                throw new ArgumentException($"--{name} {text} must be a number from {lowest.ToString(CultureInfo.InvariantCulture)} to {highest.ToString(CultureInfo.InvariantCulture)}");
            }
            return value;
        }

        private long Whole(string name, long fallback, long lowest, long highest)
        {
            if (!_values.TryGetValue(name, out var text))
            {
                return fallback;
            }
            if (!long.TryParse(text, NumberStyles.AllowLeadingSign, CultureInfo.InvariantCulture, out var value) || value < lowest || value > highest)
            {
                throw new ArgumentException($"--{name} {text} must be a whole number from {lowest} to {highest}");
            }
            return value;
        }

        private bool Flag(string name)
        {
            if (!_values.TryGetValue(name, out var text))
            {
                return false;
            }
            if (text != "true" && text != "false")
            {
                throw new ArgumentException($"--{name} {text} must be true or false, or have no value");
            }
            return text == "true";
        }

        public LmRunOptions ForRun(string workload, bool wide, bool census, bool timed)
        {
            // Timed runs keep their state on disk unless told otherwise
            string? directory = _values.TryGetValue("state-dir", out var given) ? given : null;
            if (directory == null && timed && !MemoryState)
            {
                directory = Path.GetTempPath();
            }
            return new LmRunOptions()
            {
                Workload = workload,
                Wide = wide,
                Census = census,
                Shuffled = Shuffled,
                Scale = Scale,
                Rounds = Rounds,
                Seed = Seed,
                CachePages = CachePages,
                StateDirectory = directory,
                ReportPeakMemory = timed,
                TimeoutSeconds = TimeoutSeconds,
                Arm = Environment.GetEnvironmentVariable(LmProgram.ArmVariable) ?? "Off"
            };
        }

        /// <summary>
        /// The arguments of one child run of a paired comparison.
        /// </summary>
        public List<string> ChildArguments(string[] args)
        {
            var arguments = new List<string>() { args[0], "run" };
            foreach (var value in _values)
            {
                if (value.Key != "pairs")
                {
                    arguments.Add($"--{value.Key}={value.Value}");
                }
            }
            return arguments;
        }
    }
}
