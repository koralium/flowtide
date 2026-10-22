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
using System.Runtime;
using System.Runtime.InteropServices;
using System.Text;
using System.Text.Json;

namespace FlowtideDotNet.Connector.DeltaLake.Tests
{
    // A manual fresh-process run: its start record, its watchdog and its exit code
    internal static class ManualRun
    {
        public const int Passed = 0;
        public const int BarFailed = 1;
        public const int HarnessError = 2;
        public const int WatchdogFired = 3;

        public static readonly TimeSpan Limit = TimeSpan.FromMinutes(110);

        /// <summary>
        /// Writes the start record, runs the body and writes its end record; past the limit, writes a failed envelope with the last progress line and exits the process with 3.
        /// </summary>
        public static async Task<int> RunAsync(string[] args, IReadOnlyDictionary<string, string> describe, Func<TextWriter, Task<int>> run, TextWriter output, TimeSpan? limit = null)
        {
            var progress = new ProgressWriter(output);
            var started = Stopwatch.GetTimestamp();
            var ended = 0;
            progress.Record(Start(args, describe, limit ?? Limit));
            using var watchdog = new Timer(_ =>
            {
                if (Interlocked.Exchange(ref ended, 1) == 0)
                {
                    progress.Record(new { record = "end", status = "failed-envelope", reason = "watchdog", code = WatchdogFired, elapsedSeconds = Stopwatch.GetElapsedTime(started).TotalSeconds, lastProgress = progress.Last });
                    Environment.Exit(WatchdogFired);
                }
            }, null, limit ?? Limit, Timeout.InfiniteTimeSpan);
            int code;
            string? error = null;
            try
            {
                code = await run(progress);
            }
            catch (Exception e)
            {
                code = HarnessError;
                error = e.ToString();
            }
            if (Interlocked.Exchange(ref ended, 1) == 0)
            {
                var status = code switch { Passed => "passed", BarFailed => "bar-failed", _ => "harness-error" };
                progress.Record(new { record = "end", status, code, elapsedSeconds = Stopwatch.GetElapsedTime(started).TotalSeconds, error });
            }
            return code;
        }

        // Host, build, hardware, commit, scratch space, the command's options and other dotnet processes
        private static object Start(string[] args, IReadOnlyDictionary<string, string> describe, TimeSpan limit)
        {
            var options = new SortedDictionary<string, string>(StringComparer.Ordinal);
            foreach (var (key, value) in describe)
            {
                options[key] = value;
            }
            for (int i = 1; i + 1 < args.Length; i++)
            {
                if (args[i].StartsWith("--", StringComparison.Ordinal))
                {
                    options[args[i][2..]] = args[++i];
                }
            }
            var scratch = options.GetValueOrDefault("scratch") ?? Environment.CurrentDirectory;
            var drive = new DriveInfo(Path.GetPathRoot(Path.GetFullPath(scratch))!);
#if DEBUG
            const string configuration = "Debug";
#else
            const string configuration = "Release";
#endif
#if DEBUG_WRITE
            const bool debugWrite = true;
#else
            const bool debugWrite = false;
#endif
            return new
            {
                record = "start",
                command = args.Length > 0 ? args[0] : "",
                args,
                utc = DateTimeOffset.UtcNow,
                os = RuntimeInformation.OSDescription,
                architecture = RuntimeInformation.ProcessArchitecture.ToString(),
                framework = RuntimeInformation.FrameworkDescription,
                tfm = AppContext.TargetFrameworkName,
                configuration,
                debugWrite,
                cpu = CpuName(),
                logicalCpus = Environment.ProcessorCount,
                availableMemoryBytes = GC.GetGCMemoryInfo().TotalAvailableMemoryBytes,
                serverGc = GCSettings.IsServerGC,
                commit = options.GetValueOrDefault("commit"),
                scratch = Path.GetFullPath(scratch),
                scratchFreeBytes = drive.AvailableFreeSpace,
                options,
                otherDotnetProcesses = OtherDotnetProcesses(),
                watchdogMinutes = limit.TotalMinutes
            };
        }

        private static string? CpuName()
        {
            if (OperatingSystem.IsWindows())
            {
                return Microsoft.Win32.Registry.GetValue(@"HKEY_LOCAL_MACHINE\HARDWARE\DESCRIPTION\System\CentralProcessor\0", "ProcessorNameString", null) as string;
            }
            return File.Exists("/proc/cpuinfo") ? File.ReadLines("/proc/cpuinfo").FirstOrDefault(x => x.StartsWith("model name", StringComparison.Ordinal))?.Split(':', 2)[1].Trim() : null;
        }

        private static int OtherDotnetProcesses()
        {
            var own = Environment.ProcessId;
            var count = 0;
            foreach (var process in Process.GetProcessesByName("dotnet").Concat(Process.GetProcessesByName("testhost")))
            {
                using (process)
                {
                    count += process.Id == own ? 0 : 1;
                }
            }
            return count;
        }

        // Forwards the body's output and keeps its last line; records are single JSON lines
        private sealed class ProgressWriter : TextWriter
        {
            private readonly TextWriter _inner;
            private readonly StringBuilder _line = new StringBuilder();
            private string? _last;

            public ProgressWriter(TextWriter inner)
            {
                _inner = inner;
            }

            public override Encoding Encoding => _inner.Encoding;

            public string? Last
            {
                get
                {
                    lock (_line)
                    {
                        return _line.Length > 0 ? _line.ToString() : _last;
                    }
                }
            }

            public override void Write(char value)
            {
                lock (_line)
                {
                    _inner.Write(value);
                    Track(value);
                }
            }

            public override void Write(string? value)
            {
                lock (_line)
                {
                    _inner.Write(value);
                    foreach (var c in value ?? "")
                    {
                        Track(c);
                    }
                }
            }

            public override void WriteLine(string? value)
            {
                lock (_line)
                {
                    Write(value);
                    Write(CoreNewLine);
                    _inner.Flush();
                }
            }

            private void Track(char value)
            {
                if (value == '\n')
                {
                    _last = _line.ToString().TrimEnd('\r');
                    _line.Clear();
                }
                else
                {
                    _line.Append(value);
                }
            }

            public void Record(object record)
            {
                lock (_line)
                {
                    _inner.WriteLine(JsonSerializer.Serialize(record));
                    _inner.Flush();
                }
            }

            public override void Flush()
            {
                lock (_line)
                {
                    _inner.Flush();
                }
            }
        }
    }
}
