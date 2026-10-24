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

namespace FlowtideDotNet.Connector.DeltaLake.Tests
{
    internal static class DirectoryLinks
    {
        /// <summary>
        /// A symbolic link where the process may create one, otherwise a Windows junction; false when neither can be made.
        /// </summary>
        public static async Task<bool> TryCreateAsync(string link, string target)
        {
            try
            {
                Directory.CreateSymbolicLink(link, target);
                return true;
            }
            catch (IOException)
            {
            }
            catch (UnauthorizedAccessException)
            {
            }
            if (!OperatingSystem.IsWindows())
            {
                return false;
            }
            var start = new ProcessStartInfo("cmd.exe") { UseShellExecute = false, RedirectStandardOutput = true, RedirectStandardError = true, CreateNoWindow = true };
            start.ArgumentList.Add("/c");
            start.ArgumentList.Add("mklink");
            start.ArgumentList.Add("/J");
            start.ArgumentList.Add(link);
            start.ArgumentList.Add(target);
            using var process = Process.Start(start)!;
            await Task.WhenAll(process.StandardOutput.ReadToEndAsync(), process.StandardError.ReadToEndAsync());
            await process.WaitForExitAsync();
            return process.ExitCode == 0 && Directory.Exists(link);
        }
    }
}
