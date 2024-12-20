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
using System.Runtime.CompilerServices;

namespace FlowtideDotNet.Storage.SqlServer
{
    internal class DebugWriter : IDisposable
    {
        private readonly StreamWriter _writer;
        private bool disposedValue;
        private const string DirectoryName = "debugwrite";

        public DebugWriter(string streamName)
        {
            if (!Directory.Exists(DirectoryName))
            {
                Directory.CreateDirectory(DirectoryName);
            }

            _writer = File.CreateText($"debugwrite/{streamName}.sqlstorage.{DateTime.UtcNow:yyMMddHHmm}.txt");
        }

        [Conditional("DEBUG")]
        [Conditional("DEBUG_WRITE")]
        public void WriteCall([CallerFilePath] string fp = "", [CallerMemberName] string callerMember = "", [CallerLineNumber] int ln = 0)
        {
            _writer!.WriteLine($"[{Path.GetFileName(fp)}:{ln}] {callerMember}");
            _writer.Flush();
        }

        [Conditional("DEBUG")]
        [Conditional("DEBUG_WRITE")]
        public void WriteCall(object[] args, [CallerFilePath] string fp = "", [CallerMemberName] string callerMember = "", [CallerLineNumber] int ln = 0)
        {

            _writer!.WriteLine($"[{Path.GetFileName(fp)}:{ln}] {callerMember}({args.Aggregate((s, n) => $"{s},{n}")})");
            _writer.Flush();
        }

        [Conditional("DEBUG")]
        [Conditional("DEBUG_WRITE")]
        public void WriteMessage(string message, [CallerFilePath] string fp = "", [CallerMemberName] string callerMember = "", [CallerLineNumber] int ln = 0)
        {
            _writer!.WriteLine($"[{Path.GetFileName(fp)}:{ln}] {callerMember}->{message}");
            _writer.Flush();
        }

        [Conditional("DEBUG")]
        [Conditional("DEBUG_WRITE")]
        public void DumpObj(object o, [CallerFilePath] string fp = "", [CallerMemberName] string callerMember = "", [CallerLineNumber] int ln = 0)
        {
            _writer!.WriteLine($"[{Path.GetFileName(fp)}:{ln}] {System.Text.Json.JsonSerializer.Serialize(o)}");
            _writer.Flush();
        }

        protected virtual void Dispose(bool disposing)
        {
            if (!disposedValue)
            {
                _writer!.Dispose();
                disposedValue = true;
            }
        }

        public void Dispose()
        {
            // Do not change this code. Put cleanup code in 'Dispose(bool disposing)' method
            Dispose(disposing: true);
            GC.SuppressFinalize(this);
        }
    }
}
