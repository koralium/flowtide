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

using FlowtideDotNet.AcceptanceTests.Internal;
using Stowage;
using System.Text;

namespace FlowtideDotNet.Connector.DeltaLake.Tests
{
    /// <summary>
    /// Crashes the stream on the first write of one file.
    /// </summary>
    internal class CrashOnceFileStorage : IFileStorage
    {
        private readonly IFileStorage _inner;
        private readonly string _crashOnFileName;
        private int _crashes;

        public CrashOnceFileStorage(IFileStorage inner, string crashOnFileName)
        {
            _inner = inner;
            _crashOnFileName = crashOnFileName;
        }

        public int Crashes => Volatile.Read(ref _crashes);

        private void CrashOnce(IOPath path)
        {
            if (path.Full.EndsWith("/" + _crashOnFileName) && Interlocked.Exchange(ref _crashes, 1) == 0)
            {
                throw new CrashException();
            }
        }

        public Task<Stream> OpenWrite(IOPath path, CancellationToken cancellationToken = default)
        {
            CrashOnce(path);
            return _inner.OpenWrite(path, cancellationToken);
        }

        public Task Ren(IOPath name, IOPath newName, CancellationToken cancellationToken = default)
        {
            CrashOnce(newName);
            return _inner.Ren(name, newName, cancellationToken);
        }

        public Task<string?> ReadText(IOPath path, Encoding? encoding = null, CancellationToken cancellationToken = default)
        {
            return _inner.ReadText(path, encoding, cancellationToken);
        }

        public Task WriteText(IOPath path, string contents, Encoding? encoding = null, CancellationToken cancellationToken = default)
        {
            CrashOnce(path);
            return _inner.WriteText(path, contents, encoding, cancellationToken);
        }

        public Task<IReadOnlyCollection<IOEntry>> Ls(IOPath? path = null, bool recurse = false, CancellationToken cancellationToken = default)
        {
            return _inner.Ls(path, recurse, cancellationToken);
        }

        public Task<Stream?> OpenRead(IOPath path, CancellationToken cancellationToken = default)
        {
            return _inner.OpenRead(path, cancellationToken);
        }

        public Task<T?> ReadAsJson<T>(IOPath path, CancellationToken cancellationToken = default)
        {
            return _inner.ReadAsJson<T>(path, cancellationToken);
        }

        public Task WriteAsJson(IOPath path, object value, bool writeIndented = true, CancellationToken cancellationToken = default)
        {
            return _inner.WriteAsJson(path, value, writeIndented, cancellationToken);
        }

        public Task Rm(IOPath path, CancellationToken cancellationToken = default)
        {
            return _inner.Rm(path, cancellationToken);
        }

        public Task<bool> Exists(IOPath path, CancellationToken cancellationToken = default)
        {
            return _inner.Exists(path, cancellationToken);
        }

        public Task<IOEntry?> Stat(IOPath path, CancellationToken cancellationToken = default)
        {
            return _inner.Stat(path, cancellationToken);
        }

        public void Dispose()
        {
            _inner.Dispose();
        }
    }
}
