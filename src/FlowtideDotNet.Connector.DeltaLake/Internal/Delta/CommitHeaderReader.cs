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

using Stowage;

namespace FlowtideDotNet.Connector.DeltaLake.Internal.Delta
{
    /// <summary>
    /// Reads only the first line of a commit, through a pooled 64 KiB buffer; created paths are reported as read, so a caller trusts them only from a valid header.
    /// </summary>
    internal static class CommitHeaderReader
    {
        public const int BufferSize = 64 << 10;

        /// <summary>
        /// The bytes an own commit's first line starts with, before its stage id.
        /// </summary>
        public static ReadOnlySpan<byte> OwnPrefix => "{\"commitInfo\":{\"flowtide.stageId\":\""u8;

        /// <summary>
        /// The header of a stored commit, null when the file does not exist.
        /// </summary>
        public static async Task<CommitHeader?> ReadAsync(IFileStorage storage, IOPath path, HeaderMode mode, Action<string>? onCreatedFile = null, CancellationToken cancellationToken = default)
        {
            using var stream = await storage.OpenRead(path, cancellationToken);
            if (stream == null)
            {
                return null;
            }
            return await ReadAsync(stream, mode, onCreatedFile, cancellationToken);
        }

        public static async Task<CommitHeader> ReadAsync(Stream stream, HeaderMode mode, Action<string>? onCreatedFile = null, CancellationToken cancellationToken = default)
        {
            var scanner = new CommitHeaderScanner(mode, onCreatedFile);
            var buffer = System.Buffers.ArrayPool<byte>.Shared.Rent(BufferSize);
            try
            {
                while (true)
                {
                    var read = await stream.ReadAsync(buffer.AsMemory(0, BufferSize), cancellationToken);
                    if (read == 0)
                    {
                        return scanner.Complete(terminated: false);
                    }
                    if (FeedUntilLineEnd(scanner, buffer.AsSpan(0, read)))
                    {
                        return scanner.Complete(terminated: true);
                    }
                    if (scanner.Failed)
                    {
                        return scanner.Complete(terminated: false);
                    }
                }
            }
            finally
            {
                System.Buffers.ArrayPool<byte>.Shared.Return(buffer);
            }
        }

        /// <summary>
        /// The header of commit bytes held in memory, read across their chunks without a copy.
        /// </summary>
        public static CommitHeader Read(CommitBytes bytes, HeaderMode mode, Action<string>? onCreatedFile = null)
        {
            var scanner = new CommitHeaderScanner(mode, onCreatedFile);
            foreach (var segment in bytes.Segments())
            {
                if (FeedUntilLineEnd(scanner, segment.Span))
                {
                    return scanner.Complete(terminated: true);
                }
            }
            return scanner.Complete(terminated: false);
        }

        /// <summary>
        /// The header of one line a line reader already holds, without its terminator; terminated tells whether one followed it.
        /// </summary>
        public static CommitHeader Read(ReadOnlySpan<byte> line, bool terminated, HeaderMode mode, Action<string>? onCreatedFile = null)
        {
            var scanner = new CommitHeaderScanner(mode, onCreatedFile);
            scanner.Feed(line);
            return scanner.Complete(terminated);
        }

        // True when the line ended inside the bytes
        private static bool FeedUntilLineEnd(CommitHeaderScanner scanner, ReadOnlySpan<byte> bytes)
        {
            var end = bytes.IndexOfAny((byte)'\r', (byte)'\n');
            if (end < 0)
            {
                scanner.Feed(bytes);
                return false;
            }
            scanner.Feed(bytes.Slice(0, end));
            return true;
        }
    }
}
