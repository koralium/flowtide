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
using System.Text;

namespace FlowtideDotNet.Connector.DeltaLake.Internal.Delta.Checkpoints
{
    /// <summary>
    /// Reads the UTF-8 lines of a commit into a bounded buffer, splitting as StreamReader does; a line over the cap is never buffered whole.
    /// </summary>
    internal sealed class BoundedLineReader : IDisposable
    {
        public const int ReadSize = 64 << 10;

        private readonly Stream _stream;
        private readonly JobMemory? _memory;
        private readonly int _cap;
        private readonly bool _skipOversizedCommitInfo;
        private readonly bool _ownsStream;
        private byte[] _read;
        private int _readStart;
        private int _readEnd;
        private byte[] _line = Array.Empty<byte>();
        private int _lineLength;
        private bool _started;
        private bool _pendingCarriageReturn;
        private bool _end;

        // With a ledger the buffers are charged; a cap of int.MaxValue reads like StreamReader
        public BoundedLineReader(Stream stream, JobMemory? memory, int cap, bool skipOversizedCommitInfo, bool ownsStream = false)
        {
            _stream = stream;
            _memory = memory;
            _cap = cap;
            _skipOversizedCommitInfo = skipOversizedCommitInfo;
            _ownsStream = ownsStream;
            _read = Allocate(ReadSize);
        }

        public ReadOnlyMemory<byte> Line => _line.AsMemory(0, _lineLength);

        // Oversized commitInfo lines skipped so far
        public int SkippedLines { get; private set; }

        // The largest line buffer held, for tests
        public int LineCapacity => _line.Length;

        private byte[] Allocate(int length) => _memory != null ? _memory.Allocate(length) : new byte[length];

        private void Free(byte[] buffer)
        {
            if (_memory != null && buffer.Length > 0)
            {
                _memory.Free(buffer);
            }
        }

        private async ValueTask<bool> Fill(CancellationToken cancellationToken)
        {
            if (_end)
            {
                return false;
            }
            _readStart = 0;
            _readEnd = await _stream.ReadAsync(_read.AsMemory(), cancellationToken);
            if (_readEnd == 0)
            {
                _end = true;
                return false;
            }
            return true;
        }

        // A byte order mark can arrive split across short reads, so the first bytes are gathered until three are held or the stream ends
        private async ValueTask GatherStart(CancellationToken cancellationToken)
        {
            while (!_end && _readEnd - _readStart < 3)
            {
                var read = await _stream.ReadAsync(_read.AsMemory(_readEnd), cancellationToken);
                if (read == 0)
                {
                    _end = true;
                    return;
                }
                _readEnd += read;
            }
        }

        /// <summary>
        /// Moves to the next line, blank ones included; false at the end of the stream.
        /// </summary>
        public async ValueTask<bool> ReadLineAsync(CancellationToken cancellationToken = default)
        {
            while (true)
            {
                _lineLength = 0;
                var any = false;
                OversizedLine? oversized = null;
                while (true)
                {
                    if (_readStart == _readEnd && !await Fill(cancellationToken))
                    {
                        break;
                    }
                    if (!_started)
                    {
                        await GatherStart(cancellationToken);
                        _started = true;
                        // StreamReader drops a UTF-8 byte order mark
                        if (_readEnd - _readStart >= 3 && _read[_readStart] == 0xEF && _read[_readStart + 1] == 0xBB && _read[_readStart + 2] == 0xBF)
                        {
                            _readStart += 3;
                            continue;
                        }
                    }
                    if (_pendingCarriageReturn)
                    {
                        _pendingCarriageReturn = false;
                        if (_read[_readStart] == (byte)'\n')
                        {
                            _readStart++;
                            continue;
                        }
                    }
                    if (Consume(ref oversized, ref any))
                    {
                        break;
                    }
                }
                if (!any && oversized == null)
                {
                    return false;
                }
                if (oversized == null)
                {
                    return true;
                }
                if (!_skipOversizedCommitInfo || !oversized.IsOnlyCommitInfo())
                {
                    throw new CheckpointDeclinedException(MaintenanceReason.LineOverCap, DeclineDependency.Tail, $"A log line is over the {_cap} byte action cap");
                }
                SkippedLines++;
            }
        }

        // Takes the buffered bytes up to the next line end into the line, outside the async method so spans compile on C# 12; true when the line ended
        private bool Consume(ref OversizedLine? oversized, ref bool any)
        {
            var span = _read.AsSpan(_readStart, _readEnd - _readStart);
            var end = span.IndexOfAny((byte)'\r', (byte)'\n');
            var chunk = end < 0 ? span : span.Slice(0, end);
            any |= chunk.Length > 0;
            if (oversized == null && (long)_lineLength + chunk.Length > _cap)
            {
                oversized = new OversizedLine();
                oversized.Feed(_line.AsSpan(0, _lineLength));
                _lineLength = 0;
            }
            if (oversized != null)
            {
                oversized.Feed(chunk);
            }
            else
            {
                Append(chunk);
            }
            _readStart += chunk.Length;
            if (end < 0)
            {
                return false;
            }
            any = true;
            _pendingCarriageReturn = span[end] == (byte)'\r';
            _readStart++;
            return true;
        }

        private void Append(ReadOnlySpan<byte> chunk)
        {
            if (_lineLength + chunk.Length > _line.Length)
            {
                // Grown before the old buffer is freed, so a refused growth keeps the old one
                var grown = Allocate((int)Math.Min(_cap, Math.Max(_lineLength + (long)chunk.Length, Math.Max(4096L, _line.Length * 2L))));
                _line.AsSpan(0, _lineLength).CopyTo(grown);
                Free(_line);
                _line = grown;
            }
            chunk.CopyTo(_line.AsSpan(_lineLength));
            _lineLength += chunk.Length;
        }

        /// <summary>
        /// Whether a line is blank as string.IsNullOrWhiteSpace sees it.
        /// </summary>
        public static bool IsBlank(ReadOnlySpan<byte> line)
        {
            foreach (var b in line)
            {
                if (b is (byte)' ' or (byte)'\t' or 0x0B or 0x0C)
                {
                    continue;
                }
                if (b < 0x80)
                {
                    return false;
                }
                // Unicode white space is rare, so the line is decoded only then
                return string.IsNullOrWhiteSpace(Encoding.UTF8.GetString(line));
            }
            return true;
        }

        public void Dispose()
        {
            Free(_read);
            Free(_line);
            _read = Array.Empty<byte>();
            _line = Array.Empty<byte>();
            if (_ownsStream)
            {
                _stream.Dispose();
            }
        }

        // Follows an oversized line byte by byte: only an object whose single member is "commitInfo" with an object value qualifies
        private sealed class OversizedLine
        {
            private static readonly byte[] s_name = "commitInfo"u8.ToArray();
            private int _state;
            private int _nameLength;
            private int _depth;
            // Bit i is set when the bracket opened at depth i is a '['
            private ulong _brackets;
            private bool _inString;
            private bool _escape;

            public bool IsOnlyCommitInfo() => _state == 6;

            public void Feed(ReadOnlySpan<byte> bytes)
            {
                foreach (var b in bytes)
                {
                    Step(b);
                }
            }

            private static bool Space(byte b) => b is (byte)' ' or (byte)'\t';

            private void Step(byte b)
            {
                switch (_state)
                {
                    case 0:
                        _state = Space(b) ? 0 : b == (byte)'{' ? 1 : -1;
                        break;
                    case 1:
                        _state = Space(b) ? 1 : b == (byte)'"' ? 2 : -1;
                        break;
                    case 2:
                        // The member name, compared raw; an escaped name does not qualify
                        if (b == (byte)'"')
                        {
                            _state = _nameLength == s_name.Length ? 3 : -1;
                        }
                        else if (_nameLength < s_name.Length && b == s_name[_nameLength])
                        {
                            _nameLength++;
                        }
                        else
                        {
                            _state = -1;
                        }
                        break;
                    case 3:
                        _state = Space(b) ? 3 : b == (byte)':' ? 4 : -1;
                        break;
                    case 4:
                        if (b == (byte)'{')
                        {
                            _depth = 1;
                            _brackets = 0;
                            _state = 5;
                        }
                        else if (!Space(b))
                        {
                            _state = -1;
                        }
                        break;
                    case 5:
                        if (_inString)
                        {
                            if (_escape)
                            {
                                _escape = false;
                            }
                            else if (b == (byte)'\\')
                            {
                                _escape = true;
                            }
                            else if (b == (byte)'"')
                            {
                                _inString = false;
                            }
                        }
                        else if (b == (byte)'"')
                        {
                            _inString = true;
                        }
                        else if (b is (byte)'{' or (byte)'[')
                        {
                            // Deeper than the encoder accepts, so never only a commitInfo
                            if (_depth >= 64)
                            {
                                _state = -1;
                                break;
                            }
                            _brackets = b == (byte)'[' ? _brackets | (1UL << _depth) : _brackets & ~(1UL << _depth);
                            _depth++;
                        }
                        else if (b is (byte)'}' or (byte)']')
                        {
                            _depth--;
                            // A closing bracket of the other kind is malformed, as it would be under the cap
                            if (((_brackets >> _depth) & 1) == 1 != (b == (byte)']'))
                            {
                                _state = -1;
                            }
                            else if (_depth == 0)
                            {
                                _state = 7;
                            }
                        }
                        break;
                    case 7:
                        _state = Space(b) ? 7 : b == (byte)'}' ? 6 : -1;
                        break;
                    case 6:
                        _state = Space(b) ? 6 : -1;
                        break;
                }
            }
        }
    }
}
