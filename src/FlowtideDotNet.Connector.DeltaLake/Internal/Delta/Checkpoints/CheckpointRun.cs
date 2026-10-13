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

namespace FlowtideDotNet.Connector.DeltaLake.Internal.Delta.Checkpoints
{
    /// <summary>
    /// Writes sorted entries, uncompressed, to a scratch file charged in the disk ledger.
    /// </summary>
    internal sealed class RunWriter : IDisposable
    {
        public const int BufferSize = 256 << 10;

        private readonly JobMemory _memory;
        private readonly QuotaWriteStream _stream;
        private byte[] _buffer;
        private int _used;

        public RunWriter(JobScratch scratch, JobMemory memory)
        {
            _memory = memory;
            Path = scratch.NewPath(".run");
            _buffer = memory.Allocate(BufferSize);
            try
            {
                _stream = scratch.Create(Path);
            }
            catch
            {
                memory.Free(_buffer);
                throw;
            }
        }

        public string Path { get; }

        public void Write(ReadOnlySpan<byte> entry)
        {
            if (entry.Length > _buffer.Length - _used)
            {
                Flush();
                if (entry.Length > _buffer.Length)
                {
                    _stream.Write(entry);
                    return;
                }
            }
            entry.CopyTo(_buffer.AsSpan(_used));
            _used += entry.Length;
        }

        private void Flush()
        {
            if (_used > 0)
            {
                _stream.Write(_buffer, 0, _used);
                _used = 0;
            }
        }

        /// <summary>
        /// Writes what is buffered and closes the file, keeping its charge.
        /// </summary>
        public void Close()
        {
            Flush();
            _stream.Dispose();
        }

        public void Dispose()
        {
            _stream.Dispose();
            if (_buffer.Length > 0)
            {
                _memory.Free(_buffer);
                _buffer = Array.Empty<byte>();
            }
        }
    }

    /// <summary>
    /// Reads a run's entries in order through a 256 KiB buffer that grows only for an entry larger than it.
    /// </summary>
    internal sealed class RunCursor : IDisposable
    {
        private readonly JobMemory _memory;
        private readonly FileStream _file;
        private byte[] _buffer;
        private int _start;
        private int _end;
        private int _current;
        private bool _eof;

        public RunCursor(string path, JobMemory memory)
        {
            _memory = memory;
            _buffer = memory.Allocate(RunWriter.BufferSize);
            try
            {
                _file = new FileStream(path, FileMode.Open, FileAccess.Read, FileShare.Read | FileShare.Delete, 1, FileOptions.SequentialScan);
            }
            catch
            {
                memory.Free(_buffer);
                throw;
            }
        }

        public bool HasCurrent { get; private set; }

        public ArenaEntry Current
        {
            get
            {
                ArenaEntry.TryDecode(_buffer.AsSpan(_start, _current), out var entry);
                return entry;
            }
        }

        public ReadOnlySpan<byte> CurrentRaw => _buffer.AsSpan(_start, _current);

        public bool MoveNext()
        {
            _start += _current;
            _current = 0;
            while (true)
            {
                if (ArenaEntry.TryDecode(_buffer.AsSpan(_start, _end - _start), out var entry))
                {
                    _current = entry.Length;
                    HasCurrent = true;
                    return true;
                }
                if (_eof)
                {
                    if (_end > _start)
                    {
                        throw new InvalidDataException("A sort run ends inside an entry");
                    }
                    HasCurrent = false;
                    return false;
                }
                Refill();
            }
        }

        private void Refill()
        {
            var remaining = _end - _start;
            if (_start == 0 && remaining == _buffer.Length)
            {
                // One entry is larger than the buffer: grow once to exactly that entry, whose leading lengths the buffer holds, before the old buffer is freed
                var grown = _memory.Allocate(checked((int)ArenaEntry.RequiredLength(_buffer)!.Value));
                _buffer.AsSpan(0, remaining).CopyTo(grown);
                _memory.Free(_buffer);
                _buffer = grown;
            }
            else if (_start > 0)
            {
                _buffer.AsSpan(_start, remaining).CopyTo(_buffer);
            }
            _start = 0;
            _end = remaining;
            var read = _file.Read(_buffer, _end, _buffer.Length - _end);
            if (read == 0)
            {
                _eof = true;
            }
            _end += read;
        }

        public void Dispose()
        {
            _file.Dispose();
            if (_buffer.Length > 0)
            {
                _memory.Free(_buffer);
                _buffer = Array.Empty<byte>();
            }
        }
    }
}
