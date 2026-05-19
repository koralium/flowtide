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

using Apache.Arrow.Ipc;
using System;
using System.Buffers;
using System.Collections.Generic;
using System.IO.Pipelines;
using System.Linq;
using System.Numerics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Runtime.Intrinsics;
using System.Text;
using System.Threading.Tasks;

namespace FlowtideDotNet.Connector.Files.Internal.CsvFiles.Parser
{
    public class VectorizedCsvParser
    {
        private readonly PipeReader _reader;
        private readonly byte _delimiter;

        // Memory pooling for columns and cross-segment stitching
        private readonly Range[] _columnBuffer = new Range[1024]; // Max columns per row
        private byte[] _crossSegmentBuffer = new byte[128 * 1024]; // Starts at 128KB, grows dynamically

        // Network/File Cursor State
        private ReadOnlySequence<byte> _buffer;
        private SequencePosition _consumed;
        private bool _hasActiveBuffer;
        private bool _isEof;

        public VectorizedCsvParser(PipeReader reader, byte delimiter = (byte)',')
        {
            _reader = reader;
            _delimiter = delimiter;
        }

        public async ValueTask<bool> FetchMoreDataAsync()
        {
            if (_isEof)
            {
                if (_hasActiveBuffer)
                {
                    _reader.AdvanceTo(_consumed, _buffer.End);
                    _hasActiveBuffer = false; // Prevent double-advancing
                }
                return false;
            }
            if (_hasActiveBuffer)
            {
                // Signal backpressure: we consumed up to _consumed, but examined the whole buffer
                _reader.AdvanceTo(_consumed, _buffer.End);
            }

            ReadResult result = await _reader.ReadAsync();
            _buffer = result.Buffer;
            _consumed = _buffer.Start;
            _hasActiveBuffer = true;
            _isEof = result.IsCompleted;

            // Keep returning true until both the stream is dead AND we've parsed every byte
            return !(_isEof && _buffer.IsEmpty);
        }

        public bool TryParseBatch<TReader>(ref TReader reader) where TReader : struct, ICsvRowReader
        {
            ReadOnlySequence<byte> unread = _buffer.Slice(_consumed);
            SequencePosition currentSegmentStart = unread.Start;
            SequencePosition nextPosition = currentSegmentStart;

            while (unread.TryGet(ref nextPosition, out ReadOnlyMemory<byte> memory))
            {
                ReadOnlySpan<byte> span = memory.Span;
                int position = 0;
                int length = span.Length;
                int rowStart = 0;
                bool rowHasQuotes = false;

                ref byte dataRef = ref MemoryMarshal.GetReference(span);
                Vector256<byte> vNewline = Vector256.Create((byte)'\n');
                Vector256<byte> vQuote = Vector256.Create((byte)'"');

                // ==========================================
                // 1. THE HOT PATH (SIMD Loop)
                // ==========================================
                while (position + 32 <= length)
                {
                    Vector256<byte> chunk = Vector256.LoadUnsafe(ref dataRef, (nuint)position);

                    // Poison Bit (Quotes)
                    uint quoteMask = Vector256.Equals(chunk, vQuote).ExtractMostSignificantBits();
                    if (quoteMask != 0)
                    {
                        rowHasQuotes = true;
                        position = SkipQuoteBlock(ref dataRef, length, position + BitOperations.TrailingZeroCount(quoteMask));
                        continue;
                    }

                    // Structural Row Endings
                    uint newlineMask = Vector256.Equals(chunk, vNewline).ExtractMostSignificantBits();

                    while (newlineMask != 0)
                    {
                        int offset = BitOperations.TrailingZeroCount(newlineMask);
                        int newlineIdx = position + offset;

                        int rowEnd = newlineIdx;
                        if (rowEnd > rowStart && Unsafe.Add(ref dataRef, rowEnd - 1) == (byte)'\r') rowEnd--;

                        ReadOnlySpan<byte> rowSpan = span.Slice(rowStart, rowEnd - rowStart);

                        ParseRowColumns(rowSpan, _columnBuffer, out int colCount, rowHasQuotes);
                        var rowMemory = memory.Slice(rowStart, rowEnd - rowStart);
                        bool isBatchFull = reader.ProcessRow(rowMemory, new ReadOnlySpan<Range>(_columnBuffer, 0, colCount));

                        rowStart = newlineIdx + 1;
                        rowHasQuotes = false;
                        _consumed = unread.GetPosition(rowStart, currentSegmentStart);

                        if (isBatchFull) return true;

                        newlineMask &= (newlineMask - 1);
                    }
                    position += 32;
                }

                // ==========================================
                // 2. THE SCALAR TAIL (< 32 bytes remaining)
                // ==========================================
                while (position < length)
                {
                    byte c = Unsafe.Add(ref dataRef, position);
                    if (c == (byte)'"') rowHasQuotes = true;

                    if (c == (byte)'\n')
                    {
                        int rowEnd = position;
                        if (rowEnd > rowStart && Unsafe.Add(ref dataRef, rowEnd - 1) == (byte)'\r') rowEnd--;

                        ReadOnlySpan<byte> rowSpan = span.Slice(rowStart, rowEnd - rowStart);

                        ParseRowColumns(rowSpan, _columnBuffer, out int colCount, rowHasQuotes);
                        var rowMemory = memory.Slice(rowStart, rowEnd - rowStart);
                        bool isBatchFull = reader.ProcessRow(rowMemory, new ReadOnlySpan<Range>(_columnBuffer, 0, colCount));

                        rowStart = position + 1;
                        rowHasQuotes = false;
                        _consumed = unread.GetPosition(rowStart, currentSegmentStart);

                        if (isBatchFull) return true;
                    }
                    position++;
                }

                // ==========================================
                // 3. THE SEGMENT BOUNDARY STITCHER
                // ==========================================
                if (rowStart < length)
                {
                    if (nextPosition.GetObject() == null)
                    {
                        // THE FIX: EOF Flush for final rows lacking a \n
                        if (_isEof)
                        {
                            var remainingSequence = unread.Slice(unread.GetPosition(rowStart, currentSegmentStart));
                            int remainingLength = (int)remainingSequence.Length;

                            if (remainingLength > _crossSegmentBuffer.Length)
                            {
                                Array.Resize(ref _crossSegmentBuffer, Math.Max(_crossSegmentBuffer.Length * 2, remainingLength));
                            }

                            remainingSequence.CopyTo(_crossSegmentBuffer.AsSpan(0, remainingLength));

                            int rowEnd = remainingLength;
                            if (rowEnd > 0 && _crossSegmentBuffer[rowEnd - 1] == (byte)'\r') rowEnd--;

                            ReadOnlySpan<byte> rowSpan = _crossSegmentBuffer.AsSpan(0, rowEnd);

                            ParseRowColumns(rowSpan, _columnBuffer, out int colCount, true);
                            var rowMemory = _crossSegmentBuffer.AsMemory(0, rowEnd);
                            bool isBatchFull = reader.ProcessRow(rowMemory, new ReadOnlySpan<Range>(_columnBuffer, 0, colCount));

                            _consumed = _buffer.End;

                            if (isBatchFull) return true;
                        }

                        // We are truly out of data. Break to let FetchMoreDataAsync pull from OS/network.
                        break;
                    }

                    // A split row crossing into the next segment
                    var crossSequence = unread.Slice(unread.GetPosition(rowStart, currentSegmentStart));
                    var seqReader = new SequenceReader<byte>(crossSequence);

                    if (seqReader.TryReadTo(out ReadOnlySequence<byte> splitRow, (byte)'\n'))
                    {
                        int splitLength = (int)splitRow.Length;

                        // Dynamic Buffer Expansion
                        if (splitLength > _crossSegmentBuffer.Length)
                        {
                            Array.Resize(ref _crossSegmentBuffer, Math.Max(_crossSegmentBuffer.Length * 2, splitLength));
                        }

                        splitRow.CopyTo(_crossSegmentBuffer.AsSpan(0, splitLength));

                        int rowEnd = splitLength;
                        if (rowEnd > 0 && _crossSegmentBuffer[rowEnd - 1] == (byte)'\r') rowEnd--;

                        ReadOnlySpan<byte> rowSpan = _crossSegmentBuffer.AsSpan(0, rowEnd);

                        ParseRowColumns(rowSpan, _columnBuffer, out int colCount, true);
                        var rowMemory = _crossSegmentBuffer.AsMemory(0, rowEnd);
                        bool isBatchFull = reader.ProcessRow(rowMemory, new ReadOnlySpan<Range>(_columnBuffer, 0, colCount));

                        _consumed = unread.GetPosition(seqReader.Consumed);

                        if (isBatchFull) return true;

                        // Reset loop state to continue processing the new segment immediately
                        unread = _buffer.Slice(_consumed);
                        nextPosition = unread.Start;
                        currentSegmentStart = nextPosition;
                        continue;
                    }
                    else
                    {
                        // Split row goes into the next segment, but STILL lacks a newline. Fetch more.
                        break;
                    }
                }

                currentSegmentStart = nextPosition;
            }

            return false;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void ParseRowColumns(ReadOnlySpan<byte> row, Span<Range> columns, out int columnCount, bool hasQuotes)
        {
            if (!hasQuotes) ParseRowColumnsFast(row, columns, out columnCount);
            else ParseRowColumnsWithQuotes(row, columns, out columnCount);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void ParseRowColumnsFast(ReadOnlySpan<byte> row, Span<Range> columns, out int columnCount)
        {
            columnCount = 0;
            int position = 0, start = 0, len = row.Length;
            ref byte dataRef = ref MemoryMarshal.GetReference(row);

            // Uses the dynamic delimiter injected via the constructor
            Vector256<byte> vDelimiter = Vector256.Create(_delimiter);

            while (position + 32 <= len)
            {
                Vector256<byte> chunk = Vector256.LoadUnsafe(ref dataRef, (nuint)position);
                uint delimiterMask = Vector256.Equals(chunk, vDelimiter).ExtractMostSignificantBits();

                while (delimiterMask != 0)
                {
                    int offset = BitOperations.TrailingZeroCount(delimiterMask);
                    columns[columnCount++] = new Range(start, position + offset);
                    start = position + offset + 1;
                    delimiterMask &= (delimiterMask - 1);
                }
                position += 32;
            }

            while (position < len)
            {
                if (Unsafe.Add(ref dataRef, position) == _delimiter)
                {
                    columns[columnCount++] = new Range(start, position);
                    start = position + 1;
                }
                position++;
            }

            columns[columnCount++] = new Range(start, len);
        }

        private void ParseRowColumnsWithQuotes(ReadOnlySpan<byte> row, Span<Range> columns, out int columnCount)
        {
            columnCount = 0;
            int start = 0, len = row.Length;
            bool inQuotes = false;
            ref byte dataRef = ref MemoryMarshal.GetReference(row);

            for (int i = 0; i < len; i++)
            {
                byte c = Unsafe.Add(ref dataRef, i);
                if (c == (byte)'"')
                {
                    if (i + 1 < len && Unsafe.Add(ref dataRef, i + 1) == (byte)'"') i++;
                    else inQuotes = !inQuotes;
                }
                else if (c == _delimiter && !inQuotes)
                {
                    columns[columnCount++] = new Range(start, i);
                    start = i + 1;
                }
            }
            columns[columnCount++] = new Range(start, len);
        }

        private int SkipQuoteBlock(ref byte dataRef, int length, int currentPos)
        {
            currentPos++;
            while (currentPos < length)
            {
                if (Unsafe.Add(ref dataRef, currentPos) == (byte)'"')
                {
                    if (currentPos + 1 < length && Unsafe.Add(ref dataRef, currentPos + 1) == (byte)'"') currentPos += 2;
                    else return currentPos + 1;
                }
                else currentPos++;
            }
            return currentPos;
        }
    }
}
