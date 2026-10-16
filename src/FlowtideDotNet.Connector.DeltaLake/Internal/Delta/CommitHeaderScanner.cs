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

using System.Text;

namespace FlowtideDotNet.Connector.DeltaLake.Internal.Delta
{
    internal enum HeaderMode : byte
    {
        // Ordinary readers: invalid UTF-8 is not checked
        Lenient,
        // Cleanup and publication proofs: complete syntax, UTF-8, escapes, nesting and a line terminator
        Strict
    }

    [Flags]
    internal enum HeaderFlags : ushort
    {
        None = 0,
        DuplicateField = 1 << 0,
        InvalidStageId = 1 << 1,
        InvalidAdoptedAt = 1 << 2,
        InvalidTimestamp = 1 << 3,
        InvalidOperation = 1 << 4,
        InvalidCreatedFiles = 1 << 5
    }

    /// <summary>
    /// The fixed fields of a commit's first line, read without holding the line.
    /// </summary>
    internal sealed class CommitHeader
    {
        // The line is one object whose only member is a commitInfo object
        public bool IsCommitInfo { get; internal set; }

        // Null when the line is valid JSON
        public string? Error { get; internal set; }

        // The line ended with a line terminator, not the end of the file
        public bool Terminated { get; internal set; }

        public long Length { get; internal set; }

        public string? StageId { get; internal set; }

        public long? AdoptedAt { get; internal set; }

        public long? Timestamp { get; internal set; }

        public string? Operation { get; internal set; }

        public bool HasCreatedFiles { get; internal set; }

        public long CreatedFileCount { get; internal set; }

        public HeaderFlags Flags { get; internal set; }

        // The fixed fields that appeared more than once, as bits
        public int DuplicateFields { get; internal set; }

        /// <summary>
        /// The line with the created-files array emptied, when the scanner keeps it; it reads as the whole line did.
        /// </summary>
        public ReadOnlyMemory<byte> Residual { get; internal set; }

        public const int StageIdBit = 1 << 0;
        public const int AdoptedAtBit = 1 << 1;
        public const int TimestampBit = 1 << 2;
        public const int OperationBit = 1 << 3;
        public const int CreatedFilesBit = 1 << 4;

        public bool IsValid => Error == null;
    }

    /// <summary>
    /// Follows one JSON line byte by byte with bounded buffers: projects the commitInfo fixed fields and streams the created paths.
    /// </summary>
    internal sealed class CommitHeaderScanner
    {
        public const int MaxDepth = 64;
        public const int DefaultMaxPathBytes = 64 << 10;
        private const int MaxNameBytes = 64;
        private const int MaxValueBytes = 1024;
        private const int MaxNumberBytes = 32;

        private static readonly byte[] s_commitInfo = "commitInfo"u8.ToArray();
        private static readonly byte[][] s_fields =
        {
            "flowtide.stageId"u8.ToArray(),
            "flowtide.adoptedAt"u8.ToArray(),
            "timestamp"u8.ToArray(),
            "operation"u8.ToArray(),
            "flowtide.createdFiles"u8.ToArray()
        };

        private const int FieldStageId = 0;
        private const int FieldAdoptedAt = 1;
        private const int FieldTimestamp = 2;
        private const int FieldOperation = 3;
        private const int FieldCreatedFiles = 4;

        // Container states
        private const byte KeyOrEnd = 0;
        private const byte Key = 1;
        private const byte Colon = 2;
        private const byte ObjectValue = 3;
        private const byte ObjectCommaOrEnd = 4;
        private const byte ValueOrEnd = 5;
        private const byte ArrayValue = 6;
        private const byte ArrayCommaOrEnd = 7;

        // Top-level states
        private const byte TopValue = 0;
        private const byte TopDone = 1;

        // Token kinds
        private const byte NoToken = 0;
        private const byte StringToken = 1;
        private const byte NumberToken = 2;
        private const byte LiteralToken = 3;

        // What a string is captured as
        private const byte CaptureNone = 0;
        private const byte CaptureName = 1;
        private const byte CaptureValue = 2;
        private const byte CapturePath = 3;

        private readonly HeaderMode _mode;
        private readonly Action<string>? _onCreatedFile;
        private readonly int _maxPathBytes;
        private readonly bool _keepResidual;
        private byte[] _residual = Array.Empty<byte>();
        private int _residualLength;
        private readonly byte[] _states = new byte[MaxDepth + 1];
        private readonly byte[] _name = new byte[MaxNameBytes];
        private readonly byte[] _value = new byte[MaxValueBytes];
        private readonly byte[] _number = new byte[MaxNumberBytes];
        private byte[] _path = Array.Empty<byte>();

        private CommitHeader _header = new CommitHeader();
        private int _depth;
        private ulong _arrays;
        private byte _top;
        private bool _started;
        private bool _failed;

        // The open token
        private byte _token;
        private byte _capture;
        private int _captureLength;
        private bool _captureOverflow;
        private bool _escape;
        private int _hexLeft;
        private int _hexValue;
        private int _pendingHigh;
        private int _utf8Left;
        private byte _utf8Min;
        private byte _utf8Max;
        private int _numberPhase;
        private int _numberLength;
        private byte[]? _literal;
        private int _literalIndex;
        private byte _valueKind;

        // Where the projection is
        private int _topMembers;
        private bool _currentTopIsCommitInfo;
        private bool _sawCommitInfoObject;
        private int _commitInfoDepth = -1;
        private int _createdFilesDepth = -1;
        private int _field = -1;
        private int _seenFields;

        // Created paths reach the callback as they are read, before the line is known to be valid
        public CommitHeaderScanner(HeaderMode mode, Action<string>? onCreatedFile = null, int maxPathBytes = DefaultMaxPathBytes, bool keepResidual = false)
        {
            _mode = mode;
            _onCreatedFile = onCreatedFile;
            _maxPathBytes = maxPathBytes;
            _keepResidual = keepResidual;
        }

        // Inside the created-files array, past its opening bracket
        private bool InCreatedFiles => _createdFilesDepth > 0 && _depth >= _createdFilesDepth;

        private void KeepResidual(ReadOnlySpan<byte> bytes)
        {
            if (_residualLength + bytes.Length > _residual.Length)
            {
                Array.Resize(ref _residual, Math.Max(_residualLength + bytes.Length, Math.Max(256, _residual.Length * 2)));
            }
            bytes.CopyTo(_residual.AsSpan(_residualLength));
            _residualLength += bytes.Length;
        }

        public bool Failed => _failed;

        // The value tokens are kept by kind: what the last finished value was
        private const byte KindString = 1;
        private const byte KindNumber = 2;
        private const byte KindLiteral = 3;
        private const byte KindContainer = 4;

        // Bytes that end a plain run inside a string: a quote, an escape, a control character, and in strict mode any non-ASCII byte
        private static readonly System.Buffers.SearchValues<byte> s_lenientStops = System.Buffers.SearchValues.Create(StopBytes(strict: false));
        private static readonly System.Buffers.SearchValues<byte> s_strictStops = System.Buffers.SearchValues.Create(StopBytes(strict: true));

        private static byte[] StopBytes(bool strict)
        {
            var stops = new List<byte>() { (byte)'"', (byte)'\\' };
            for (int b = 0; b < 0x20; b++)
            {
                stops.Add((byte)b);
            }
            if (strict)
            {
                for (int b = 0x80; b <= 0xFF; b++)
                {
                    stops.Add((byte)b);
                }
            }
            return stops.ToArray();
        }

        /// <summary>
        /// Feeds bytes of the line, never its terminator.
        /// </summary>
        public void Feed(ReadOnlySpan<byte> bytes)
        {
            var stops = _mode == HeaderMode.Strict ? s_strictStops : s_lenientStops;
            var i = 0;
            while (i < bytes.Length && !_failed)
            {
                // A plain run inside a string is taken at once
                if (_token == StringToken && !_escape && _hexLeft == 0 && _utf8Left == 0 && _pendingHigh == 0)
                {
                    var rest = bytes.Slice(i);
                    var stop = rest.IndexOfAny(stops);
                    var run = stop < 0 ? rest : rest.Slice(0, stop);
                    if (run.Length > 0)
                    {
                        CaptureRun(run);
                        if (_keepResidual && !InCreatedFiles)
                        {
                            KeepResidual(run);
                        }
                        i += run.Length;
                        continue;
                    }
                }
                var inside = InCreatedFiles;
                Step(bytes[i]);
                // The array's own brackets stay, its content does not
                if (_keepResidual && (!inside || !InCreatedFiles))
                {
                    KeepResidual(bytes.Slice(i, 1));
                }
                i++;
            }
            _header.Length += bytes.Length;
        }

        private void CaptureRun(ReadOnlySpan<byte> run)
        {
            switch (_capture)
            {
                case CaptureName:
                    CopyBounded(run, _name, MaxNameBytes);
                    return;
                case CaptureValue:
                    CopyBounded(run, _value, MaxValueBytes);
                    return;
                case CapturePath:
                    var room = _maxPathBytes - _captureLength;
                    if (run.Length > room)
                    {
                        _captureOverflow = true;
                        run = run.Slice(0, room);
                    }
                    if (_captureLength + run.Length > _path.Length)
                    {
                        Array.Resize(ref _path, Math.Min(_maxPathBytes, Math.Max(_captureLength + run.Length, Math.Max(256, _path.Length * 2))));
                    }
                    run.CopyTo(_path.AsSpan(_captureLength));
                    _captureLength += run.Length;
                    return;
            }
        }

        private void CopyBounded(ReadOnlySpan<byte> run, byte[] target, int max)
        {
            var room = max - _captureLength;
            if (run.Length > room)
            {
                _captureOverflow = true;
                run = run.Slice(0, room);
            }
            run.CopyTo(target.AsSpan(_captureLength));
            _captureLength += run.Length;
        }

        /// <summary>
        /// Ends the line; terminated tells whether a line terminator followed it.
        /// </summary>
        public CommitHeader Complete(bool terminated)
        {
            var header = _header;
            header.Terminated = terminated;
            if (!_failed && _token == NumberToken)
            {
                EndNumber();
            }
            if (!_failed && (_token != NoToken || _top != TopDone || _depth != 0))
            {
                Fail("The line ends inside its JSON value");
            }
            if (!_failed && _mode == HeaderMode.Strict && !terminated)
            {
                Fail("The line has no terminator");
            }
            header.IsCommitInfo = !_failed && _topMembers == 1 && _sawCommitInfoObject;
            if (_keepResidual)
            {
                header.Residual = _residual.AsMemory(0, _residualLength);
            }
            return header;
        }

        private void Fail(string reason)
        {
            if (!_failed)
            {
                _failed = true;
                _header.Error = reason;
            }
        }

        private static bool IsSpace(byte b) => b is (byte)' ' or (byte)'\t';

        private void Step(byte b)
        {
            if (!_started)
            {
                _started = true;
                // A byte order mark is dropped as a line reader drops it, a proof never accepts one
                if (b == 0xEF)
                {
                    if (_mode == HeaderMode.Strict)
                    {
                        Fail("The line starts with a byte order mark");
                        return;
                    }
                    _literal = s_bom;
                    _literalIndex = 1;
                    _token = LiteralToken;
                    _bom = true;
                    return;
                }
            }
            switch (_token)
            {
                case StringToken:
                    StringByte(b);
                    return;
                case NumberToken:
                    if (NumberByte(b))
                    {
                        return;
                    }
                    EndNumber();
                    if (_failed)
                    {
                        return;
                    }
                    break;
                case LiteralToken:
                    LiteralByte(b);
                    return;
            }
            Structural(b);
        }

        private static readonly byte[] s_bom = { 0xEF, 0xBB, 0xBF };
        private static readonly byte[] s_true = "true"u8.ToArray();
        private static readonly byte[] s_false = "false"u8.ToArray();
        private static readonly byte[] s_null = "null"u8.ToArray();
        private bool _bom;

        private void LiteralByte(byte b)
        {
            if (b != _literal![_literalIndex])
            {
                Fail(_bom ? "The line starts with a broken byte order mark" : "Invalid literal");
                return;
            }
            _literalIndex++;
            if (_literalIndex == _literal.Length)
            {
                _token = NoToken;
                if (_bom)
                {
                    _bom = false;
                    return;
                }
                EndValue(KindLiteral);
            }
        }

        private void Structural(byte b)
        {
            if (IsSpace(b))
            {
                return;
            }
            if (_depth == 0)
            {
                if (_top == TopDone)
                {
                    Fail("The line has content after its JSON value");
                    return;
                }
                BeginValue(b);
                return;
            }
            var state = _states[_depth];
            switch (state)
            {
                case KeyOrEnd:
                case Key:
                    if (b == (byte)'"')
                    {
                        BeginString(CaptureName);
                    }
                    else if (b == (byte)'}' && state == KeyOrEnd)
                    {
                        CloseContainer();
                    }
                    else
                    {
                        Fail("Expected a member name");
                    }
                    return;
                case Colon:
                    if (b == (byte)':')
                    {
                        _states[_depth] = ObjectValue;
                    }
                    else
                    {
                        Fail("Expected a colon");
                    }
                    return;
                case ObjectValue:
                case ArrayValue:
                    BeginValue(b);
                    return;
                case ValueOrEnd:
                    if (b == (byte)']')
                    {
                        CloseContainer();
                    }
                    else
                    {
                        BeginValue(b);
                    }
                    return;
                case ObjectCommaOrEnd:
                    if (b == (byte)',')
                    {
                        _states[_depth] = Key;
                    }
                    else if (b == (byte)'}')
                    {
                        CloseContainer();
                    }
                    else
                    {
                        Fail("Expected a comma or the end of an object");
                    }
                    return;
                case ArrayCommaOrEnd:
                    if (b == (byte)',')
                    {
                        _states[_depth] = ArrayValue;
                    }
                    else if (b == (byte)']')
                    {
                        CloseContainer();
                    }
                    else
                    {
                        Fail("Expected a comma or the end of an array");
                    }
                    return;
            }
        }

        // A value starts at the current position
        private void BeginValue(byte b)
        {
            var field = ValueField();
            switch (b)
            {
                case (byte)'{':
                case (byte)'[':
                    if (_depth >= MaxDepth)
                    {
                        Fail("The line nests deeper than the limit");
                        return;
                    }
                    var parentIsTop = _depth == 1 && _currentTopIsCommitInfo;
                    _depth++;
                    if (b == (byte)'[')
                    {
                        _arrays |= 1UL << _depth;
                        _states[_depth] = ValueOrEnd;
                    }
                    else
                    {
                        _arrays &= ~(1UL << _depth);
                        _states[_depth] = KeyOrEnd;
                    }
                    if (parentIsTop && b == (byte)'{')
                    {
                        _commitInfoDepth = _depth;
                        _sawCommitInfoObject = true;
                    }
                    else if (field == FieldCreatedFiles && b == (byte)'[')
                    {
                        _createdFilesDepth = _depth;
                        _header.HasCreatedFiles = true;
                    }
                    else
                    {
                        WrongType(field);
                        if (_depth - 1 == _createdFilesDepth)
                        {
                            _header.Flags |= HeaderFlags.InvalidCreatedFiles;
                        }
                    }
                    return;
                case (byte)'"':
                    if (field is FieldStageId or FieldOperation)
                    {
                        BeginString(CaptureValue);
                    }
                    else if (_depth > 0 && _depth == _createdFilesDepth)
                    {
                        BeginString(CapturePath);
                    }
                    else
                    {
                        WrongType(field);
                        BeginString(CaptureNone);
                    }
                    return;
                case (byte)'-':
                case >= (byte)'0' and <= (byte)'9':
                    if (field is not (FieldAdoptedAt or FieldTimestamp))
                    {
                        WrongType(field);
                    }
                    _token = NumberToken;
                    _numberPhase = 0;
                    _numberLength = 0;
                    NumberByte(b);
                    return;
                case (byte)'t':
                case (byte)'f':
                case (byte)'n':
                    WrongType(field);
                    _token = LiteralToken;
                    _literal = b == (byte)'t' ? s_true : b == (byte)'f' ? s_false : s_null;
                    _literalIndex = 1;
                    return;
                default:
                    Fail("Expected a value");
                    return;
            }
        }

        // The fixed field whose value starts here, -1 elsewhere
        private int ValueField()
        {
            if (_depth > 0 && _depth == _commitInfoDepth && _states[_depth] == ObjectValue)
            {
                return _field;
            }
            if (_depth > 0 && _depth == _createdFilesDepth)
            {
                // Array elements are not fields, a non-string one is flagged by the caller
                if (_depth > 0 && _states[_depth] is ValueOrEnd or ArrayValue)
                {
                    return -2;
                }
            }
            return -1;
        }

        // A fixed field or a created path with a value of another type
        private void WrongType(int field)
        {
            switch (field)
            {
                case FieldStageId:
                    _header.StageId = null;
                    _header.Flags |= HeaderFlags.InvalidStageId;
                    break;
                case FieldAdoptedAt:
                    _header.AdoptedAt = null;
                    _header.Flags |= HeaderFlags.InvalidAdoptedAt;
                    break;
                case FieldTimestamp:
                    _header.Timestamp = null;
                    _header.Flags |= HeaderFlags.InvalidTimestamp;
                    break;
                case FieldOperation:
                    _header.Operation = null;
                    _header.Flags |= HeaderFlags.InvalidOperation;
                    break;
                case FieldCreatedFiles:
                case -2:
                    _header.Flags |= HeaderFlags.InvalidCreatedFiles;
                    break;
            }
        }

        private void CloseContainer()
        {
            if (_depth == _commitInfoDepth)
            {
                _commitInfoDepth = -1;
            }
            if (_depth == _createdFilesDepth)
            {
                _createdFilesDepth = -1;
            }
            _depth--;
            EndValue(KindContainer);
        }

        // The value at the current position finished
        private void EndValue(byte kind)
        {
            _valueKind = kind;
            if (_depth == 0)
            {
                _top = TopDone;
                return;
            }
            if (((_arrays >> _depth) & 1) == 1)
            {
                _states[_depth] = ArrayCommaOrEnd;
            }
            else
            {
                if (_depth == _commitInfoDepth)
                {
                    _field = -1;
                }
                _states[_depth] = ObjectCommaOrEnd;
            }
        }

        private void BeginString(byte capture)
        {
            _token = StringToken;
            _capture = capture;
            _captureLength = 0;
            _captureOverflow = false;
            _escape = false;
            _hexLeft = 0;
            _pendingHigh = 0;
            _utf8Left = 0;
        }

        private void StringByte(byte b)
        {
            if (_hexLeft > 0)
            {
                int digit = b switch
                {
                    >= (byte)'0' and <= (byte)'9' => b - '0',
                    >= (byte)'a' and <= (byte)'f' => b - 'a' + 10,
                    >= (byte)'A' and <= (byte)'F' => b - 'A' + 10,
                    _ => -1
                };
                if (digit < 0)
                {
                    Fail("Invalid unicode escape");
                    return;
                }
                _hexValue = (_hexValue << 4) | digit;
                _hexLeft--;
                if (_hexLeft == 0)
                {
                    CodeUnit(_hexValue);
                }
                return;
            }
            if (_escape)
            {
                _escape = false;
                byte decoded;
                switch (b)
                {
                    case (byte)'"': decoded = (byte)'"'; break;
                    case (byte)'\\': decoded = (byte)'\\'; break;
                    case (byte)'/': decoded = (byte)'/'; break;
                    case (byte)'b': decoded = 0x08; break;
                    case (byte)'f': decoded = 0x0C; break;
                    case (byte)'n': decoded = (byte)'\n'; break;
                    case (byte)'r': decoded = (byte)'\r'; break;
                    case (byte)'t': decoded = (byte)'\t'; break;
                    case (byte)'u':
                        _hexLeft = 4;
                        _hexValue = 0;
                        return;
                    default:
                        Fail("Invalid escape");
                        return;
                }
                if (!FlushHigh())
                {
                    return;
                }
                Capture(decoded);
                return;
            }
            if (_utf8Left > 0)
            {
                if (b < _utf8Min || b > _utf8Max)
                {
                    Fail("Invalid UTF-8");
                    return;
                }
                _utf8Min = 0x80;
                _utf8Max = 0xBF;
                _utf8Left--;
                Capture(b);
                return;
            }
            if (b == (byte)'"')
            {
                if (!FlushHigh())
                {
                    return;
                }
                EndString();
                return;
            }
            if (b == (byte)'\\')
            {
                _escape = true;
                return;
            }
            if (b < 0x20)
            {
                Fail("A control character inside a string");
                return;
            }
            if (!FlushHigh())
            {
                return;
            }
            if (b >= 0x80 && _mode == HeaderMode.Strict && !Utf8Lead(b))
            {
                return;
            }
            Capture(b);
        }

        // A strict lead byte and the range of its first continuation byte
        private bool Utf8Lead(byte b)
        {
            switch (b)
            {
                case >= 0xC2 and <= 0xDF:
                    _utf8Left = 1; _utf8Min = 0x80; _utf8Max = 0xBF; return true;
                case 0xE0:
                    _utf8Left = 2; _utf8Min = 0xA0; _utf8Max = 0xBF; return true;
                case >= 0xE1 and <= 0xEC:
                case 0xEE:
                case 0xEF:
                    _utf8Left = 2; _utf8Min = 0x80; _utf8Max = 0xBF; return true;
                case 0xED:
                    _utf8Left = 2; _utf8Min = 0x80; _utf8Max = 0x9F; return true;
                case 0xF0:
                    _utf8Left = 3; _utf8Min = 0x90; _utf8Max = 0xBF; return true;
                case >= 0xF1 and <= 0xF3:
                    _utf8Left = 3; _utf8Min = 0x80; _utf8Max = 0xBF; return true;
                case 0xF4:
                    _utf8Left = 3; _utf8Min = 0x80; _utf8Max = 0x8F; return true;
                default:
                    Fail("Invalid UTF-8");
                    return false;
            }
        }

        // A high surrogate escape followed by anything but a low one is lone
        private bool FlushHigh()
        {
            if (_pendingHigh == 0)
            {
                return true;
            }
            _pendingHigh = 0;
            return LoneSurrogate();
        }

        private bool LoneSurrogate()
        {
            if (_mode == HeaderMode.Strict)
            {
                Fail("A lone surrogate escape");
                return false;
            }
            Capture(0xEF);
            Capture(0xBF);
            Capture(0xBD);
            return true;
        }

        private void CodeUnit(int unit)
        {
            if (unit is >= 0xD800 and <= 0xDBFF)
            {
                if (_pendingHigh != 0 && !LoneSurrogate())
                {
                    return;
                }
                _pendingHigh = unit;
                return;
            }
            if (unit is >= 0xDC00 and <= 0xDFFF)
            {
                if (_pendingHigh == 0)
                {
                    LoneSurrogate();
                    return;
                }
                var scalar = 0x10000 + ((_pendingHigh - 0xD800) << 10) + (unit - 0xDC00);
                _pendingHigh = 0;
                CaptureScalar(scalar);
                return;
            }
            if (!FlushHigh())
            {
                return;
            }
            CaptureScalar(unit);
        }

        private void CaptureScalar(int scalar)
        {
            if (_capture == CaptureNone)
            {
                return;
            }
            Span<byte> utf8 = stackalloc byte[4];
            var length = new Rune(scalar).EncodeToUtf8(utf8);
            for (int i = 0; i < length; i++)
            {
                Capture(utf8[i]);
            }
        }

        private void Capture(byte b)
        {
            switch (_capture)
            {
                case CaptureName:
                    if (_captureLength < MaxNameBytes)
                    {
                        _name[_captureLength++] = b;
                    }
                    else
                    {
                        _captureOverflow = true;
                    }
                    return;
                case CaptureValue:
                    if (_captureLength < MaxValueBytes)
                    {
                        _value[_captureLength++] = b;
                    }
                    else
                    {
                        _captureOverflow = true;
                    }
                    return;
                case CapturePath:
                    if (_captureLength >= _maxPathBytes)
                    {
                        _captureOverflow = true;
                        return;
                    }
                    if (_captureLength == _path.Length)
                    {
                        Array.Resize(ref _path, Math.Min(_maxPathBytes, Math.Max(256, _path.Length * 2)));
                    }
                    _path[_captureLength++] = b;
                    return;
            }
        }

        private void EndString()
        {
            _token = NoToken;
            if (_utf8Left > 0)
            {
                Fail("Invalid UTF-8");
                return;
            }
            switch (_capture)
            {
                case CaptureName:
                    _states[_depth] = Colon;
                    Name();
                    return;
                case CaptureValue:
                    var text = _captureOverflow ? null : Encoding.UTF8.GetString(_value, 0, _captureLength);
                    if (_field == FieldStageId)
                    {
                        _header.StageId = text;
                        if (text == null)
                        {
                            _header.Flags |= HeaderFlags.InvalidStageId;
                        }
                    }
                    else
                    {
                        _header.Operation = text;
                        if (text == null)
                        {
                            _header.Flags |= HeaderFlags.InvalidOperation;
                        }
                    }
                    break;
                case CapturePath:
                    _header.CreatedFileCount++;
                    if (_captureOverflow || _captureLength == 0)
                    {
                        _header.Flags |= HeaderFlags.InvalidCreatedFiles;
                    }
                    else if (_onCreatedFile != null && (_header.Flags & HeaderFlags.InvalidCreatedFiles) == 0 && (_header.DuplicateFields & CommitHeader.CreatedFilesBit) == 0)
                    {
                        _onCreatedFile(Encoding.UTF8.GetString(_path, 0, _captureLength));
                    }
                    break;
            }
            EndValue(KindString);
        }

        // A member name finished: the top member or a commitInfo field
        private void Name()
        {
            var name = _captureOverflow ? ReadOnlySpan<byte>.Empty : _name.AsSpan(0, _captureLength);
            if (_depth == 1)
            {
                _topMembers++;
                _currentTopIsCommitInfo = _topMembers == 1 && !_captureOverflow && name.SequenceEqual(s_commitInfo);
                return;
            }
            if (_depth == _commitInfoDepth)
            {
                _field = -1;
                if (_captureOverflow)
                {
                    return;
                }
                for (int f = 0; f < s_fields.Length; f++)
                {
                    if (name.SequenceEqual(s_fields[f]))
                    {
                        _field = f;
                        var bit = 1 << f;
                        if ((_seenFields & bit) != 0)
                        {
                            _header.Flags |= HeaderFlags.DuplicateField;
                            _header.DuplicateFields |= bit;
                        }
                        _seenFields |= bit;
                        return;
                    }
                }
            }
        }

        // True while the byte continues the number
        private bool NumberByte(byte b)
        {
            var digit = b is >= (byte)'0' and <= (byte)'9';
            int next;
            switch (_numberPhase)
            {
                case 0:
                    next = b == (byte)'-' ? 1 : b == (byte)'0' ? 2 : digit ? 3 : -1;
                    break;
                case 1:
                    next = b == (byte)'0' ? 2 : digit ? 3 : -1;
                    break;
                case 2:
                    next = b == (byte)'.' ? 4 : b is (byte)'e' or (byte)'E' ? 6 : 0;
                    break;
                case 3:
                    next = digit ? 3 : b == (byte)'.' ? 4 : b is (byte)'e' or (byte)'E' ? 6 : 0;
                    break;
                case 4:
                    next = digit ? 5 : -1;
                    break;
                case 5:
                    next = digit ? 5 : b is (byte)'e' or (byte)'E' ? 6 : 0;
                    break;
                case 6:
                    next = b is (byte)'+' or (byte)'-' ? 7 : digit ? 8 : -1;
                    break;
                case 7:
                    next = digit ? 8 : -1;
                    break;
                case 8:
                    next = digit ? 8 : 0;
                    break;
                default:
                    next = -1;
                    break;
            }
            if (next < 0)
            {
                Fail("Invalid number");
                return true;
            }
            if (next == 0)
            {
                return false;
            }
            _numberPhase = next;
            if (_numberLength < MaxNumberBytes)
            {
                _number[_numberLength] = b;
            }
            _numberLength++;
            return true;
        }

        private void EndNumber()
        {
            _token = NoToken;
            if (_numberPhase is not (2 or 3 or 5 or 8))
            {
                Fail("Invalid number");
                return;
            }
            if (_depth > 0 && _depth == _commitInfoDepth && _field is FieldAdoptedAt or FieldTimestamp)
            {
                long? value = null;
                if (_numberPhase is 2 or 3 && _numberLength <= MaxNumberBytes && long.TryParse(_number.AsSpan(0, _numberLength), System.Globalization.NumberStyles.AllowLeadingSign, System.Globalization.CultureInfo.InvariantCulture, out var parsed))
                {
                    value = parsed;
                }
                if (_field == FieldAdoptedAt)
                {
                    _header.AdoptedAt = value;
                    if (value == null)
                    {
                        _header.Flags |= HeaderFlags.InvalidAdoptedAt;
                    }
                }
                else
                {
                    _header.Timestamp = value;
                    if (value == null)
                    {
                        _header.Flags |= HeaderFlags.InvalidTimestamp;
                    }
                }
            }
            EndValue(KindNumber);
        }
    }
}
