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

using System.Buffers;
using System.Collections;
using System.Numerics;
using System.Text;

namespace FlexBuffers
{
    public class FlexBuffer
    {
        [Flags]
        public enum Options: byte
        {
            None = 0,
            ShareKeys = 1,
            ShareStrings = 1 << 1,
            ShareKeyVectors = 1 << 2,
        } 
        private readonly List<StackValue> _stack = new List<StackValue>();
        private readonly Dictionary<string, ulong> _stringCache = new Dictionary<string, ulong>();
        private readonly Dictionary<string, ulong> _keyCache = new Dictionary<string, ulong>();
        private readonly Dictionary<long[], StackValue> _keyVectorCache = new Dictionary<long[], StackValue>(new OffsetArrayComparer());
        private byte[]? _bytes;
        private ulong _size = 2048;
        private ulong _offset;
        private readonly ArrayPool<byte> pool;
        private readonly Options _options;
        private bool _finished = false;

        public FlexBuffer(ArrayPool<byte> pool, ulong size = 2048, Options options = Options.ShareKeys | Options.ShareKeyVectors)
        {
            if (size > 0)
            {
                _size = size;
            }
            _offset = 0;
            this.pool = pool;
            _options = options;
        }

        public void NewObject()
        {
            _bytes = pool.Rent((int)_size);
            Array.Clear(_bytes);
        }

        public void Clear()
        {
            if (_bytes != null)
            {
                pool.Return(_bytes);
            }
            _offset = 0;
            _finished = false;
            _stack.Clear();
            _stringCache.Clear();
            _keyCache.Clear();
            _keyVectorCache.Clear();
        }

        public static byte[] Null()
        {
            var buffer = new FlexBuffer(ArrayPool<byte>.Shared, 3);
            buffer.NewObject();
            buffer.AddNull();
            return buffer.Finish();
        }
        
        public static byte[] SingleValue(long value)
        {
            var buffer = new FlexBuffer(ArrayPool<byte>.Shared, 10);
            buffer.NewObject();
            buffer.Add(value);
            return buffer.Finish();
        }

        public static byte[] SingleValue(FlxValue value)
        {
            var buffer = new FlexBuffer(ArrayPool<byte>.Shared);
            buffer.NewObject();
            buffer.Add(value);
            return buffer.Finish();
        }

        public static byte[] SingleValue(ulong value)
        {
            var buffer = new FlexBuffer(ArrayPool<byte>.Shared, 10);
            buffer.NewObject();
            buffer.Add(value);
            return buffer.Finish();
        }
        
        public static byte[] SingleValue(double value)
        {
            var buffer = new FlexBuffer(ArrayPool<byte>.Shared, 10);
            buffer.NewObject();
            buffer.Add(value);
            return buffer.Finish();
        }
        
        public static byte[] SingleValue(bool value)
        {
            var buffer = new FlexBuffer(ArrayPool<byte>.Shared, 3);
            buffer.NewObject();
            buffer.Add(value);
            return buffer.Finish();
        }
        
        public static byte[] SingleValue(string value)
        {
            var buffer = new FlexBuffer(ArrayPool<byte>.Shared, (ulong)value.Length + 2);
            buffer.NewObject();
            buffer.Add(value);
            return buffer.Finish();
        }
        
        public static byte[] SingleValue(long x, long y)
        {
            var buffer = new FlexBuffer(ArrayPool<byte>.Shared, 20);
            buffer.NewObject();
            var start = buffer.StartVector();
            buffer.Add(x);
            buffer.Add(y);
            buffer.EndVector(start, true, true);
            return buffer.Finish();
        }
        
        public static byte[] SingleValue(long x, long y, long z)
        {
            var buffer = new FlexBuffer(ArrayPool<byte>.Shared, 28);
            buffer.NewObject();
            var start = buffer.StartVector();
            buffer.Add(x);
            buffer.Add(y);
            buffer.Add(z);
            buffer.EndVector(start, true, true);
            return buffer.Finish();
        }
        
        public static byte[] SingleValue(long x, long y, long z, long w)
        {
            var buffer = new FlexBuffer(ArrayPool<byte>.Shared, 36);
            buffer.NewObject();
            var start = buffer.StartVector();
            buffer.Add(x);
            buffer.Add(y);
            buffer.Add(z);
            buffer.Add(w);
            buffer.EndVector(start, true, true);
            return buffer.Finish();
        }
        
        public static byte[] SingleValue(ulong x, ulong y)
        {
            var buffer = new FlexBuffer(ArrayPool<byte>.Shared, 20);
            buffer.NewObject();
            var start = buffer.StartVector();
            buffer.Add(x);
            buffer.Add(y);
            buffer.EndVector(start, true, true);
            return buffer.Finish();
        }
        
        public static byte[] SingleValue(ulong x, ulong y, ulong z)
        {
            var buffer = new FlexBuffer(ArrayPool<byte>.Shared, 28);
            buffer.NewObject();
            var start = buffer.StartVector();
            buffer.Add(x);
            buffer.Add(y);
            buffer.Add(z);
            buffer.EndVector(start, true, true);
            return buffer.Finish();
        }
        
        public static byte[] SingleValue(ulong x, ulong y, ulong z, ulong w)
        {
            var buffer = new FlexBuffer(ArrayPool<byte>.Shared, 36);
            buffer.NewObject();
            var start = buffer.StartVector();
            buffer.Add(x);
            buffer.Add(y);
            buffer.Add(z);
            buffer.Add(w);
            buffer.EndVector(start, true, true);
            return buffer.Finish();
        }
        
        public static byte[] SingleValue(double x, double y)
        {
            var buffer = new FlexBuffer(ArrayPool<byte>.Shared, 20);
            buffer.NewObject();
            var start = buffer.StartVector();
            buffer.Add(x);
            buffer.Add(y);
            buffer.EndVector(start, true, true);
            return buffer.Finish();
        }
        
        public static byte[] SingleValue(double x, double y, double z)
        {
            var buffer = new FlexBuffer(ArrayPool<byte>.Shared, 28);
            buffer.NewObject();
            var start = buffer.StartVector();
            buffer.Add(x);
            buffer.Add(y);
            buffer.Add(z);
            buffer.EndVector(start, true, true);
            return buffer.Finish();
        }
        
        public static byte[] SingleValue(double x, double y, double z, double w)
        {
            var buffer = new FlexBuffer(ArrayPool<byte>.Shared, 36);
            buffer.NewObject();
            var start = buffer.StartVector();
            buffer.Add(x);
            buffer.Add(y);
            buffer.Add(z);
            buffer.Add(w);
            buffer.EndVector(start, true, true);
            return buffer.Finish();
        }
        
        public static byte[] SingleValue(byte[] blob)
        {
            var buffer = new FlexBuffer(ArrayPool<byte>.Shared, (ulong)blob.Length + 10);
            buffer.NewObject();
            buffer.Add(blob);
            return buffer.Finish();
        }

        public static byte[] From(IEnumerable value, Options options = Options.ShareKeys | Options.ShareStrings | Options.ShareKeyVectors)
        {
            var buffer = new FlexBuffer(ArrayPool<byte>.Shared, options:options);
            buffer.NewObject();
            if (value is IDictionary dictionary)
            {
                buffer.AddDynamicMap(dictionary);
            }
            else
            {
                buffer.AddDynamicVector(value);    
            }
            
            return buffer.Finish();
        }

        public byte[] Finish()
        {
            if (_finished == false)
            {
                FinishBuffer();
            }

            var result = new byte[_offset];
            Buffer.BlockCopy(_bytes, 0, result, 0, (int)_offset);
            pool.Return(_bytes);
            _bytes = null;
            Clear();
            return result;
        }

        private void FinishBuffer()
        {
            if (_finished)
            {
                throw  new Exception("FlexBuffer is already finished");
            }

            if (_stack.Count != 1)
            {
                throw new Exception("Stack needs to be exactly 1");
            }

            var value = _stack[0];

            var byteWidth = Align(value.ElementWidth(_offset, 0));
            
            Write(value, byteWidth);
            Write(value.StoredPackedType());
            Write(byteWidth);
            _finished = true;
        }
        
        internal Type AddNull()
        {
            _stack.Add(StackValue.Null());
            return Type.Null;
        }

        internal Type Add(long value)
        {
            _stack.Add(StackValue.Value(value));
            return Type.Int;
        }
        
        internal Type AddIndirect(long value)
        {
            var type = Type.IndirectInt;
            var bitWidth = BitWidthUtil.Width(value);
            var byteWidth = Align(bitWidth);
            var valueOffset = _offset;
            Write(value, byteWidth);
            _stack.Add(StackValue.Value(valueOffset, bitWidth, type));
            return type;
        }

        internal Type Add(ulong value)
        {
            _stack.Add(StackValue.Value(value));
            return Type.Uint;
        }
        
        internal Type AddIndirect(ulong value)
        {
            var type = Type.IndirectUInt;
            var bitWidth = BitWidthUtil.Width(value);
            var byteWidth = Align(bitWidth);
            var valueOffset = _offset;
            Write(value, byteWidth);
            _stack.Add(StackValue.Value(valueOffset, bitWidth, type));
            return type;
        }
        
        internal Type Add(double value)
        {
            _stack.Add(StackValue.Value(value));
            return Type.Float;
        }
        
        internal Type AddIndirect(double value)
        {
            var type = Type.IndirectFloat;
            var bitWidth = BitWidthUtil.Width(value);
            var byteWidth = Align(bitWidth);
            var valueOffset = _offset;
            Write(value, byteWidth);
            _stack.Add(StackValue.Value(valueOffset, bitWidth, type));
            return type;
        }
        
        internal Type Add(bool value)
        {
            _stack.Add(StackValue.Value(value));
            return Type.Bool;
        }

        internal Type Add(string value)
        {

            var bytes = Encoding.UTF8.GetBytes(value);
            var length = (ulong)bytes.Length;
            var bitWidth = BitWidthUtil.Width(length);
            if (_options.HasFlag(Options.ShareStrings) && _stringCache.ContainsKey(value))
            {
                _stack.Add(StackValue.Value(_stringCache[value], bitWidth, Type.String));
                return Type.String;
            }
            var byteWidth = Align(bitWidth);
            Write(length, byteWidth);
            var stringOffset = _offset;
            var newOffset = NewOffset(length + 1);
            Buffer.BlockCopy(bytes, 0, _bytes, (int)_offset, (int)length);
            _bytes[(int)newOffset - 1] = 0;
            _offset = newOffset;
            _stack.Add(StackValue.Value(stringOffset, bitWidth, Type.String));
            if (_options.HasFlag(Options.ShareStrings))
            {
                _stringCache[value] = stringOffset;
            }
            return Type.String;
        }

        private Type AddFlxValueString(FlxValue flxValue)
        {
            var span = flxValue.Buffer;
            var indirectOffset = FlxValue.ComputeIndirectOffset(span, flxValue._offset, flxValue._parentWidth);

            var offsetStart = indirectOffset - flxValue._byteWidth;
            var size = (int)FlxValue.ReadULong(span, offsetStart, flxValue._byteWidth);
            var sizeWidth = (int)flxValue._byteWidth;
            while (span[indirectOffset + size] != 0)
            {
                sizeWidth <<= 1;
                size = (int)FlxValue.ReadULong(span, indirectOffset - sizeWidth, (byte)sizeWidth);
            }
            var copyLength = size + flxValue._byteWidth;

            var bitWidth = (BitWidth)BitOperations.TrailingZeroCount(flxValue._byteWidth);
            Align(bitWidth);
            var stringOffset = _offset;
            var newOffset = NewOffset((ulong)copyLength + 1);
            
            
            span.Slice(offsetStart, copyLength).CopyTo(_bytes.AsSpan().Slice((int)_offset));
            _bytes[(int)newOffset - 1] = 0;
            _offset = newOffset;
            _stack.Add(StackValue.Value(stringOffset + flxValue._byteWidth, (BitWidth)BitOperations.TrailingZeroCount(flxValue._byteWidth), Type.String));
            // No share strings when copying from a flx value
            return Type.String;
        }

        internal Type Add(FlxValue flxValue)
        {
            switch (flxValue.ValueType)
            {
                case Type.Int:
                    _stack.Add(StackValue.Value(flxValue.AsLong));
                    return Type.Int;
                case Type.Bool:
                    _stack.Add(StackValue.Value(flxValue.AsBool));
                    return Type.Bool;
                case Type.Float:
                    _stack.Add(StackValue.Value(flxValue.AsDouble));
                    return Type.Float;
                case Type.String:
                    return AddFlxValueString(flxValue); //Add(flxValue.AsString);
                case Type.Null:
                    return AddNull();
                case Type.Vector:
                    {
                        var vector = flxValue.AsVector;
                        var start = StartVector();
                        for (int i = 0; i < vector.Length; i++)
                        {
                            Add(vector[i]);
                        }
                        EndVector(start, false, false);
                        return Type.Vector;
                    }
                case Type.Map:
                    {
                        var map = flxValue.AsMap;
                        var start = StartVector();

                        for(int i = 0; i < map.Keys.Length; i++)
                        {
                            AddKey(map.Keys[i].AsString);
                            Add(map.Values[i]);
                        }
                        SortAndEndMap(start);

                        return Type.Map;
                    }
                default:
                    throw new NotImplementedException();
            }
        }

        //internal Type Add(string value)
        //{
        //    var estimatedLength = value.Length * 2;
        //    var width = BitWidthUtil.Width(estimatedLength);

        //    var spn = _bytes.AsSpan();
        //    var sizeWidth = Align(width);
        //    var sizeOffset = _offset;
        //    _offset += sizeWidth;
        //    //var sizeOffset = NewOffset();
        //    int writtenSize = Encoding.UTF8.GetBytes(value, spn.Slice((int)_offset));

        //    if (width == BitWidth.Width8)
        //    {
        //        spn[0] = (byte)writtenSize;
        //    }

        //    _offset = NewOffset((ulong)writtenSize + 1);
        //    _stack.Add(StackValue.Value(sizeOffset, width, Type.String));

        //    return Type.String;

        //    //var bytes = Encoding.UTF8.GetBytes(value);
        //    //var length = (ulong)bytes.Length;
        //    //var bitWidth = BitWidthUtil.Width(length);
        //    //if (_options.HasFlag(Options.ShareStrings) && _stringCache.ContainsKey(value))
        //    //{
        //    //    _stack.Add(StackValue.Value(_stringCache[value], bitWidth, Type.String));
        //    //    return Type.String;
        //    //}
        //    //var byteWidth = Align(bitWidth);
        //    //Write(length, byteWidth);
        //    //var stringOffset = _offset;
        //    //var newOffset = NewOffset(length + 1);
        //    //Buffer.BlockCopy(bytes, 0, _bytes, (int)_offset, (int)length);
        //    //_offset = newOffset;
        //    //_stack.Add(StackValue.Value(stringOffset, bitWidth, Type.String));
        //    //if (_options.HasFlag(Options.ShareStrings))
        //    //{
        //    //    _stringCache[value] = stringOffset;
        //    //}
        //    //return Type.String;
        //}

        internal Type Add(byte[] value)
        {
            var length = (ulong)value.Length;
            var bitWidth = BitWidthUtil.Width(length);
            var byteWidth = Align(bitWidth);
            Write(value.Length, byteWidth);
            
            var newOffset = NewOffset(length);
            var blobOffset = _offset;
            Buffer.BlockCopy(value, 0, _bytes, (int)_offset, value.Length);
            _offset = newOffset;
            _stack.Add(StackValue.Value(blobOffset, bitWidth, Type.Blob));
            return Type.Blob;
        }

        private void AddDynamicVector(IEnumerable values)
        {
            var start = StartVector();
            var typed = true;
            var prevType = -1;
            foreach (object value in values)
            {
                var currentType = AddDynamic(value);

                if (typed == false || TypesUtil.IsTypedVectorElement(currentType) == false)
                {
                    typed = false;
                    continue;
                }

                if (prevType == -1)
                {
                    prevType = (int)currentType;
                }

                if (typed)
                {
                    typed = prevType == (int)currentType;
                }
            }
            EndVector(start, typed, false);
        }
        
        private void AddDynamicMap(IDictionary values)
        {
            var start = StartVector();
            var keyStrings = new List<string>(values.Count);
            foreach (var key in values.Keys)
            {
                if (key is string s)
                {
                    keyStrings.Add(s);
                }
                else
                {
                    throw new Exception($"Key {key} is not a string.");
                }
            }

            foreach (var key in keyStrings)
            {
                AddKey(key);
                AddDynamic(values[key]);
            }

            SortAndEndMap(start);
        }

        internal void SortAndEndMap(int start)
        {
            if (((_stack.Count - start) & 1) == 1)
            {
                throw new Exception("The stack needs to hold key value pairs (even number of elements)");
            }

            var sorted = true;
            for (var i = start; i < _stack.Count - 2; i+=2)
            {
                if (ShouldFlip(_stack[i], _stack[i + 2]))
                {
                    sorted = false;
                    break;
                }
            }

            if (sorted == false)
            {
                for (var i = start; i < _stack.Count; i += 2)
                {
                    var flipIndex = i;
                    for (var j = i + 2; j < _stack.Count; j += 2)
                    {
                        if (ShouldFlip(_stack[flipIndex], _stack[j]))
                        {
                            flipIndex = j;
                        }
                    }

                    if (flipIndex != i)
                    {
                        var k = _stack[flipIndex];
                        var v = _stack[flipIndex + 1];
                        _stack[flipIndex] = _stack[i];
                        _stack[flipIndex + 1] = _stack[i + 1];
                        _stack[i] = k;
                        _stack[i + 1] = v;
                        
                    }
                }
            }

            EndMap(start);
        }

        private void EndMap(int start)
        {
            var vecLen = (_stack.Count - start) / 2;
            StackValue keys;
            if (_options.HasFlag(Options.ShareKeyVectors))
            {
                var offsets = new long[vecLen];
                for (var i = start; i < _stack.Count; i += 2)
                {
                    offsets[(i - start) / 2] = _stack[i].AsLong;
                }

                if (_keyVectorCache.ContainsKey(offsets))
                {
                    keys = _keyVectorCache[offsets];
                }
                else
                {
                    keys = CreateVector(start, vecLen, 2, true, false);
                    _keyVectorCache[offsets] = keys;
                }
            }
            else
            {
                keys = CreateVector(start, vecLen, 2, true, false);
            }

            var vec = CreateVector(start + 1, vecLen, 2, false, false, keys);
            _stack.RemoveRange(_stack.Count - vecLen * 2, vecLen * 2);
            _stack.Add(vec);
        }

        private bool ShouldFlip(StackValue v1, StackValue v2)
        {
            if (v1.TypeOfValue != Type.Key || v2.TypeOfValue != Type.Key)
            {
                throw new Exception($"Stack values are not keys {v1} | {v2}");
            }

            byte c1, c2;
            var index = 0;
            do
            {
                c1 = _bytes[v1.AsLong + index];
                c2 = _bytes[v2.AsLong + index];
                if (c2 < c1)
                {
                    return true;
                }

                if (c1 < c2)
                {
                    return false;
                }

                index++;
            } while (c1 != 0 && c2 != 0);

            return false;
        }

        private Type AddDynamic(object value)
        {
            switch (value)
            {
                case null:
                    return AddNull();
                case string s1:
                    return Add(s1);
                case bool b1:
                    return Add(b1);
                case sbyte i1:
                    return Add(i1);
                case short i1:
                    return Add(i1);
                case int i1:
                    return Add(i1);
                case long i1:
                    return Add(i1);
                case byte l1:
                    return Add(l1);
                case ushort l1:
                    return Add(l1);
                case uint l1:
                    return Add(l1);
                case ulong l1:
                    return Add(l1);
                case double d1:
                    return Add(d1);
                case IDictionary d:
                    AddDynamicMap(d);
                    return Type.Map;
                case IEnumerable l:
                    AddDynamicVector(l);
                    return Type.Vector;
                default:
                    throw new Exception($"Unexpected type of {value}");
            }
        }
        
        internal void AddKey(string value)
        {
            if (_options.HasFlag(Options.ShareKeys) && _keyCache.ContainsKey(value))
            {
                _stack.Add(StackValue.Value(_keyCache[value], BitWidth.Width8, Type.Key));
                return;
            }
            var bytes = Encoding.UTF8.GetBytes(value);
            var length = (ulong)bytes.Length;
            var keyOffset = _offset;
            var newOffset = NewOffset(length + 1);
            Buffer.BlockCopy(bytes, 0, _bytes, (int)_offset, (int)length);
            _offset = newOffset;
            _stack.Add(StackValue.Value(keyOffset, BitWidth.Width8, Type.Key));
            if (_options.HasFlag(Options.ShareKeys))
            {
                _keyCache[value] = keyOffset;
            }
        }

        private byte Align(BitWidth width)
        {
            var byteWidth = 1UL << (int) width;
            _offset += BitWidthUtil.PaddingSize(_offset, byteWidth);
            return (byte) byteWidth;
        }
        
        private void Write(StackValue value, ulong width)
        {
            var newOffset = NewOffset(width);
            if (value.IsOffset)
            {
                var relOffset = _offset - value.AsULong;
                if (width == 8 || relOffset < (ulong)1 << ((int)width * 8))
                {
                    Write(relOffset, width);
                }
                else
                {
                    throw new Exception("Unexpected size");
                }
            }
            else
            {
                var bytes = value.IsFloat32 && width == 4 ? BitConverter.GetBytes((float)value.AsDouble) : BitConverter.GetBytes(value.AsULong);
                var count = Math.Min((ulong)bytes.Length, width); 
                Buffer.BlockCopy(bytes, 0, _bytes, (int)_offset, (int)count);
            }
            _offset = newOffset;
        }
        
        private void Write(byte value)
        {
            var newOffset = NewOffset(1);
            _bytes[_offset] = value;
            _offset = newOffset;
        }
        
        private void Write(long value, ulong width)
        {
            var newOffset = NewOffset(width);
            var bytes = BitConverter.GetBytes(value);
            var count = Math.Min((ulong)bytes.Length, width); 
            Buffer.BlockCopy(bytes, 0, _bytes, (int)_offset, (int)count);
            _offset = newOffset;
        }
        
        private void Write(ulong value, ulong width)
        {
            var newOffset = NewOffset(width);
            var bytes = BitConverter.GetBytes(value);
            var count = Math.Min((ulong)bytes.Length, width); 
            Buffer.BlockCopy(bytes, 0, _bytes, (int)_offset, (int)count);
            _offset = newOffset;
        }
        
        private void Write(double value, ulong width)
        {
            var newOffset = NewOffset(width);
            var bytes = BitConverter.GetBytes(value);
            var count = Math.Min((ulong)bytes.Length, width); 
            Buffer.BlockCopy(bytes, 0, _bytes, (int)_offset, (int)count);
            _offset = newOffset;
        }

        private ulong NewOffset(ulong width)
        {
            var newOffset = _offset + width;
            var prevSize = _size;
            while (_size < newOffset)
            {
                _size <<= 1;
            }

            if (prevSize < _size)
            {
                var prevBytes = _bytes;
                _bytes = pool.Rent((int)_size); // new byte[_size];
                Array.Clear(_bytes);

                Buffer.BlockCopy(prevBytes, 0, _bytes, 0, (int)_offset);
                pool.Return(prevBytes);
            }

            return newOffset;
        }

        internal int StartVector()
        {
            return _stack.Count;
        }

        internal int EndVector(int start, bool typed, bool fix)
        {
            var vecLen = _stack.Count - start;
            var vec = CreateVector(start, vecLen, 1, typed, fix);
            
            _stack.RemoveRange(_stack.Count - vecLen, vecLen);
            _stack.Add(vec);
            return (int)vec.AsLong;
        }

        private StackValue CreateVector(int start, int vecLen, int step, bool typed, bool fix, StackValue? keys = null)
        {
            var bitWidth = BitWidthUtil.Width(vecLen);
            var prefixElems = 1;
            if (keys != null)
            {
                var elemWidth = keys.Value.ElementWidth(_offset, 0);
                if ((int) elemWidth > (int) bitWidth)
                {
                    bitWidth = elemWidth;
                }

                prefixElems += 2;
            }

            var vectorType = Type.Key;
            for (var i = start; i < _stack.Count; i+=step)
            {
                var elemWidth = _stack[i].ElementWidth(_offset, i + prefixElems);
                if ((int) elemWidth > (int) bitWidth)
                {
                    bitWidth = elemWidth;
                }

                if (typed)
                {
                    if (i == start)
                    {
                        vectorType = _stack[i].TypeOfValue;
                    }
                    else
                    {
                        if (vectorType != _stack[i].TypeOfValue)
                        {
                            throw new Exception($"Your typed vector is of type {vectorType} but the item on index {i} is of type {_stack[i].TypeOfValue}");
                        }
                    }
                }
            }

            if (TypesUtil.IsTypedVectorElement(vectorType) == false)
            {
                throw new Exception("Your fixed types are not one of: Int / UInt / Float / Key");
            }

            var byteWidth = Align(bitWidth);
            if (keys != null)
            {
                Write(keys.Value, byteWidth);
                Write(1 << (int)keys.Value.InternalWidth, byteWidth);
            }

            if (!fix)
            {
                Write(vecLen, byteWidth);
            }

            var vloc = _offset;

            for (var i = start; i < _stack.Count; i += step)
            {
                Write(_stack[i], byteWidth);
            }

            if (!typed)
            {
                for (var i = start; i < _stack.Count; i += step)
                {
                    Write(_stack[i].StoredPackedType());
                }
            }

            
            if (keys != null)
            {
                return StackValue.Value(vloc, bitWidth, Type.Map);
            }

            if (typed)
            {
                var type = TypesUtil.ToTypedVector(vectorType, (byte)(fix ? vecLen : 0));
                return StackValue.Value(vloc, bitWidth, type);
            }
            
            return StackValue.Value(vloc, bitWidth, Type.Vector);
        }
    }

    internal class OffsetArrayComparer : IEqualityComparer<long[]>
    {
        public bool Equals(long[] x, long[] y)
        {
            if (x.Length != y.Length)
            {
                return false;
            }
            for (var i = 0; i < x.Length; i++)
            {
                if (x[i] != y[i])
                {
                    return false;
                }
            }
            return true;
        }

        public int GetHashCode(long[] obj)
        {
            var result = 17;
            for (var i = 0; i < obj.Length; i++)
            {
                unchecked
                {
                    result = (int) (result * 23 + obj[i]);
                }
            }
            return result;
        }
    }
}