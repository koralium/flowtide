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

namespace FlexBuffers
{
    public class FlexBufferBuilder
    {
        public static byte[] Map(Action<IFlexBufferMapBuilder> map)
        {
            var buffer = new FlexBuffer(ArrayPool<byte>.Shared);
            buffer.NewObject();
            var start = buffer.StartVector();
            var builder = new FlexBufferMapBuilder(buffer);
            map(builder);
            buffer.SortAndEndMap(start);
            return buffer.Finish();
        }
        
        public static byte[] Vector(Action<IFlexBufferVectorBuilder> vector)
        {
            var buffer = new FlexBuffer(ArrayPool<byte>.Shared);
            buffer.NewObject();
            var start = buffer.StartVector();
            var builder = new FlexBufferVectorBuilder(buffer);
            vector(builder);
            buffer.EndVector(start, false, false);
            return buffer.Finish();
        }
    }

    public interface IFlexBufferMapBuilder
    {
        void AddNull(string key);
        void Add(string key, long value, bool indirect = false);
        void Add(string key, long x, long y);
        void Add(string key, long x, long y, long z);
        void Add(string key, long x, long y, long z, long w);
        void Add(string key, ulong value, bool indirect = false);
        void Add(string key, ulong x, ulong y);
        void Add(string key, ulong x, ulong y, ulong z);
        void Add(string key, ulong x, ulong y, ulong z, ulong w);
        void Add(string key, double value, bool indirect = false);
        void Add(string key, double x, double y);
        void Add(string key, double x, double y, double z);
        void Add(string key, double x, double y, double z, double w);
        void Add(string key, bool value);
        void Add(string key, string value);
        void Add(string key, byte[] value);
        void Add(string key, FlxValue value);
        void Map(string key, Action<IFlexBufferMapBuilder> map);
        void Vector(string key, Action<IFlexBufferVectorBuilder> vector);
    }
    
    public interface IFlexBufferVectorBuilder
    {
        void AddNull();
        void Add(long value, bool indirect = false);
        void Add(long x, long y);
        void Add(long x, long y, long z);
        void Add(long x, long y, long z, long w);
        void Add(ulong value, bool indirect = false);
        void Add(ulong x, ulong y);
        void Add(ulong x, ulong y, ulong z);
        void Add(ulong x, ulong y, ulong z, ulong w);
        void Add(double value, bool indirect = false);
        void Add(double x, double y);
        void Add(double x, double y, double z);
        void Add(double x, double y, double z, double w);
        void Add(bool value);
        void Add(string value);
        void Add(byte[] value);
        void Map(Action<IFlexBufferMapBuilder> map);
        void Vector(Action<IFlexBufferVectorBuilder> vector);

        void Add(FlxValue flxValue);
        void Add(decimal value);
    }

    internal struct FlexBufferMapBuilder : IFlexBufferMapBuilder
    {
        private readonly FlexBuffer _buffer;

        internal FlexBufferMapBuilder(FlexBuffer buffer)
        {
            _buffer = buffer;
        }

        public void AddNull(string key)
        {
            _buffer.AddKey(key);
            _buffer.AddNull();
        }
        
        public void Add(string key, long value, bool indirect = false)
        {
            _buffer.AddKey(key);
            if (indirect)
            {
                _buffer.AddIndirect(value);
            }
            else
            {
                _buffer.Add(value);
            }
        }

        public void Add(string key, long x, long y)
        {
            _buffer.AddKey(key);
            var start = _buffer.StartVector();
            _buffer.Add(x);
            _buffer.Add(y);
            _buffer.EndVector(start, true, true);
        }

        public void Add(string key, long x, long y, long z)
        {
            _buffer.AddKey(key);
            var start = _buffer.StartVector();
            _buffer.Add(x);
            _buffer.Add(y);
            _buffer.Add(z);
            _buffer.EndVector(start, true, true);
        }

        public void Add(string key, long x, long y, long z, long w)
        {
            _buffer.AddKey(key);
            var start = _buffer.StartVector();
            _buffer.Add(x);
            _buffer.Add(y);
            _buffer.Add(z);
            _buffer.Add(w);
            _buffer.EndVector(start, true, true);
        }

        public void Add(string key, ulong value, bool indirect = false)
        {
            _buffer.AddKey(key);
            if (indirect)
            {
                _buffer.AddIndirect(value);
            }
            else
            {
                _buffer.Add(value);
            }
        }

        public void Add(string key, ulong x, ulong y)
        {
            _buffer.AddKey(key);
            var start = _buffer.StartVector();
            _buffer.Add(x);
            _buffer.Add(y);
            _buffer.EndVector(start, true, true);
        }

        public void Add(string key, ulong x, ulong y, ulong z)
        {
            _buffer.AddKey(key);
            var start = _buffer.StartVector();
            _buffer.Add(x);
            _buffer.Add(y);
            _buffer.Add(z);
            _buffer.EndVector(start, true, true);
        }

        public void Add(string key, ulong x, ulong y, ulong z, ulong w)
        {
            _buffer.AddKey(key);
            var start = _buffer.StartVector();
            _buffer.Add(x);
            _buffer.Add(y);
            _buffer.Add(z);
            _buffer.Add(w);
            _buffer.EndVector(start, true, true);
        }

        public void Add(string key, double value, bool indirect = false)
        {
            _buffer.AddKey(key);
            if (indirect)
            {
                _buffer.AddIndirect(value);
            }
            else
            {
                _buffer.Add(value);
            }
        }

        public void Add(string key, double x, double y)
        {
            _buffer.AddKey(key);
            var start = _buffer.StartVector();
            _buffer.Add(x);
            _buffer.Add(y);
            _buffer.EndVector(start, true, true);
        }

        public void Add(string key, double x, double y, double z)
        {
            _buffer.AddKey(key);
            var start = _buffer.StartVector();
            _buffer.Add(x);
            _buffer.Add(y);
            _buffer.Add(z);
            _buffer.EndVector(start, true, true);
        }

        public void Add(string key, double x, double y, double z, double w)
        {
            _buffer.AddKey(key);
            var start = _buffer.StartVector();
            _buffer.Add(x);
            _buffer.Add(y);
            _buffer.Add(z);
            _buffer.Add(w);
            _buffer.EndVector(start, true, true);
        }

        public void Add(string key, bool value)
        {
            _buffer.AddKey(key);
            _buffer.Add(value);
        }

        public void Add(string key, string value)
        {
            _buffer.AddKey(key);
            _buffer.Add(value);
        }

        public void Add(string key, byte[] value)
        {
            _buffer.AddKey(key);
            _buffer.Add(value);
        }

        public void Map(string key, Action<IFlexBufferMapBuilder> map)
        {
            _buffer.AddKey(key);
            var start = _buffer.StartVector();
            var builder = new FlexBufferMapBuilder(_buffer);
            map(builder);
            _buffer.SortAndEndMap(start);
        }

        public void Vector(string key, Action<IFlexBufferVectorBuilder> vector)
        {
            _buffer.AddKey(key);
            var start = _buffer.StartVector();
            var builder = new FlexBufferVectorBuilder(_buffer);
            vector(builder);
            _buffer.EndVector(start, false, false);
        }

        public void Add(string key, FlxValue value)
        {
            _buffer.AddKey(key);
            _buffer.Add(value);
        }
    }

    internal struct FlexBufferVectorBuilder : IFlexBufferVectorBuilder
    {
        private readonly FlexBuffer _buffer;

        internal FlexBufferVectorBuilder(FlexBuffer buffer)
        {
            _buffer = buffer;
        }

        public void AddNull()
        {
            _buffer.AddNull();
        }
        public void Add(long value, bool indirect = false)
        {
            if (indirect)
            {
                _buffer.AddIndirect(value);
            }
            else
            {
                _buffer.Add(value);
            }
        }

        public void Add(long x, long y)
        {
            var start = _buffer.StartVector();
            _buffer.Add(x);
            _buffer.Add(y);
            _buffer.EndVector(start, true, true);
        }

        public void Add(long x, long y, long z)
        {
            var start = _buffer.StartVector();
            _buffer.Add(x);
            _buffer.Add(y);
            _buffer.Add(z);
            _buffer.EndVector(start, true, true);
        }

        public void Add(long x, long y, long z, long w)
        {
            var start = _buffer.StartVector();
            _buffer.Add(x);
            _buffer.Add(y);
            _buffer.Add(z);
            _buffer.Add(w);
            _buffer.EndVector(start, true, true);
        }

        public void Add(ulong value, bool indirect = false)
        {
            if (indirect)
            {
                _buffer.AddIndirect(value);
            }
            else
            {
                _buffer.Add(value);
            }
        }

        public void Add(ulong x, ulong y)
        {
            var start = _buffer.StartVector();
            _buffer.Add(x);
            _buffer.Add(y);
            _buffer.EndVector(start, true, true);
        }

        public void Add(ulong x, ulong y, ulong z)
        {
            var start = _buffer.StartVector();
            _buffer.Add(x);
            _buffer.Add(y);
            _buffer.Add(z);
            _buffer.EndVector(start, true, true);
        }

        public void Add(ulong x, ulong y, ulong z, ulong w)
        {
            var start = _buffer.StartVector();
            _buffer.Add(x);
            _buffer.Add(y);
            _buffer.Add(z);
            _buffer.Add(w);
            _buffer.EndVector(start, true, true);
        }

        public void Add(double value, bool indirect = false)
        {
            if (indirect)
            {
                _buffer.AddIndirect(value);
            }
            else
            {
                _buffer.Add(value);
            }
        }

        public void Add(double x, double y)
        {
            var start = _buffer.StartVector();
            _buffer.Add(x);
            _buffer.Add(y);
            _buffer.EndVector(start, true, true);
        }

        public void Add(double x, double y, double z)
        {
            var start = _buffer.StartVector();
            _buffer.Add(x);
            _buffer.Add(y);
            _buffer.Add(z);
            _buffer.EndVector(start, true, true);
        }

        public void Add(double x, double y, double z, double w)
        {
            var start = _buffer.StartVector();
            _buffer.Add(x);
            _buffer.Add(y);
            _buffer.Add(z);
            _buffer.Add(w);
            _buffer.EndVector(start, true, true);
        }

        public void Add(bool value)
        {
            _buffer.Add(value);
        }

        public void Add(string value)
        {
            _buffer.Add(value);
        }

        public void Add(byte[] value)
        {
            _buffer.Add(value);
        }

        public void Map(Action<IFlexBufferMapBuilder> map)
        {
            var start = _buffer.StartVector();
            var builder = new FlexBufferMapBuilder(_buffer);
            map(builder);
            _buffer.SortAndEndMap(start);
        }

        public void Vector(Action<IFlexBufferVectorBuilder> vector)
        {
            var start = _buffer.StartVector();
            var builder = new FlexBufferVectorBuilder(_buffer);
            vector(builder);
            _buffer.EndVector(start, false, false);
        }

        public void Add(FlxValue flxValue)
        {
            _buffer.Add(flxValue);
        }

        public void Add(decimal value)
        {
            _buffer.Add(value);
        }
    }
}