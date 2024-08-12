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

using FlexBuffers;

namespace FlowtideDotNet.Core.Flexbuffer
{
    public static class FlexbufferMerger
    {
        public static void Emit(FlxVector vec1, List<int> emit)
        {
            
            var span = vec1.Span;
            for (int i = 0; i < vec1.Length; i++)
            {
                var packedType = span[vec1._offset + vec1._length * vec1._byteWidth + i];
                var _type = (FlexBuffers.Type)(packedType >> 2);
                var elemOffset = vec1._offset + (i * vec1._byteWidth);
                switch (_type)
                {
                    case FlexBuffers.Type.String:
                        // Fetch size
                        int indirectOffset = FlxValue.ComputeIndirectOffset(span, elemOffset, vec1._byteWidth);
                        var size = (int)FlxValue.ReadULong(span, indirectOffset - vec1._byteWidth, vec1._byteWidth);
                        var sizeWidth = (int)vec1._byteWidth;
                        while (span[indirectOffset + size] != 0)
                        {
                            sizeWidth <<= 1;
                            size = (int)FlxValue.ReadULong(span, indirectOffset - sizeWidth, (byte)sizeWidth);
                        }
                        
                        break;
                    case FlexBuffers.Type.Vector:
                        

                        break;
                }
            }
        }

        public static void Merge(FlxVector vec1, FlxVector vec2)
        {
            var newLength = vec1._buffer.Length + vec2._buffer.Length;
            var buffer = new byte[newLength];
            var vec1childrenstart = vec1._offset; // + (vec1._length * vec1._byteWidth) + vec1._length;

            var vv = vec1[0];
            var val = vv.AsLong;
            var vv2 = vec1[1];
            var vals = vv2.AsString;
            vec1._buffer.AsSpan().Slice(0, vec1childrenstart).CopyTo(buffer);

            var vec2childrenstart = vec2._offset + vec2._byteWidth;
            vec2._buffer.AsSpan().Slice(0, vec2childrenstart).CopyTo(buffer.AsSpan().Slice(vec1childrenstart));

            //var vec1Table = vec1._buffer.Slice(vec1childrenstart, (vec1._offset + vec1._length * vec1._byteWidth + vec1.Length) - vec1childrenstart);

            for (int i = 0; i < vec1.Length; i++)
            {
                var type = vec1._buffer[vec1._offset + vec1._length * vec1._byteWidth + i];
                var t = (FlexBuffers.Type)(type >> 2);
                var elemOffset = vec1._offset + (i * vec1._byteWidth);
                switch (t)
                {
                    case FlexBuffers.Type.String:
                    case FlexBuffers.Type.Vector:
                        int indirectOffset = FlxValue.ComputeIndirectOffset(vec1._buffer, elemOffset, vec1._byteWidth);
                        break;
                }
            }

            for (int i = 0; i < vec2.Length; i++)
            {
                var type = vec2._buffer[vec2._offset + vec2._length * vec2._byteWidth + i];
                var t = (FlexBuffers.Type)(type >> 2);
                var elemOffset = vec2._offset + (i * vec2._byteWidth);
                switch (t)
                {
                    case FlexBuffers.Type.String:
                    case FlexBuffers.Type.Vector:
                        int indirectOffset = FlxValue.ComputeIndirectOffset(vec2._buffer, elemOffset, vec2._byteWidth);
                        var newOffset = indirectOffset + vec1childrenstart;
                        break;
                }
            }
        }
    }
}
