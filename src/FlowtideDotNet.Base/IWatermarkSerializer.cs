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

using System;
using System.Buffers;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FlowtideDotNet.Base
{
    /// <summary>
    /// Provides a mechanism to serialize and deserialize watermark values.
    /// This is used when transferring watermarks across network boundaries (such as between partitions) 
    /// or when persisting them.
    /// </summary>
    public interface IWatermarkSerializer
    {
        /// <summary>
        /// Serializes an <see cref="AbstractWatermarkValue"/> into the provided buffer writer.
        /// </summary>
        /// <param name="value">The watermark value to serialize.</param>
        /// <param name="writer">The buffer writer to write the serialized bytes to.</param>
        void Serialize(AbstractWatermarkValue value, IBufferWriter<byte> writer);

        /// <summary>
        /// Deserializes a sequence of bytes into an <see cref="AbstractWatermarkValue"/>.
        /// </summary>
        /// <param name="reader">The sequence reader containing the serialized watermark data.</param>
        /// <returns>The deserialized <see cref="AbstractWatermarkValue"/>.</returns>
        AbstractWatermarkValue Deserialize(ref SequenceReader<byte> reader);
    }
}
