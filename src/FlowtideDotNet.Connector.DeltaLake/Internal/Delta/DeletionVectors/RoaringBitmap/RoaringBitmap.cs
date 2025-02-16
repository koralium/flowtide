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
using System.Collections;
using System.Collections.Generic;
using System.IO;
using System.Linq;

namespace FlowtideDotNet.Connector.DeltaLake.Internal.Delta.DeletionVectors.RoaringBitmap
{
    /// <summary>
    /// Code from https://github.com/Tornhoof/RoaringBitmap/tree/master which is archived
    /// </summary>
    public class RoaringBitmap : IEnumerable<int>, IEquatable<RoaringBitmap>
    {
        private readonly RoaringArray m_HighLowContainer;

        private RoaringBitmap(RoaringArray input)
        {
            m_HighLowContainer = input;
        }

        public long Cardinality => m_HighLowContainer.Cardinality;

        public IEnumerator<int> GetEnumerator()
        {
            return m_HighLowContainer.GetEnumerator();
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return GetEnumerator();
        }

        public bool Equals(RoaringBitmap? other)
        {
            if (ReferenceEquals(this, other))
            {
                return true;
            }
            if (ReferenceEquals(null, other))
            {
                return false;
            }
            return m_HighLowContainer.Equals(other.m_HighLowContainer);
        }

        /// <summary>
        ///     Creates a new immutable RoaringBitmap from an existing list of integers
        /// </summary>
        /// <param name="values">List of integers</param>
        /// <returns>RoaringBitmap</returns>
        public static RoaringBitmap Create(params int[] values)
        {
            return Create(values.AsEnumerable());
        }

        /// <summary>
        ///     Optimizes a RoaringBitmap to prepare e.g. for Serialization/Deserialization
        /// </summary>
        /// <returns>RoaringBitmap</returns>
        public RoaringBitmap Optimize()
        {
            return new RoaringBitmap(RoaringArray.Optimize(m_HighLowContainer));
        }

        /// <summary>
        ///     Creates a new immutable RoaringBitmap from an existing list of integers
        /// </summary>
        /// <param name="values">List of integers</param>
        /// <returns>RoaringBitmap</returns>
        public static RoaringBitmap Create(IEnumerable<int> values)
        {
            var groupbyHb = values.Distinct().OrderBy(t => t).GroupBy(Util.HighBits).OrderBy(t => t.Key).ToList();
            var keys = new List<ushort>();
            var containers = new List<Container>();
            var size = 0;
            foreach (var group in groupbyHb)
            {
                keys.Add(group.Key);
                if (group.Count() > Container.MaxSize)
                {
                    containers.Add(BitmapContainer.Create(group.Select(Util.LowBits).ToArray()));
                }
                else
                {
                    containers.Add(ArrayContainer.Create(group.Select(Util.LowBits).ToArray()));
                }
                size++;
            }
            return new RoaringBitmap(new RoaringArray(size, keys, containers));
        }

        /// <summary>
        ///     Bitwise Or operation of two RoaringBitmaps
        /// </summary>
        /// <param name="x">RoaringBitmap</param>
        /// <param name="y">RoaringBitmap</param>
        /// <returns>RoaringBitmap</returns>
        public static RoaringBitmap operator |(RoaringBitmap x, RoaringBitmap y)
        {
            return new RoaringBitmap(x.m_HighLowContainer | y.m_HighLowContainer);
        }

        /// <summary>
        ///     Bitwise And operation of two RoaringBitmaps
        /// </summary>
        /// <param name="x">RoaringBitmap</param>
        /// <param name="y">RoaringBitmap</param>
        /// <returns>RoaringBitmap</returns>
        public static RoaringBitmap operator &(RoaringBitmap x, RoaringBitmap y)
        {
            return new RoaringBitmap(x.m_HighLowContainer & y.m_HighLowContainer);
        }

        /// <summary>
        ///     Bitwise Not operation of a RoaringBitmap
        /// </summary>
        /// <param name="x">RoaringBitmap</param>
        /// <returns>RoaringBitmap</returns>
        public static RoaringBitmap operator ~(RoaringBitmap x)
        {
            return new RoaringBitmap(~x.m_HighLowContainer);
        }

        /// <summary>
        ///     Bitwise Xor operation of two RoaringBitmaps
        /// </summary>
        /// <param name="x">RoaringBitmap</param>
        /// <param name="y">RoaringBitmap</param>
        /// <returns>RoaringBitmap</returns>
        public static RoaringBitmap operator ^(RoaringBitmap x, RoaringBitmap y)
        {
            return new RoaringBitmap(x.m_HighLowContainer ^ y.m_HighLowContainer);
        }

        /// <summary>
        ///     Bitwise AndNot operation of two RoaringBitmaps
        /// </summary>
        /// <param name="x">RoaringBitmap</param>
        /// <param name="y">RoaringBitmap</param>
        /// <returns>RoaringBitmap</returns>
        public static RoaringBitmap AndNot(RoaringBitmap x, RoaringBitmap y)
        {
            return new RoaringBitmap(RoaringArray.AndNot(x.m_HighLowContainer, y.m_HighLowContainer));
        }

        public override bool Equals(object? obj)
        {
            var ra = obj as RoaringArray;
            return ra != null && Equals(ra);
        }

        public override int GetHashCode()
        {
            return (13 ^ m_HighLowContainer.GetHashCode()) << 3;
        }

        /// <summary>
        ///     Serializes a RoaringBitmap into a stream using the 'official' RoaringBitmap file format
        /// </summary>
        /// <param name="roaringBitmap">RoaringBitmap</param>
        /// <param name="stream">Stream</param>
        public static void Serialize(RoaringBitmap roaringBitmap, Stream stream)
        {
            RoaringArray.Serialize(roaringBitmap.m_HighLowContainer, stream);
        }

        /// <summary>
        ///     Deserializes a RoaringBitmap from astream using the 'official' RoaringBitmap file format
        /// </summary>
        /// <param name="stream">Stream</param>
        public static RoaringBitmap Deserialize(Stream stream)
        {
            var ra = RoaringArray.Deserialize(stream);
            return new RoaringBitmap(ra);
        }
    }
}