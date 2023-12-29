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

using System.Diagnostics;

namespace FlowtideDotNet.Base.Metrics
{
    /// <summary>
    /// Copy of System.Diagnostics.Metrics.UpDownCounter as an interface
    /// </summary>
    /// <typeparam name="T"></typeparam>
    public interface IUpDownCounter<T> where T : struct
    {

        /// <summary>
        /// Record the delta value of the measurement. The delta can be positive, negative or zero.
        /// </summary>
        /// <param name="delta">The amount to be added which can be positive, negative or zero.</param>
        public void Add(T delta);

        /// <summary>
        /// Record the delta value of the measurement. The delta can be positive, negative or zero.
        /// </summary>
        /// <param name="delta">The amount to be added which can be positive, negative or zero.</param>
        /// <param name="tag">A key-value pair tag associated with the measurement.</param>
        public void Add(T delta, KeyValuePair<string, object?> tag);

        /// <summary>
        /// Record the delta value of the measurement. The delta can be positive, negative or zero.
        /// </summary>
        /// <param name="delta">The amount to be added which can be positive, negative or zero.</param>
        /// <param name="tag1">A first key-value pair tag associated with the measurement.</param>
        /// <param name="tag2">A second key-value pair tag associated with the measurement.</param>
        public void Add(T delta, KeyValuePair<string, object?> tag1, KeyValuePair<string, object?> tag2);

        /// <summary>
        /// Record the delta value of the measurement. The delta can be positive, negative or zero.
        /// </summary>
        /// <param name="delta">The amount to be added which can be positive, negative or zero.</param>
        /// <param name="tag1">A first key-value pair tag associated with the measurement.</param>
        /// <param name="tag2">A second key-value pair tag associated with the measurement.</param>
        /// <param name="tag3">A third key-value pair tag associated with the measurement.</param>
        public void Add(T delta, KeyValuePair<string, object?> tag1, KeyValuePair<string, object?> tag2, KeyValuePair<string, object?> tag3);

        /// <summary>
        /// Record the delta value of the measurement. The delta can be positive, negative or zero.
        /// </summary>
        /// <param name="delta">The amount to be added which can be positive, negative or zero.</param>
        /// <param name="tags">A span of key-value pair tags associated with the measurement.</param>
        public void Add(T delta, ReadOnlySpan<KeyValuePair<string, object?>> tags);

        /// <summary>
        /// Record the delta value of the measurement. The delta can be positive, negative or zero.
        /// </summary>
        /// <param name="delta">The amount to be added which can be positive, negative or zero.</param>
        /// <param name="tags">A list of key-value pair tags associated with the measurement.</param>
        public void Add(T delta, params KeyValuePair<string, object?>[] tags);

        /// <summary>
        /// Record the delta value of the measurement. The delta can be positive, negative or zero.
        /// </summary>
        /// <param name="delta">The amount to be added which can be positive, negative or zero.</param>
        /// <param name="tagList">A <see cref="T:System.Diagnostics.TagList" /> of tags associated with the measurement.</param>
        public void Add(T delta, in TagList tagList);
    }
}
