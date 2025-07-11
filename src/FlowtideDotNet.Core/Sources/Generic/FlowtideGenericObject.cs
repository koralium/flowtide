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

namespace FlowtideDotNet.Core.Sources.Generic
{
    public class FlowtideGenericObject<T>
    {
        public FlowtideGenericObject(string key, T? value, long watermark, bool isDelete)
        {
            Key = key;
            Value = value;
            Watermark = watermark;
            this.isDelete = isDelete;
        }

        /// <summary>
        /// Unique identifier of the object 
        /// </summary>
        public string Key { get; }

        /// <summary>
        /// Value object, contains the data that will be used by the stream, can be null if its a delete
        /// </summary>
        public T? Value { get; }

        /// <summary>
        /// Watermark of the object, used to determine the order of the objects in the stream
        /// </summary>
        public long Watermark { get; }

        /// <summary>
        /// Indicates if the object is deleted or not
        /// </summary>
        public bool isDelete { get; }
    }
}
