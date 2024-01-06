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
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FlowtideDotNet.Core.Sources.Generic
{
    public abstract class GenericDataSourceAsync<T> where T : class
    {
        /// <summary>
        /// Fetches all objects from the data source
        /// </summary>
        /// <returns></returns>
        public abstract IAsyncEnumerable<FlowtideGenericObject<T>> FullLoadAsync();

        /// <summary>
        /// Fetches all objects from the data source that have changed since the last delta load
        /// </summary>
        /// <returns></returns>
        public abstract IAsyncEnumerable<FlowtideGenericObject<T>> DeltaLoadAsync(long lastWatermark);

        /// <summary>
        /// Interval between delta loads, set to null to disable delta loads
        /// </summary>
        public abstract TimeSpan? DeltaLoadInterval { get; }

        /// <summary>
        /// Interval between full loads, set to null to disable, defaults to null
        /// </summary>
        public virtual TimeSpan? FullLoadInterval => default;
    }
    public abstract class GenericDataSource<T> : GenericDataSourceAsync<T> where T : class
    {
        public override IAsyncEnumerable<FlowtideGenericObject<T>> FullLoadAsync()
        {
            return FullLoad().ToAsyncEnumerable();
        }

        public override IAsyncEnumerable<FlowtideGenericObject<T>> DeltaLoadAsync(long lastWatermark)
        {
            return DeltaLoad(lastWatermark).ToAsyncEnumerable();
        }

        protected abstract IEnumerable<FlowtideGenericObject<T>> FullLoad();

        protected abstract IEnumerable<FlowtideGenericObject<T>> DeltaLoad(long lastWatermark);
    }
}
