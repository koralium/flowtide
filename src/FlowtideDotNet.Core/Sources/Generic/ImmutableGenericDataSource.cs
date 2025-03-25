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

using FlowtideDotNet.Storage.StateManager;
using FlowtideDotNet.Substrait.Relations;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FlowtideDotNet.Core.Sources.Generic
{
    public struct ImmutableRow<T>
    {
        public T Value { get; }

        public long Watermark { get; }

        public ImmutableRow(T value, long watermark)
        {
            Value = value;
            Watermark = watermark;
        }
    }

    /// <summary>
    /// An immutable data source, only inserts are allowed
    /// </summary>
    /// <typeparam name="T"></typeparam>
    public abstract class ImmutableGenericDataSourceAsync<T>
        where T : class
    {
        public virtual Task Initialize(ReadRelation readRelation, IStateManagerClient stateManagerClient)
        {
            return Task.CompletedTask;
        }

        public virtual Task Checkpoint()
        {
            return Task.CompletedTask;
        }

        /// <summary>
        /// Fetches all objects from the data source
        /// </summary>
        /// <returns></returns>
        public abstract IAsyncEnumerable<ImmutableRow<T>> FullLoadAsync();

        /// <summary>
        /// Fetches all objects from the data source that are new since the last delta load
        /// </summary>
        /// <returns></returns>
        public abstract IAsyncEnumerable<ImmutableRow<T>> DeltaLoadAsync(long lastWatermark);

        /// <summary> 
        /// Interval between delta loads, set to null to disable delta loads
        /// </summary>
        public abstract TimeSpan? DeltaLoadInterval { get; }
    }

    public abstract class ImmutableGenericDataSource<T> : ImmutableGenericDataSourceAsync<T>
        where T : class
    {
        public override IAsyncEnumerable<ImmutableRow<T>> FullLoadAsync()
        {
            return FullLoad().ToAsyncEnumerable();
        }

        public override IAsyncEnumerable<ImmutableRow<T>> DeltaLoadAsync(long lastWatermark)
        {
            return DeltaLoad(lastWatermark).ToAsyncEnumerable();
        }

        protected abstract IEnumerable<ImmutableRow<T>> FullLoad();

        protected abstract IEnumerable<ImmutableRow<T>> DeltaLoad(long lastWatermark);
    }
}
