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

using FlowtideDotNet.Core.ColumnStore.ObjectConverter.Resolvers;
using FlowtideDotNet.Storage.StateManager;
using FlowtideDotNet.Substrait.Relations;

namespace FlowtideDotNet.Core.Sources.Generic
{
    public abstract class GenericDataSourceAsync<T> where T : class
    {
        private Func<string, ValueTask<T?>>? _lookupRowFunc;

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

        public virtual IEnumerable<IObjectColumnResolver> GetCustomConverters()
        {
            yield break;
        }

        internal void SetLookupRowFunc(Func<string, ValueTask<T?>> lookupRowFunc)
        {
            _lookupRowFunc = lookupRowFunc;
        }

        public virtual Task Initialize(ReadRelation readRelation, IStateManagerClient stateManagerClient)
        {
            return Task.CompletedTask;
        }

        public virtual Task Checkpoint()
        {
            return Task.CompletedTask;
        }

        /// <summary>
        /// Looks up a rows value in the state by key.
        /// This only contains values for the properties that are used in the stream.
        /// </summary>
        /// <param name="key"></param>
        /// <returns></returns>
        /// <exception cref="InvalidOperationException">Thrown if the lookup function has not yet been initialized</exception>
        protected ValueTask<T?> LookupRow(string key)
        {
            if (_lookupRowFunc == null)
            {
                throw new InvalidOperationException("LookupRow is not supported, or has not yet been initialized");
            }
            return _lookupRowFunc(key);
        }

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
