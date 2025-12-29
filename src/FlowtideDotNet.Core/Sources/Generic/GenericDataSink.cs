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

using FlowtideDotNet.Base;
using FlowtideDotNet.Base.Utils;
using FlowtideDotNet.Core.ColumnStore.ObjectConverter.Resolvers;
using FlowtideDotNet.Substrait.Relations;

namespace FlowtideDotNet.Core.Sources.Generic
{
    public abstract class GenericDataSink<T>
    {
        /// <summary>
        /// Return the names of the primary keys
        /// </summary>
        /// <returns></returns>
        public abstract Task<List<string>> GetPrimaryKeyNames();

        public virtual bool FetchExistingData { get; } = true;

        public virtual IEnumerable<IObjectColumnResolver> GetCustomConverters()
        {
            yield break;
        }

        public abstract Task OnChanges(IAsyncEnumerable<FlowtideGenericWriteObject<T>> changes, Watermark watermark, bool isInitialData, CancellationToken cancellationToken);

        public virtual Task Initialize(WriteRelation writeRelation)
        {
            return Task.CompletedTask;
        }

        public virtual IAsyncEnumerable<T> GetExistingData()
        {
            return EmptyAsyncEnumerable<T>.Instance;
        }
    }
}
