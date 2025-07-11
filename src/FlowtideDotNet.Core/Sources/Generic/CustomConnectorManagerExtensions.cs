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

using FlowtideDotNet.Core.Operators.Write;
using FlowtideDotNet.Core.Sources.Generic.Internal;
using FlowtideDotNet.Substrait.Relations;

namespace FlowtideDotNet.Core.Sources.Generic
{
    public static class CustomConnectorManagerExtensions
    {
        public static IConnectorManager AddCustomSource<T>(this IConnectorManager manager, string tableName, Func<ReadRelation, GenericDataSourceAsync<T>> createFunc)
            where T : class
        {
            manager.AddSource(new CustomSourceFactory<T>(tableName, createFunc));
            return manager;
        }

        public static IConnectorManager AddCustomSink<T>(
            this IConnectorManager manager,
            string tableName,
            Func<WriteRelation, GenericDataSink<T>> createFunc,
            ExecutionMode executionMode = ExecutionMode.Hybrid)
            where T : class
        {
            manager.AddSink(new CustomSinkFactory<T>(tableName, createFunc, executionMode));
            return manager;
        }
    }
}
