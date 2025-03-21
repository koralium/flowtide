﻿// Licensed under the Apache License, Version 2.0 (the "License")
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

using FlowtideDotNet.Base.Vertices.Ingress;
using FlowtideDotNet.Core.Compute;
using FlowtideDotNet.Core.Connectors;
using FlowtideDotNet.Substrait.Relations;
using FlowtideDotNet.Substrait.Sql;
using FlowtideDotNet.Substrait.Type;
using System.Threading.Tasks.Dataflow;

namespace FlowtideDotNet.Core.Sources.Generic.Internal
{
    public class CustomSourceFactory<T> : AbstractConnectorSourceFactory, IConnectorTableProviderFactory
        where T : class
    {
        private readonly string tableName;
        private readonly Func<ReadRelation, GenericDataSourceAsync<T>> dataSourceFunc;

        public CustomSourceFactory(string tableName, Func<ReadRelation, GenericDataSourceAsync<T>> dataSourceFunc)
        {
            this.tableName = tableName;
            this.dataSourceFunc = dataSourceFunc;
        }

        public override bool CanHandle(ReadRelation readRelation)
        {
            return readRelation.NamedTable.DotSeperated.Equals(tableName, StringComparison.OrdinalIgnoreCase);
        }

        public ITableProvider Create()
        {
            return new GenericTableProvider<T>(tableName);
        }

        public override Relation ModifyPlan(ReadRelation readRelation)
        {
            var keyIndex = readRelation.BaseSchema.Names.IndexOf("__key");
            if (keyIndex < 0)
            {
                readRelation.BaseSchema.Names.Add("__key");
                if (readRelation.BaseSchema.Struct != null)
                {
                    readRelation.BaseSchema.Struct.Types.Add(new StringType());
                }
            }
            return base.ModifyPlan(readRelation);
        }

        public override IStreamIngressVertex CreateSource(ReadRelation readRelation, IFunctionsRegister functionsRegister, DataflowBlockOptions dataflowBlockOptions)
        {
            return new GenericReadOperator<T>(dataSourceFunc(readRelation), readRelation, functionsRegister, dataflowBlockOptions);
        }
    }
}
