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
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;

namespace FlowtideDotNet.TestFramework.Internal
{
    internal class TestDataSourceFactory : IConnectorSourceFactory, IConnectorTableProviderFactory
    {
        private readonly string _tableName;
        private readonly TestDataTable _table;

        public TestDataSourceFactory(string tableName, TestDataTable table)
        {
            this._tableName = tableName;
            this._table = table;
        }
        public bool CanHandle(ReadRelation readRelation)
        {
            if (readRelation.NamedTable.DotSeperated == _tableName)
            {
                return true;
            }
            return false;
        }

        public ITableProvider Create()
        {
            return new TestDataTableProvider(_tableName, _table);
        }

        public IStreamIngressVertex CreateSource(ReadRelation readRelation, IFunctionsRegister functionsRegister, DataflowBlockOptions dataflowBlockOptions)
        {
            return new TestDataSource(_table, readRelation, dataflowBlockOptions);
        }

        public Relation ModifyPlan(ReadRelation readRelation)
        {
            if (readRelation.Filter != null)
            {
                return new FilterRelation()
                {
                    Emit = readRelation.Emit,
                    Condition = readRelation.Filter,
                    Input = readRelation,
                };
            }

            return readRelation;
        }
    }
}
