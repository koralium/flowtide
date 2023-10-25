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

using FlowtideDotNet.AcceptanceTests.Entities;
using FlowtideDotNet.Substrait.Sql;
using System;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FlowtideDotNet.AcceptanceTests.Internal
{
    internal class DatasetTableProvider : ITableProvider
    {
        private readonly MockDatabase mockDatabase;

        public DatasetTableProvider(MockDatabase mockDatabase)
        {
            this.mockDatabase = mockDatabase;
        }

        public bool TryGetTableInformation(string tableName, [NotNullWhen(true)] out TableMetadata? tableMetadata)
        {
            if (mockDatabase.Tables.TryGetValue(tableName, out var mockTable))
            {
                tableMetadata = new TableMetadata(tableName, mockTable.Columns);
                return true;
            }
            tableMetadata = null;
            return false;
        }
    }
}
