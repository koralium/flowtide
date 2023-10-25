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

using FastMember;
using Microsoft.EntityFrameworkCore.Metadata.Internal;
using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using static SqlParser.Ast.ConflictTarget;

namespace FlowtideDotNet.AcceptanceTests.Internal
{
    public class MockDatabase
    {
        public MockDatabase()
        {
            Tables = new Dictionary<string, MockTable>(StringComparer.OrdinalIgnoreCase);
        }

        public Dictionary<string, MockTable> Tables { get; set; }

        public MockTable GetOrCreateTable<T>(string tableName)
        {
            if (!Tables.TryGetValue(tableName, out var mockTable))
            {
                var tableMembers = TypeAccessor.Create(typeof(T)).GetMembers();
                List<int> keyIndices = new List<int>();
                for (int i = 0; i < tableMembers.Count; i++)
                {
                    var attr = tableMembers[i].GetAttribute(typeof(KeyAttribute), false);
                    if (attr != null)
                    {
                        keyIndices.Add(i);
                    }
                }
                mockTable = new MockTable(tableMembers.Select(x => x.Name).ToList(), keyIndices);
                Tables.Add(tableName, mockTable);
            }
            return mockTable;
        }

        public MockTable GetTable(string tableName)
        {
            if (Tables.TryGetValue(tableName, out var mockTable))
            {
                return mockTable;
            }
            throw new NotSupportedException("Table does not exist");
        }
    }
}
