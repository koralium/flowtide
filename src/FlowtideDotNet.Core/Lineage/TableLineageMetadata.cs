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

using FlowtideDotNet.Substrait.Type;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FlowtideDotNet.Core.Lineage
{
    /// <summary>
    /// Represents lineage metadata for a table, used to describe a dataset in an OpenLineage context.
    /// Contains the namespace, table name, and optional schema that together identify a dataset
    /// and its structure for lineage tracking.
    /// </summary>
    public class TableLineageMetadata
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="TableLineageMetadata"/> class.
        /// </summary>
        /// <param name="namespace">The OpenLineage namespace that identifies the source or destination of the dataset (e.g., a connection string or URI).</param>
        /// <param name="tableName">The name of the table or dataset.</param>
        /// <param name="schema">The optional schema describing the column names and types of the dataset.</param>
        public TableLineageMetadata(string @namespace, string tableName, NamedStruct? schema)
        {
            Namespace = @namespace;
            TableName = tableName;
            Schema = schema;
        }

        /// <summary>
        /// Gets the OpenLineage namespace that identifies the source or destination of the dataset (e.g., a connection string or URI).
        /// </summary>
        public string Namespace { get; }

        /// <summary>
        /// Gets the name of the table or dataset.
        /// </summary>
        public string TableName { get; }

        /// <summary>
        /// Gets the optional schema describing the column names and types of the dataset.
        /// </summary>
        public NamedStruct? Schema { get; } 
    }
}
