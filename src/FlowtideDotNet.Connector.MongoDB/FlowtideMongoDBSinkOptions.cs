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
using MongoDB.Bson;
using MongoDB.Driver;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FlowtideDotNet.Connector.MongoDB
{
    public class FlowtideMongoDBSinkOptions
    {
        public string ConnectionString { get; set; }

        public string Database { get; set; }

        public string Collection { get; set; }

        public List<string> PrimaryKeys { get; set; }

        public Action<BsonDocument>? TransformDocument { get; set; }

        public Func<IMongoCollection<BsonDocument>, Task>? OnInitialDataSent { get; set; }

        public Func<Watermark, Task>? OnWatermarkUpdate { get; set; }

        /// <summary>
        /// Set the amount of documents that will be sent per batch to mongodb.
        /// </summary>
        public int DocumentsPerBatch { get; set; } = 100;

        /// <summary>
        /// Set the amount of batches that will be sent in parallel to mongodb.
        /// </summary>
        public int ParallelBatches { get; set; } = 10;
    }
}
