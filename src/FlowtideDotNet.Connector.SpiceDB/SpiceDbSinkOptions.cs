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

using Authzed.Api.V1;
using FlowtideDotNet.Base;
using Grpc.Core;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FlowtideDotNet.Connector.SpiceDB
{
    public class SpiceDbSinkOptions
    {
        public required ChannelBase Channel { get; set; }

        public Func<Metadata>? GetMetadata { get; set; }

        public int BatchSize { get; set; } = 50;

        /// <summary>
        /// If set, all relationships returned by this filter that are not in the result set
        /// will be deleted after the initial loading of data has completed.
        /// </summary>
        public ReadRelationshipsRequest? DeleteExistingDataFilter { get; set; }

        /// <summary>
        /// Called before each write to SpiceDB.
        /// Makes it possible to modify any data before it is sent.
        /// </summary>
        public Func<WriteRelationshipsRequest, Task>? BeforeWriteRequestFunc { get; set; }

        /// <summary>
        /// Called each time a new watermark is received.
        /// The second argument contains the last recieved zedtoken from spicedb.
        /// Can be used to keep track what data has been sent to SpiceDB from the sources.
        /// </summary>
        public Func<Watermark, string, Task>? OnWatermarkFunc { get; set; }

        /// <summary>
        /// Called when the initial data has been sent to the SpiceDB API.
        /// This can be used to do any cleanup of data.
        /// 
        /// Such as if an external store is used to store tuples that this integration has handled.
        /// It can then delete old tuples that was not present in the initial data.
        /// </summary>
        public Func<Task>? OnInitialDataSentFunc { get; set; }
    }
}
