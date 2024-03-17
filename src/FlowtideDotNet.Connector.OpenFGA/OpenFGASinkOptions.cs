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
using FlowtideDotNet.Core.Operators.Write;
using OpenFga.Sdk.Client;
using OpenFga.Sdk.Client.Model;
using OpenFga.Sdk.Model;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FlowtideDotNet.Connector.OpenFGA
{
    public class OpenFgaSinkOptions
    {
        /// <summary>
        /// The OpenFGA client configuration.
        /// Used to connect to the OpenFGA API.
        /// </summary>
        public required OpenFga.Sdk.Client.ClientConfiguration ClientConfiguration { get; set; }

        /// <summary>
        /// Function that is run before each write of a tuple.
        /// Makes it possible to send true/false back where true executes the write and false does not.
        /// This method can be used to store tuples in an external store that this integration has handled.
        /// Can be beneficial if one wants to remove all tuples added by the integration.
        /// 
        /// This method can be called multiple times in parallell.
        /// </summary>
        public Func<ClientTupleKey, Task<bool>>? BeforeWriteFunc { get; set; }

        /// <summary>
        /// Function that is run before each delete of a tuple.
        /// Makes it possible to send true/false back where true executes the delete and false does not.
        /// This method can be used to store tuples in an external store that this integration has handled.
        /// Can be beneficial if one wants to remove all tuples added by the integration.
        /// 
        /// This method can be called multiple times in parallell.
        /// </summary>
        public Func<ClientTupleKeyWithoutCondition, Task<bool>>? BeforeDeleteFunc { get; set; }

        public int MaxParallellCalls { get; set; } = 50;

        /// <summary>
        /// Called when the initial data has been sent to the OpenFGA API.
        /// This can be used to do any cleanup of data.
        /// 
        /// Such as if an external store is used to store tuples that this integration has handled.
        /// It can then delete old tuples that was not present in the initial data.
        /// </summary>
        public Func<Task>? OnInitialDataSentFunc { get; set; }

        /// <summary>
        /// Called each time a new watermark is received.
        /// Can be used to keep track what data has been sent to OpenFGA from the sources.
        /// </summary>
        public Func<Watermark, Task>? OnWatermarkFunc { get; set; }

        /// <summary>
        /// If set, all relationships returned by this query that are not in the result set
        /// will be deleted after the initial loading of data has completed.
        /// </summary>
        public Func<OpenFgaClient, IAsyncEnumerable<TupleKey>>? DeleteExistingDataFetcher { get; set; }

        public ExecutionMode ExecutionMode { get; set; } = ExecutionMode.Hybrid;
    }
}
