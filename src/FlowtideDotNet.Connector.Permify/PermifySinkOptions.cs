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

using FlowtideDotNet.Base;
using FlowtideDotNet.Core.Operators.Write;
using Grpc.Core;

namespace FlowtideDotNet.Connector.Permify
{
    public class PermifySinkOptions
    {
        public required ChannelBase Channel { get; set; }

        public required string TenantId { get; set; }

        public ExecutionMode ExecutionMode { get; set; } = ExecutionMode.Hybrid;

        public int BatchSize { get; set; } = 50;

        /// <summary>
        /// Allows setting custom headers, useful for authentication
        /// </summary>
        public Func<Metadata>? GetMetadata { get; set; }

        /// <summary>
        /// Called each time a new watermark is received.
        /// The second argument contains the last recieved snaptoken from permify.
        /// Can be used to keep track what data has been sent to Permify from the sources.
        /// </summary>
        public Func<Watermark, string, Task>? OnWatermarkFunc { get; set; }
    }
}
