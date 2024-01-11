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

using Azure.Core;

namespace FlowtideDotNet.Connector.Sharepoint
{
    public class SharepointSinkOptions
    {
        /// <summary>
        /// Name of the columns that should be treated as the primary key.
        /// </summary>
        public required List<string> PrimaryKeyColumnNames { get; set; }

        /// <summary>
        /// The URL of the Sharepoint tenant without 'https://'
        /// Example: {tenant}.sharepoint.com
        /// </summary>
        public required string SharepointUrl { get; set; }

        /// <summary>
        /// The name of the Sharepoint site
        /// </summary>
        public required string Site { get; set; }
        
        /// <summary>
        /// Token credential to use for authentication
        /// </summary>
        public required TokenCredential TokenCredential { get; set; }

        /// <summary>
        /// Do not delete items from Sharepoint
        /// </summary>
        public bool DisableDelete { get; set; }

        /// <summary>
        /// Stop the stream if a UPN is not found
        /// </summary>
        public bool ThrowOnPersonOrGroupNotFound { get; set; }

        /// <summary>
        /// Preprocess the row before it is sent to Sharepoint.
        /// Allows addition of metadata columns.
        /// </summary>
        public Action<Dictionary<string, object>>? PreprocessRow { get; set; }
    }
}
