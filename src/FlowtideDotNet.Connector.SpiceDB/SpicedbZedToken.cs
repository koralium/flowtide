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
using FlowtideDotNet.Connector.SpiceDB;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Authzed.Api.V1
{
    internal sealed partial class ZedToken : ISpiceDbZedToken
    {

    }
}

namespace FlowtideDotNet.Connector.SpiceDB
{
    /// <summary>
    /// Represents a ZedToken that encodes causality metadata for SpiceDB read and write operations.
    /// </summary>
    /// <remarks>
    /// This interface wraps the Authzed API v1 <c>ZedToken</c> message. ZedTokens are returned
    /// by SpiceDB after each write operation and can be stored and supplied back in subsequent
    /// read operations to establish causality guarantees. They are used as
    /// <see cref="ISpiceDbConsistency.AtLeastAsFresh"/> to ensure reads reflect at least a known
    /// write, or as <see cref="ISpiceDbConsistency.AtExactSnapshot"/> to read at a precise
    /// revision. The raw token string is also surfaced by
    /// <see cref="SpiceDbSinkOptions.OnWatermarkFunc"/> after each batch is committed.
    /// Use the static <see cref="Create"/> factory method to construct a new instance.
    /// </remarks>
    public interface ISpiceDbZedToken
    {
        /// <summary>
        /// Gets or sets the opaque token string returned by SpiceDB that encodes the snapshot
        /// revision at which a write was committed.
        /// </summary>
        string Token { get; set; }

        /// <summary>
        /// Creates a new <see cref="ISpiceDbZedToken"/> instance from the specified token string.
        /// </summary>
        /// <param name="token">
        /// The opaque token string returned by SpiceDB after a write operation, encoding the
        /// snapshot revision to use for causality.
        /// </param>
        /// <returns>
        /// A new <see cref="ISpiceDbZedToken"/> with the specified token string.
        /// </returns>
        public static ISpiceDbZedToken Create(string token)
        {
            return new ZedToken()
            {
                Token = token
            };
        }
    }
}
