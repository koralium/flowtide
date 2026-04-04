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

namespace Authzed.Api.V1
{
    internal sealed partial class Consistency : ISpiceDbConsistency
    {
        ISpiceDbZedToken ISpiceDbConsistency.AtExactSnapshot 
        { 
            get => AtExactSnapshot; 
            set
            {
                if (value is ZedToken zedToken)
                {
                    this.AtExactSnapshot = zedToken;
                }
                else if (value != null)
                {
                    var token = (ISpiceDbZedToken)new ZedToken();
                    token.Token = value.Token;
                    this.AtExactSnapshot = (ZedToken)token;
                }
                else
                {
                    this.AtExactSnapshot = null;
                }
            }
        }
        ISpiceDbZedToken ISpiceDbConsistency.AtLeastAsFresh 
        { 
            get => AtLeastAsFresh; 
            set
            {
                if (value is ZedToken zedToken)
                {
                    this.AtLeastAsFresh = zedToken;
                }
                else if (value != null)
                {
                    var token = (ISpiceDbZedToken)new ZedToken();
                    token.Token = value.Token;
                    this.AtLeastAsFresh = (ZedToken)token;
                }
                else
                {
                    this.AtLeastAsFresh = null;
                }
            } 
        }
    }
}

namespace FlowtideDotNet.Connector.SpiceDB
{
    /// <summary>
    /// Represents the consistency requirements for a SpiceDB API request,
    /// controlling how data freshness is balanced against latency.
    /// </summary>
    /// <remarks>
    /// This interface wraps the Authzed API v1 <c>Consistency</c> message. Each request must
    /// specify exactly one consistency requirement. Use the static factory methods
    /// (<see cref="CreateFullyConsistent"/>, <see cref="CreateMinimizeLatency"/>,
    /// <see cref="CreateAtExactSnapshot"/>, <see cref="CreateAtLeastAsFresh"/>) to create instances.
    /// </remarks>
    public interface ISpiceDbConsistency
    {
        /// <summary>
        /// Gets or sets a value indicating that all data used in the API call must be
        /// at the most recent snapshot found.
        /// </summary>
        /// <remarks>
        /// Using this option can be quite slow. Unless there is a specific need to enforce
        /// full consistency, it is recommended to use <see cref="AtLeastAsFresh"/> with a stored
        /// <see cref="ISpiceDbZedToken"/> instead.
        /// </remarks>
        bool FullyConsistent { get; set; }

        /// <summary>
        /// Gets a value indicating whether <see cref="FullyConsistent"/> has been set on this instance.
        /// </summary>
        bool HasFullyConsistent { get; }

        /// <summary>
        /// Gets a value indicating whether <see cref="MinimizeLatency"/> has been set on this instance.
        /// </summary>
        bool HasMinimizeLatency { get; }

        /// <summary>
        /// Gets or sets a value indicating that the latency for the call should be minimized
        /// by having the system select the fastest snapshot available.
        /// </summary>
        bool MinimizeLatency { get; set; }

        /// <summary>
        /// Gets or sets the <see cref="ISpiceDbZedToken"/> that specifies the exact snapshot
        /// at which all data used in the API call must be evaluated.
        /// </summary>
        /// <remarks>
        /// If the specified snapshot is no longer available, an error will be returned to the caller.
        /// </remarks>
        ISpiceDbZedToken AtExactSnapshot { get; set; }

        /// <summary>
        /// Gets or sets the <see cref="ISpiceDbZedToken"/> that specifies the minimum freshness
        /// of data used in the API call.
        /// </summary>
        /// <remarks>
        /// All data used in the API call must be at least as fresh as that found in the provided
        /// <see cref="ISpiceDbZedToken"/>. More recent data may be used if it is available or faster.
        /// </remarks>
        ISpiceDbZedToken AtLeastAsFresh { get; set; }

        /// <summary>
        /// Creates an <see cref="ISpiceDbConsistency"/> instance that requires all data used
        /// in the API call to be at the most recent snapshot found.
        /// </summary>
        /// <returns>
        /// An <see cref="ISpiceDbConsistency"/> with <see cref="FullyConsistent"/> set to <c>true</c>.
        /// </returns>
        public static ISpiceDbConsistency CreateFullyConsistent()
        {
            return new Consistency()
            {
                FullyConsistent = true
            };
        }

        /// <summary>
        /// Creates an <see cref="ISpiceDbConsistency"/> instance that minimizes latency
        /// by having the system select the fastest available snapshot.
        /// </summary>
        /// <returns>
        /// An <see cref="ISpiceDbConsistency"/> with <see cref="MinimizeLatency"/> set to <c>true</c>.
        /// </returns>
        public static ISpiceDbConsistency CreateMinimizeLatency()
        {
            return new Consistency()
            {
                MinimizeLatency = true
            };
        }

        /// <summary>
        /// Creates an <see cref="ISpiceDbConsistency"/> instance that requires all data used
        /// in the API call to be evaluated at the exact snapshot identified by <paramref name="zedToken"/>.
        /// </summary>
        /// <param name="zedToken">
        /// The <see cref="ISpiceDbZedToken"/> identifying the exact snapshot to use.
        /// If the snapshot is no longer available, the API call will return an error.
        /// </param>
        /// <returns>
        /// An <see cref="ISpiceDbConsistency"/> with <see cref="AtExactSnapshot"/> set to <paramref name="zedToken"/>.
        /// </returns>
        public static ISpiceDbConsistency CreateAtExactSnapshot(ISpiceDbZedToken zedToken)
        {
            var result = (ISpiceDbConsistency)new Consistency();
            result.AtExactSnapshot = zedToken;
            return result;
        }

        /// <summary>
        /// Creates an <see cref="ISpiceDbConsistency"/> instance that requires all data used
        /// in the API call to be at least as fresh as the snapshot identified by <paramref name="zedToken"/>.
        /// </summary>
        /// <param name="zedToken">
        /// The <see cref="ISpiceDbZedToken"/> identifying the minimum required freshness.
        /// More recent data may be used if it is available or faster.
        /// </param>
        /// <returns>
        /// An <see cref="ISpiceDbConsistency"/> with <see cref="AtLeastAsFresh"/> set to <paramref name="zedToken"/>.
        /// </returns>
        public static ISpiceDbConsistency CreateAtLeastAsFresh(ISpiceDbZedToken zedToken)
        {
            var result = (ISpiceDbConsistency)new Consistency();
            result.AtLeastAsFresh = zedToken;
            return result;
        }
    }
}
