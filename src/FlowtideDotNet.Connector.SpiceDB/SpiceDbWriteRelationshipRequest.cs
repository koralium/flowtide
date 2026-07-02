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
using FlowtideDotNet.Connector.SpiceDB.Internal;

namespace Authzed.Api.V1
{
    internal sealed partial class WriteRelationshipsRequest : ISpiceDbWriteRelationshipRequest
    {
        private SpiceDbListWrapper<RelationshipUpdate, ISpiceDbRelationshipUpdate>? _updatesWrapper;
        private SpiceDbListWrapper<Precondition, ISpiceDbPrecondition>? _preconditionsWrapper;

        IList<ISpiceDbRelationshipUpdate> ISpiceDbWriteRelationshipRequest.Updates
        {
            get
            {
                return _updatesWrapper ??= new SpiceDbListWrapper<RelationshipUpdate, ISpiceDbRelationshipUpdate>(this.Updates);
            }
        }

        IList<ISpiceDbPrecondition> ISpiceDbWriteRelationshipRequest.OptionalPreconditions
        {
            get
            {
                return _preconditionsWrapper ??= new SpiceDbListWrapper<Precondition, ISpiceDbPrecondition>(this.OptionalPreconditions);
            }
        }
    }
}

namespace FlowtideDotNet.Connector.SpiceDB
{
    /// <summary>
    /// Represents a request to atomically apply a set of relationship mutations to SpiceDB,
    /// with optional preconditions.
    /// </summary>
    /// <remarks>
    /// This interface wraps the Authzed API v1 <c>WriteRelationshipsRequest</c> message. The
    /// mutations in <see cref="Updates"/> are submitted to SpiceDB via the
    /// <c>WriteRelationships</c> RPC. When <see cref="OptionalPreconditions"/> is non-empty,
    /// all specified preconditions must be satisfied before the write is committed; if any
    /// precondition fails the entire write is rejected. The request is also passed to
    /// <see cref="SpiceDbSinkOptions.BeforeWriteRequestFunc"/> before each batch submission,
    /// allowing callers to inspect or modify the updates prior to sending.
    /// Use the static <see cref="Create"/> factory method to construct a new instance.
    /// </remarks>
    public interface ISpiceDbWriteRelationshipRequest
    {
        /// <summary>
        /// Gets the mutable list of relationship mutations to apply atomically.
        /// </summary>
        /// <remarks>
        /// Add <see cref="ISpiceDbRelationshipUpdate"/> instances to specify which relationships
        /// to create, upsert, or delete as part of this request.
        /// </remarks>
        IList<ISpiceDbRelationshipUpdate> Updates { get; }

        /// <summary>
        /// Gets the mutable list of preconditions that must all be satisfied before the
        /// mutations in <see cref="Updates"/> are committed.
        /// </summary>
        /// <remarks>
        /// When empty, no preconditions are evaluated and the write proceeds unconditionally.
        /// If any precondition is not satisfied, the entire write is rejected without applying
        /// any of the mutations.
        /// </remarks>
        IList<ISpiceDbPrecondition> OptionalPreconditions { get; }

        /// <summary>
        /// Creates a new <see cref="ISpiceDbWriteRelationshipRequest"/> instance pre-populated
        /// with the specified relationship updates and optional preconditions.
        /// </summary>
        /// <param name="updates">
        /// The <see cref="ISpiceDbRelationshipUpdate"/> mutations to include in the request.
        /// If <c>null</c>, no updates will be added.
        /// </param>
        /// <param name="optionalPreconditions">
        /// The <see cref="ISpiceDbPrecondition"/> conditions that must all be satisfied before
        /// the write is committed. If <c>null</c>, no preconditions will be added and the write
        /// proceeds unconditionally.
        /// </param>
        /// <returns>
        /// A new <see cref="ISpiceDbWriteRelationshipRequest"/> containing the specified updates
        /// and preconditions.
        /// </returns>
        public static ISpiceDbWriteRelationshipRequest Create(
            IEnumerable<ISpiceDbRelationshipUpdate>? updates = default,
            IEnumerable<ISpiceDbPrecondition>? optionalPreconditions = default)
        {
            var result = (ISpiceDbWriteRelationshipRequest)new WriteRelationshipsRequest();
            if (updates != null)
            {
                foreach (var update in updates)
                {
                    result.Updates.Add(update);
                }
            }
            if (optionalPreconditions != null)
            {
                foreach (var precondition in optionalPreconditions)
                {
                    result.OptionalPreconditions.Add(precondition);
                }
            }
            return result;
        }
    }
}
