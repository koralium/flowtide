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
    internal sealed partial class ReadRelationshipsRequest : ISpiceDbReadRelationshipsRequest
    {
        ISpiceDbRelationshipFilter ISpiceDbReadRelationshipsRequest.RelationshipFilter 
        { 
            get => RelationshipFilter; 
            set
            {
                if (value is RelationshipFilter relationshipFilter)
                {
                    this.RelationshipFilter = relationshipFilter;
                }
                else if (value != null)
                {
                    var rel = (ISpiceDbRelationshipFilter)new RelationshipFilter();
                    rel.OptionalSubjectFilter = value.OptionalSubjectFilter;
                    rel.OptionalRelation = value.OptionalRelation;
                    rel.OptionalResourceId = value.OptionalResourceId;
                    this.RelationshipFilter = (RelationshipFilter)rel;
                }
                else
                {
                    this.RelationshipFilter = null;
                }
            }
        }

        ISpiceDbConsistency ISpiceDbReadRelationshipsRequest.Consistency 
        { 
            get => Consistency; 
            set
            {
                if (value is Consistency consistency)
                {
                    this.Consistency = consistency;
                }
                else if (value != null)
                {
                    var c = (ISpiceDbConsistency)new Consistency();
                    c.AtExactSnapshot = value.AtExactSnapshot;
                    c.FullyConsistent = value.FullyConsistent;
                    c.AtLeastAsFresh = value.AtLeastAsFresh;
                    c.MinimizeLatency = value.MinimizeLatency;
                    this.Consistency = (Consistency)c;
                }
                else
                {
                    this.Consistency = null;
                }
            }
        }
    }
}

namespace FlowtideDotNet.Connector.SpiceDB
{
    /// <summary>
    /// Represents a request to read relationships from SpiceDB that match one or more filters.
    /// </summary>
    /// <remarks>
    /// This interface wraps the Authzed API v1 <c>ReadRelationshipsRequest</c> message. It is
    /// used to query SpiceDB for existing relationships via the <c>ReadRelationships</c> RPC, and
    /// is also accepted by <see cref="SpiceDbSinkOptions.DeleteExistingDataFilter"/> to identify
    /// relationships that should be removed after the initial data load has completed.
    /// Use the static <see cref="Create"/> factory method to construct a new instance.
    /// </remarks>
    public interface ISpiceDbReadRelationshipsRequest
    {
        /// <summary>
        /// Gets or sets the filter applied to the relationships returned by the read operation.
        /// </summary>
        ISpiceDbRelationshipFilter RelationshipFilter { get; set; }

        /// <summary>
        /// Gets or sets the consistency requirement for the read operation.
        /// </summary>
        /// <remarks>
        /// When <c>null</c>, the SpiceDB server applies its default consistency behaviour.
        /// Use <see cref="ISpiceDbConsistency"/> factory methods to specify an explicit requirement.
        /// </remarks>
        ISpiceDbConsistency Consistency { get; set; }

        /// <summary>
        /// Creates a new <see cref="ISpiceDbReadRelationshipsRequest"/> instance with the specified
        /// relationship filter and consistency requirement.
        /// </summary>
        /// <param name="relationshipFilter">
        /// The <see cref="ISpiceDbRelationshipFilter"/> to apply to the relationships returned by
        /// the read operation. If <c>null</c>, the filter will not be set.
        /// </param>
        /// <param name="consistency">
        /// The <see cref="ISpiceDbConsistency"/> requirement for the read operation.
        /// If <c>null</c>, the consistency will not be set.
        /// </param>
        /// <returns>
        /// A new <see cref="ISpiceDbReadRelationshipsRequest"/> with the specified filter and
        /// consistency requirement.
        /// </returns>
        public static ISpiceDbReadRelationshipsRequest Create(
            ISpiceDbRelationshipFilter? relationshipFilter = default,
            ISpiceDbConsistency? consistency = default
            )
        {
            var readReq = (ISpiceDbReadRelationshipsRequest)new ReadRelationshipsRequest();
            if (relationshipFilter != null)
            {
                readReq.RelationshipFilter = relationshipFilter;
            }
            if (consistency != null)
            {
                readReq.Consistency = consistency;
            }
            return readReq;
        }
    }
}
