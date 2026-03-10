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
    internal sealed partial class RelationshipUpdate : ISpiceDbRelationshipUpdate
    {
        SpiceDbRelationshipUpdateOperation ISpiceDbRelationshipUpdate.Operation 
        { 
            get => (SpiceDbRelationshipUpdateOperation)this.Operation;
            set
            {
                this.Operation = (Types.Operation)value;
            } 
        }

        ISpiceDbRelationship ISpiceDbRelationshipUpdate.Relationship 
        { 
            get => Relationship; 
            set 
            {
                if (value is Relationship relationship)
                {
                    this.Relationship = relationship;
                }
                else if (value != null)
                {
                    var rel = (ISpiceDbRelationship)new Relationship();
                    rel.Subject = value.Subject;
                    rel.Resource = value.Resource;
                    rel.Relation = value.Relation;
                    rel.OptionalCaveat = value.OptionalCaveat;
                    this.Relationship = (Relationship)rel;
                }
                else
                {
                    this.Relationship = null;
                }
            }
        }
    }
}

namespace FlowtideDotNet.Connector.SpiceDB
{
    /// <summary>
    /// Represents a mutation of a single relationship in SpiceDB, combining the operation
    /// to perform with the relationship to act upon.
    /// </summary>
    /// <remarks>
    /// This interface wraps the Authzed API v1 <c>RelationshipUpdate</c> message. Relationship
    /// updates are submitted in batches via <see cref="ISpiceDbWriteRelationshipRequest.Updates"/>.
    /// Use the static <see cref="Create"/> factory method to construct a new instance.
    /// </remarks>
    public interface ISpiceDbRelationshipUpdate
    {
        /// <summary>
        /// Gets or sets the mutation operation to perform on the relationship.
        /// </summary>
        /// <remarks>
        /// <see cref="SpiceDbRelationshipUpdateOperation.Create"/> fails if the relationship
        /// already exists. <see cref="SpiceDbRelationshipUpdateOperation.Touch"/> upserts the
        /// relationship and succeeds even if it already exists.
        /// <see cref="SpiceDbRelationshipUpdateOperation.Delete"/> removes the relationship and
        /// no-ops if it does not exist.
        /// </remarks>
        SpiceDbRelationshipUpdateOperation Operation { get; set; }

        /// <summary>
        /// Gets or sets the relationship to create, upsert, or delete.
        /// </summary>
        ISpiceDbRelationship Relationship { get; set; }

        /// <summary>
        /// Creates a new <see cref="ISpiceDbRelationshipUpdate"/> instance with the specified
        /// operation and relationship.
        /// </summary>
        /// <param name="operation">
        /// The <see cref="SpiceDbRelationshipUpdateOperation"/> to perform on the relationship.
        /// If <c>null</c>, the operation will not be set.
        /// </param>
        /// <param name="relationship">
        /// The <see cref="ISpiceDbRelationship"/> to act upon.
        /// If <c>null</c>, the relationship will not be set.
        /// </param>
        /// <returns>
        /// A new <see cref="ISpiceDbRelationshipUpdate"/> with the specified operation and
        /// relationship.
        /// </returns>
        public static ISpiceDbRelationshipUpdate Create(
            SpiceDbRelationshipUpdateOperation? operation = default,
            ISpiceDbRelationship? relationship = default)
        {
            var result = (ISpiceDbRelationshipUpdate)new RelationshipUpdate();
            if (operation != null)
            {
                result.Operation = operation.Value;
            }
            if (relationship != null)
            {
                result.Relationship = relationship;
            }
            return result;
        }
    }
}
