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
    internal sealed partial class ContextualizedCaveat : ISpiceDbContextualizedCaveat
    {

    }
}

    namespace FlowtideDotNet.Connector.SpiceDB
{
    /// <summary>
    /// Represents a caveat attached to a caveated relationship in SpiceDB, combining
    /// a caveat expression name with its evaluation context.
    /// </summary>
    /// <remarks>
    /// This interface wraps the Authzed API v1 <c>ContextualizedCaveat</c> message.
    /// A caveated relationship is considered active only when the referenced caveat expression
    /// evaluates to <c>true</c> given the supplied context. Instances are assigned via
    /// <see cref="ISpiceDbRelationship.OptionalCaveat"/>. Use the static <see cref="Create"/>
    /// factory method to construct a new instance.
    /// </remarks>
    public interface ISpiceDbContextualizedCaveat
    {
        /// <summary>
        /// Gets or sets the name of the caveat expression to use, as defined in the SpiceDB schema.
        /// </summary>
        string CaveatName { get; set; }

        /// <summary>
        /// Gets or sets the key-value pairs that are injected into the caveat expression at evaluation time.
        /// </summary>
        /// <remarks>
        /// The keys must match the arguments defined for the caveat in the SpiceDB schema.
        /// These values are fixed at write time and passed to the caveat expression when
        /// the relationship is evaluated.
        /// </remarks>
        Google.Protobuf.WellKnownTypes.Struct Context { get; set; }

        /// <summary>
        /// Creates a new <see cref="ISpiceDbContextualizedCaveat"/> instance with the specified
        /// caveat name and evaluation context.
        /// </summary>
        /// <param name="caveatName">
        /// The name of the caveat expression to use, as defined in the SpiceDB schema.
        /// If <c>null</c>, the caveat name will not be set.
        /// </param>
        /// <param name="context">
        /// The key-value pairs to inject into the caveat expression at evaluation time.
        /// The keys must match the arguments defined for the caveat in the schema.
        /// If <c>null</c>, no context will be set.
        /// </param>
        /// <returns>
        /// A new <see cref="ISpiceDbContextualizedCaveat"/> with the specified caveat name and context.
        /// </returns>
        public static ISpiceDbContextualizedCaveat Create(
            string? caveatName = default,
            Google.Protobuf.WellKnownTypes.Struct? context = default)
        {
            var result = new ContextualizedCaveat();
            if (caveatName != null)
            {
                result.CaveatName = caveatName;
            }
            if (context != null)
            {
                result.Context = context;
            }
            return result;
        }
    }
}
