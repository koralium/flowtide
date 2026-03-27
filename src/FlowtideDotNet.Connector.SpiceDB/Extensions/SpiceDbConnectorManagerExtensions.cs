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

using FlowtideDotNet.Connector.SpiceDB;
using FlowtideDotNet.Connector.SpiceDB.Internal;

namespace FlowtideDotNet.Core
{
    /// <summary>
    /// Provides extension methods on <see cref="IConnectorManager"/> to register SpiceDB
    /// relationship sources and sinks in a Flowtide stream.
    /// </summary>
    public static class SpiceDbConnectorManagerExtensions
    {
        /// <summary>
        /// Registers a SpiceDB relationship source with the connector manager that reads
        /// relationships from tables whose names match the specified pattern.
        /// </summary>
        /// <remarks>
        /// The source streams SpiceDB relationships as rows with columns
        /// <c>resource_type</c>, <c>resource_id</c>, <c>relation</c>, <c>subject_type</c>,
        /// <c>subject_id</c>, and <c>subject_relation</c>. Use <c>"*"</c> as
        /// <paramref name="regexPattern"/> to match all table names.
        /// </remarks>
        /// <param name="connectorManager">
        /// The <see cref="IConnectorManager"/> to register the SpiceDB source with.
        /// </param>
        /// <param name="regexPattern">
        /// A regular expression pattern used to match table names declared in the stream plan.
        /// Use <c>"*"</c> to match all table names.
        /// </param>
        /// <param name="spiceDbSourceOptions">
        /// The <see cref="SpiceDbSourceOptions"/> that configure the SpiceDB gRPC channel,
        /// request metadata, and read consistency requirements.
        /// </param>
        /// <returns>
        /// The same <see cref="IConnectorManager"/> instance to allow method chaining.
        /// </returns>
        public static IConnectorManager AddSpiceDbSource(this IConnectorManager connectorManager, string regexPattern, SpiceDbSourceOptions spiceDbSourceOptions)
        {
            connectorManager.AddSource(new SpiceDbSourceFactory(regexPattern, spiceDbSourceOptions));
            return connectorManager;
        }

        /// <summary>
        /// Registers a SpiceDB relationship sink with the connector manager that writes
        /// relationships to tables whose names match the specified pattern.
        /// </summary>
        /// <remarks>
        /// The sink maps incoming rows to SpiceDB relationships using the columns
        /// <c>resource_type</c>, <c>resource_id</c>, <c>relation</c>, <c>subject_type</c>,
        /// <c>subject_id</c>, and <c>subject_relation</c>. Use <c>"*"</c> as
        /// <paramref name="regexPattern"/> to match all table names.
        /// </remarks>
        /// <param name="connectorManager">
        /// The <see cref="IConnectorManager"/> to register the SpiceDB sink with.
        /// </param>
        /// <param name="regexPattern">
        /// A regular expression pattern used to match table names declared in the stream plan.
        /// Use <c>"*"</c> to match all table names.
        /// </param>
        /// <param name="spiceDbSinkOptions">
        /// The <see cref="SpiceDbSinkOptions"/> that configure the SpiceDB gRPC channel,
        /// batch size, write hooks, watermark callbacks, and the optional deletion filter
        /// applied after the initial data load.
        /// </param>
        /// <returns>
        /// The same <see cref="IConnectorManager"/> instance to allow method chaining.
        /// </returns>
        public static IConnectorManager AddSpiceDbSink(this IConnectorManager connectorManager, string regexPattern, SpiceDbSinkOptions spiceDbSinkOptions)
        {
            connectorManager.AddSink(new SpiceDbSinkFactory(regexPattern, spiceDbSinkOptions));
            return connectorManager;
        }
    }
}
