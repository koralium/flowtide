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

namespace AspireSamples.MongoDbToConsole
{
    internal class MongoDbResource : ContainerResource, IResourceWithConnectionString
    {
        public MongoDbResource(string name) : base(name)
        {
        }

        internal ReferenceExpression BuildConnectionString()
        {
            var builder = new ReferenceExpressionBuilder();
            builder.AppendLiteral("mongodb://");

            var tcpEndpoint = this.GetEndpoint("tcp");
            builder.Append($"{tcpEndpoint.Property(EndpointProperty.Host)}:{tcpEndpoint.Property(EndpointProperty.Port)}");

            return builder.Build();
        }

        public ReferenceExpression ConnectionStringExpression => BuildConnectionString();
    }
}
