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

using Antlr4.Runtime;
using FlowtideDotNet.Connector.SpiceDB.Internal.SchemaParser.Internal;
using FlowtideDotNet.Zanzibar;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FlowtideDotNet.Connector.SpiceDB.Internal.SchemaParser
{
    internal static class SpiceDbParser
    {
        public static ZanzibarSchema ParseSchema(string schemaText)
        {
            ICharStream stream = CharStreams.fromString(schemaText);
            var lexer = new SpicedbLexer(stream);
            ITokenStream tokens = new CommonTokenStream(lexer);
            SpicedbParser parser = new SpicedbParser(tokens)
            {
                BuildParseTree = true
            };

            var context = parser.parse();

            var visitor = new SpicedbVisitor();
            var schema = visitor.Visit(context) as ZanzibarSchema;

            if (schema == null)
            {
                throw new InvalidOperationException("Failed to parse schema");
            }

            return schema;
        }
    }
}
