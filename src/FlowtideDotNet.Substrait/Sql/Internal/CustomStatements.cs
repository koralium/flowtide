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

using SqlParser;
using SqlParser.Ast;

namespace FlowtideDotNet.Substrait.Sql.Internal
{
    public record BeginSubStream([property: Visit(0)] ObjectName Name) : Statement()
    {
        public override void ToSql(SqlTextWriter writer)
        {
            writer.Write("SUBSTREAM");
            writer.Write(Name);
            writer.Write(';');
        }
    };
}
