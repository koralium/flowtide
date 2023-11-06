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

using FlowtideDotNet.Connector.CosmosDB;
using FlowtideDotNet.Connector.CosmosDB.Internal;
using FlowtideDotNet.Core.Engine;
using FlowtideDotNet.Substrait.Relations;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading.Tasks;
using static SqlParser.Ast.FetchDirection;

namespace FlowtideDotNet.Core.Engine
{
    public static class FlowtideCosmosDbReadWriteFactoryExtensions
    {
        public static ReadWriteFactory AddCosmosDbSink(
            this ReadWriteFactory readWriteFactory, 
            string regexPattern,
            string connectionString,
            string databaseName,
            string containerName,
            Action<WriteRelation>? transform = null)
        {
            if (regexPattern == "*")
            {
                regexPattern = ".*";
            }
            readWriteFactory.AddWriteResolver((writeRel, opt) =>
            {
                var regexResult = Regex.Match(writeRel.NamedObject.DotSeperated, regexPattern, RegexOptions.IgnoreCase);
                if (!regexResult.Success)
                {
                    return null;
                }
                transform?.Invoke(writeRel);

                return new CosmosDbSink(new FlowtideCosmosOptions()
                {
                    ConnectionString = connectionString,
                    ContainerName = containerName,
                    DatabaseName = databaseName
                }, writeRel, opt);
            });
            return readWriteFactory;
        }
    }
}
