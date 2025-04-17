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

using FlowtideDotNet.Base;
using FlowtideDotNet.Core.ColumnStore;
using FlowtideDotNet.Substrait.Type;
using Stowage;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FlowtideDotNet.Connector.Files.Tests
{
    public class XmlSourceTests
    {
        [Fact]
        public async Task TestXmlSource()
        {
            var schema = File.ReadAllText("testschema.xsd");

            XmlTestStream stream = new XmlTestStream("testxmlsource", new XmlFileOptions()
            {
                ElementName = "shiporder",
                FileStorage = Stowage.Files.Of.LocalDisk("./"),
                GetInitialFiles = (storage, state) =>
                {
                    return Task.FromResult<IEnumerable<string>>(new List<string>() { "test.xml" });
                },
                XmlSchema = schema,
                ExtraColumns = new List<FileExtraColumn>()
                {
                    new FileExtraColumn("my_col", new StringType(), (fileName, batchNumber, state) =>
                    {
                        return new StringValue("hello");
                    })
                }
            });

            await stream.StartStream(@"
                INSERT INTO output
                SELECT * FROM test
            ");

            await stream.WaitForUpdate();

            stream.AssertCurrentDataEqual(new[]
            {
                new {
                    orderperson = "John Smith",
                    shipto = new
                    {
                        name = "Ola Nordmann",
                        address = "Langgt 23",
                        city = "4000 Stavanger",
                        country = "Norway"
                    },
                    item = new[] {
                        new
                        {
                            title = "Empire Burlesque",
                            note = (string?)"Special Edition",
                            quantity = 1,
                            price = 10.90m,
                        },
                        new
                        {
                            title = "Hide your heart",
                            note = default(string),
                            quantity = 1,
                            price = 9.90m
                        }
                    },
                    orderid = "889923",
                    my_col = "hello"
                },
                new {
                    orderperson = "John Smit",
                    shipto = new
                    {
                        name = "Ola Nordman",
                        address = "Langgt 23",
                        city = "4000 Stavanger",
                        country = "Sweden"
                    },
                    item = new[] {
                        new
                        {
                            title = "Empire Burlesque",
                            note = (string?)"Special Edition",
                            quantity = 1,
                            price = 10.90m,
                        },
                        new
                        {
                            title = "Hide your heart",
                            note = default(string),
                            quantity = 1,
                            price = 9.90m,
                        }
                    },
                    orderid = "889924",
                    my_col = "hello"
                }

            });
        }
    }
}
