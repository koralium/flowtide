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

using FlowtideDotNet.Connector.Files;
using FlowtideDotNet.Connector.Files.Internal;
using FlowtideDotNet.Connector.Files.Internal.CsvFiles;
using FlowtideDotNet.Connector.Files.Internal.XmlFiles;
using FlowtideDotNet.Core;
using FlowtideDotNet.Substrait.Type;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Xml;
using System.Xml.Schema;

namespace Microsoft.Extensions.DependencyInjection
{
    public static class FilesConnectorManagerExtensions
    {
        public static IConnectorManager AddCsvFileSource(this IConnectorManager connectorManager, string tableName, CsvFileOptions options)
        {
            var deltaColumns = options.DeltaCsvColumns;

            if (deltaColumns == null)
            {
                deltaColumns = options.CsvColumns;
            }

            var deltaWeightFunction = options.DeltaWeightFunction;
            if (deltaWeightFunction == null)
            {
                deltaWeightFunction = (values) => 1;
            }

            var initialWeightFunction = options.InitialWeightFunction;
            if (initialWeightFunction == null)
            {
                initialWeightFunction = (values) => 1;
            }

            var outputSchema = options.OutputSchema;
            if (outputSchema == null)
            {
                List<string> columnNames = [.. options.CsvColumns];
                outputSchema = new NamedStruct()
                {
                    Names = columnNames,
                };
            }

            var internalOptions = new CsvFileInternalOptions()
            {
                CsvColumns = options.CsvColumns,
                DeltaCsvColumns = deltaColumns,
                DeltaWeightFunction = deltaWeightFunction,
                FileStorage = options.FileStorage,
                InitialFile = options.InitialFile,
                InitialWeightFunction = initialWeightFunction,
                OutputSchema = outputSchema,
                BeforeReadFile = options.BeforeReadFile,
                DeltaGetNextFile = options.DeltaGetNextFile,
                DeltaInterval = options.DeltaInterval,
                FilesHaveHeader = options.FilesHaveHeader,
                ModifyRow = options.ModifyRow,
                Delimiter = options.Delimiter
            };

            connectorManager.AddSource(new CsvFileDataSourceFactory(tableName, internalOptions));
            return connectorManager;
        }

        public static IConnectorManager AddXmlFileSource(this IConnectorManager connectorManager, string tableName, XmlFileOptions xmlFileOptions)
        {
            var schemaReader = new StringReader(xmlFileOptions.XmlSchema);
            var schemaSet = new XmlSchemaSet();
            
            var schema = XmlSchema.Read(schemaReader, (obj, events) =>
            {

            });

            if (schema == null)
            {
                throw new InvalidOperationException($"Failed to read XML schema from {xmlFileOptions.XmlSchema}");
            }

            schemaSet.Add(schema);

            var baseType = new XmlTypeParser().ParseElement(xmlFileOptions.ElementName, schemaSet);

            if (baseType is not NamedStruct namedStruct)
            {
                throw new InvalidOperationException($"Element {xmlFileOptions.ElementName} is not a complex type.");
            }

            connectorManager.AddSource(new XmlFileDataSourceFactory(tableName, new XmlFileInternalOptions()
            {
                ElementName = xmlFileOptions.ElementName,
                FileStorage = xmlFileOptions.FileStorage,
                FlowtideSchema = namedStruct,
                InitialFile = xmlFileOptions.InitialFile,
                XmlSchema = schemaSet
            }));

            return connectorManager;
        }
    }
}
