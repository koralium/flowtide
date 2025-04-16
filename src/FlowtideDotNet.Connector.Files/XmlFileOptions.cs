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

using Stowage;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FlowtideDotNet.Connector.Files
{
    public class XmlFileOptions
    {
        /// <summary>
        /// File storage where to find the files
        /// </summary>
        public required IFileStorage FileStorage { get; set; }

        /// <summary>
        /// The xml schema that the file will be in. Used to create metadata and parsers.
        /// </summary>
        public required string XmlSchema { get; set; }

        /// <summary>
        /// The element name in the schema which will be used as rows
        /// </summary>
        public required string ElementName { get; set; }

        /// <summary>
        /// The initial file to read from for initial load.
        /// </summary>
        public required string InitialFile { get; set; }
    }
}
