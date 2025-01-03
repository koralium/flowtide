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

using DeltaLake.Interfaces;
using DeltaLake.Table;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FlowtideDotNet.Connector.DeltaLake
{
    public class DeltaLakeSinkOptions
    {
        /// <summary>
        /// Optional DeltaEngine instance to use for writing to Delta Lake.
        /// </summary>
        public DeltaEngine? DeltaEngine { get; set; }

        /// <summary>
        /// Possibility to provide a table instance to write to.
        /// This is useful in unit testing
        /// </summary>
        public ITable? Table { get; set; }

        public string? TableLocation { get; set; }

        public required List<string> PrimaryKeyColumns { get; set; }
    }
}
