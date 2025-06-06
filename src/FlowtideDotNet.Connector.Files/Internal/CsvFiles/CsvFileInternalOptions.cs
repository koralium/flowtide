﻿// Licensed under the Apache License, Version 2.0 (the "License")
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

using FlowtideDotNet.Substrait.Type;
using Stowage;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FlowtideDotNet.Connector.Files.Internal.CsvFiles
{
    /// <summary>
    /// Internal options class with alot of the properties as required for easy null handling.
    /// </summary>
    public class CsvFileInternalOptions
    {
        /// <summary>
        /// File storage where to find the files
        /// </summary>
        public required IFileStorage FileStorage { get; set; }

        /// <summary>
        /// Contains the columns in the CSV file
        /// </summary>
        public required List<string> CsvColumns { get; set; }

        /// <summary>
        /// Which columns that should be output from the source, and optionally what data type
        /// </summary>
        public required NamedStruct OutputSchema { get; set; }

        public required string Delimiter { get; set; }

        /// <summary>
        /// The initial file to read from for initial load.
        /// </summary>
        public required Func<Task<IEnumerable<string>>> GetInitialFiles { get; set; }

        /// <summary>
        /// If the file have a header, set this to true to skip it.
        /// </summary>
        public bool FilesHaveHeader { get; set; } = false;

        /// <summary>
        /// Hook before a file is read, allows loading in custom state data that can be accessed in modify row.
        /// First parameter is the file name that will be read, second is the batch number, third is the state that can be modified.
        /// </summary>
        public Func<string, long, Dictionary<string, string>, IFileStorage, Task>? BeforeReadFile { get; set; }

        public Func<long, Dictionary<string, string>, IFileStorage, Task>? BeforeBatch { get; set; }

        /// <summary>
        /// Optional modify function, the first parameter contains the columns from the csv and the second is the output columns before being converted.
        /// The third parameter is the batch number and the fourth is the file name, the fifth is the custom state.
        /// </summary>
        public Action<string?[], string?[], long, string, Dictionary<string, string>>? ModifyRow { get; set; }

        public required Func<string?[], int> InitialWeightFunction { get; set; }

        public required List<string> DeltaCsvColumns { get; set; }

        public required Func<string?[], int> DeltaWeightFunction { get; set; }

        public Func<string, long, string>? DeltaGetNextFile { get; set; }

        public TimeSpan? DeltaInterval { get; set; }
    }
}
