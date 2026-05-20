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

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FlowtideDotNet.Nexmark.Internal
{
    internal static class FileDataGenerator
    {

        public static void GenerateData(int genCalls, int batchSize)
        {
            bool dataFilesExist = File.Exists("auction_batches.bin") && File.Exists("bid_batches.bin") && File.Exists("person_batches.bin");
            // Check metadata file that values are correct, if not regenerate. If files don't exist, generate.
            if (File.Exists("metadata.txt"))
            {
                var lines = File.ReadAllLines("metadata.txt");
                if (lines.Length == 2 &&
                    long.TryParse(lines[0].Split(':')[1].Trim(), out var existingGenCalls) &&
                    int.TryParse(lines[1].Split(':')[1].Trim(), out var existingBatchSize) &&
                    existingGenCalls == genCalls &&
                    existingBatchSize == batchSize &&
                    dataFilesExist)
                {
                    return;
                }
            }

            Console.WriteLine($"// Generating {genCalls} rows of data...");
            var generator = new NexmarkGenerator(genCalls: genCalls, batchSize: batchSize);
            generator.Generate();

            // Write metadata to file
            File.WriteAllLines("metadata.txt", new[]
            {
                $"GenCalls: {genCalls}",
                $"BatchSize: {batchSize}"
            });
        }
    }
}
