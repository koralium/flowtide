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

        public static void GenerateData(int genCalls, int batchSize, string baseDir)
        {
            string auctionPath = Path.Join(baseDir, "auction_batches.bin");
            string bidPath = Path.Join(baseDir, "bid_batches.bin");
            string personPath = Path.Join(baseDir, "person_batches.bin");
            string metaPath = Path.Join(baseDir, "metadata.txt");

            bool dataFilesExist = File.Exists(auctionPath) && File.Exists(bidPath) && File.Exists(personPath);
            // Check metadata file that values are correct, if not regenerate. If files don't exist, generate.
            if (File.Exists(metaPath))
            {
                var lines = File.ReadAllLines(metaPath);
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

            Console.WriteLine($"// Generating {genCalls} rows of data into {baseDir}...");
            var generator = new NexmarkGenerator(genCalls: genCalls, batchSize: batchSize, baseDir: baseDir);
            generator.Generate();
            Console.WriteLine($"// Done generating");

            // Write metadata to file
            File.WriteAllLines(metaPath, new[]
            {
                $"GenCalls: {genCalls}",
                $"BatchSize: {batchSize}"
            });
        }
    }
}
