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

using FlowtideDotNet.Connector.DeltaLake.Internal.Delta.Actions;
using Stowage;
using System.Text.Json;

namespace FlowtideDotNet.Connector.DeltaLake.Internal.Delta
{
    internal static class DeltaTransactionWriter
    {
        public const string DeltaLogDirName = "_delta_log/";

        public static async Task WriteCommit(IFileStorage storage, IOPath tablePath, long version, List<DeltaAction> actions)
        {
            string fileName = version.ToString("D20") + ".json";
            using var stream = await storage.OpenWrite(tablePath.Combine(DeltaLogDirName).Combine(fileName));

            if (stream == null)
            {
                throw new Exception("Failed to open stream for writing");
            }

            using var writer = new StreamWriter(stream);

            var options = new JsonSerializerOptions()
            {
                DefaultIgnoreCondition = System.Text.Json.Serialization.JsonIgnoreCondition.WhenWritingNull,
                Encoder = System.Text.Encodings.Web.JavaScriptEncoder.UnsafeRelaxedJsonEscaping
            };
            foreach(var commit in actions)
            {
                var text = JsonSerializer.Serialize(commit, options);
                await writer.WriteLineAsync(text);
            }
            writer.Close();
            stream.Close();
        }
    }
}
