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

        public static Task WriteCommit(IFileStorage storage, IOPath tablePath, long version, List<DeltaAction> actions)
        {
            return WriteActions(storage, tablePath.Combine(DeltaLogDirName).Combine(CommitFileName(version)), actions);
        }

        /// <summary>
        /// Writes the commit to a hidden file, published later.
        /// </summary>
        public static async Task<string> StageCommit(IFileStorage storage, IOPath tablePath, long version, List<DeltaAction> actions)
        {
            // Hidden name, log readers skip it.
            var stagedFileName = $".{CommitFileName(version)}.{Guid.NewGuid():N}.tmp";
            await WriteActions(storage, tablePath.Combine(DeltaLogDirName).Combine(stagedFileName), actions);
            return stagedFileName;
        }

        /// <summary>
        /// Moves a staged commit into the log, idempotent.
        /// </summary>
        public static async Task PublishCommit(IFileStorage storage, IOPath tablePath, long version, string stagedFileName)
        {
            var logPath = tablePath.Combine(DeltaLogDirName);
            var stagedPath = logPath.Combine(stagedFileName);
            var commitPath = logPath.Combine(CommitFileName(version));

            if (!await storage.Exists(stagedPath))
            {
                // Published by an earlier call.
                if (await storage.Exists(commitPath))
                {
                    return;
                }
                throw new InvalidOperationException($"Staged commit '{stagedFileName}' for version {version} is missing");
            }

            // Stowage Ren copies then deletes, fails on Windows disk.
            using (var source = await storage.OpenRead(stagedPath))
            using (var target = await storage.OpenWrite(commitPath))
            {
                if (source == null || target == null)
                {
                    throw new InvalidOperationException($"Failed to publish staged commit '{stagedFileName}' as version {version}");
                }
                await source.CopyToAsync(target);
            }

            await storage.Rm(stagedPath);
        }

        private static string CommitFileName(long version)
        {
            return version.ToString("D20") + ".json";
        }

        private static async Task WriteActions(IFileStorage storage, IOPath path, List<DeltaAction> actions)
        {
            using var stream = await storage.OpenWrite(path);

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
            foreach (var commit in actions)
            {
                var text = JsonSerializer.Serialize(commit, options);
                await writer.WriteLineAsync(text);
            }
            writer.Close();
            stream.Close();
        }
    }
}
