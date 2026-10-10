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
using System.Text;
using System.Text.Json;

namespace FlowtideDotNet.Connector.DeltaLake.Internal.Delta
{
    /// <summary>
    /// A staged commit, the stage id is null for commits without a Flowtide commitInfo.
    /// </summary>
    internal sealed record StagedCommit(string FileName, string? StageId, long Length, CommitBytes Bytes);

    internal enum PublishOutcome
    {
        Published,
        AlreadyPublished,
        Repaired,
        FailedClosed
    }

    internal readonly record struct PublishResult(PublishOutcome Outcome, string? Reason = null);

    internal static class DeltaTransactionWriter
    {
        public const string DeltaLogDirName = "_delta_log/";

        private static readonly JsonSerializerOptions s_jsonOptions = new JsonSerializerOptions()
        {
            DefaultIgnoreCondition = System.Text.Json.Serialization.JsonIgnoreCondition.WhenWritingNull,
            Encoder = System.Text.Encodings.Web.JavaScriptEncoder.UnsafeRelaxedJsonEscaping
        };

        public static Task WriteCommit(IFileStorage storage, IOPath tablePath, long version, List<DeltaAction> actions)
        {
            return WriteBytes(storage, tablePath.Combine(DeltaLogDirName).Combine(CommitFileName(version)), Serialize(actions), 0);
        }

        /// <summary>
        /// Writes the commit to a hidden file, published later.
        /// The stage id is read from a leading commitInfo and names the file.
        /// </summary>
        public static async Task<StagedCommit> StageCommit(IFileStorage storage, IOPath tablePath, long version, List<DeltaAction> actions)
        {
            var stageId = actions.Count > 0 ? actions[0].CommitInfo?.StageId : null;
            var bytes = Serialize(actions);
            // Hidden name, log readers skip it.
            var stagedFileName = $".{CommitFileName(version)}.{stageId ?? Guid.NewGuid().ToString("N")}.tmp";
            await WriteBytes(storage, tablePath.Combine(DeltaLogDirName).Combine(stagedFileName), bytes, 0);
            return new StagedCommit(stagedFileName, stageId, bytes.Length, bytes);
        }

        /// <summary>
        /// Publishes a staged commit, the target is checked first and is never overwritten unless it is a torn copy of the stage.
        /// The stage bytes are optional, they save reading the staged file in the run that staged it.
        /// With <paramref name="checkSuccessor"/> an absent target is only written while the log holds nothing at its version or later.
        /// </summary>
        public static async Task<PublishResult> PublishCommit(IFileStorage storage, IOPath tablePath, DeltaLakePendingCommit pending, CommitBytes? stageBytes, bool checkSuccessor)
        {
            var logPath = tablePath.Combine(DeltaLogDirName);
            var stagedPath = logPath.Combine(pending.StagedFile);
            var commitPath = logPath.Combine(CommitFileName(pending.Version));

            var target = await CommitBytes.Read(storage, commitPath);

            // A conflicting commit removed after another writer went on, or checkpointed it, must not be replaced
            if (target == null && checkSuccessor && await HistoryFrom(storage, tablePath, pending.Version))
            {
                return Failed(pending, "is absent but the log holds a later version or a checkpoint at this version, publishing it would break the log");
            }

            if (pending.StageId == null)
            {
                return await PublishLegacy(storage, stagedPath, commitPath, pending, target);
            }

            var prefix = StagePrefix(pending.StageId);
            if (target == null)
            {
                var (stageState, stage) = await ReadStage(storage, stagedPath, pending, prefix, stageBytes);
                if (stageState != StageState.Valid)
                {
                    return Failed(pending, "was never published and its staged commit is missing or damaged");
                }
                await WriteBytes(storage, commitPath, stage!, prefix.Length);
                await storage.Rm(stagedPath);
                return new PublishResult(PublishOutcome.Published);
            }

            if (target.StartsWith(prefix))
            {
                if (target.Length == pending.Length)
                {
                    // Only a removed stage falls back to the first line, a damaged one cannot vouch for the target
                    var (stageState, stage) = await ReadStage(storage, stagedPath, pending, prefix, stageBytes);
                    var ours = stageState switch
                    {
                        StageState.Valid => target.ContentEquals(stage!),
                        StageState.Missing => FirstLineHasStageId(target, pending.StageId),
                        _ => false
                    };
                    if (!ours)
                    {
                        return Failed(pending, "carries this sink's stage id but cannot be matched to the staged commit");
                    }
                    await storage.Rm(stagedPath);
                    return new PublishResult(PublishOutcome.AlreadyPublished);
                }
                if (target.Length < pending.Length)
                {
                    var (stageState, stage) = await ReadStage(storage, stagedPath, pending, prefix, stageBytes);
                    if (stageState != StageState.Valid || !stage!.StartsWith(target))
                    {
                        return Failed(pending, "is a torn copy of this sink's commit and the staged commit cannot repair it");
                    }
                    await WriteBytes(storage, commitPath, stage!, prefix.Length);
                    await storage.Rm(stagedPath);
                    return new PublishResult(PublishOutcome.Repaired);
                }
            }
            return Failed(pending, target.Length == 0 ? "exists but is empty" : "was written by another writer or is damaged");
        }

        // Pending commits persisted before stage ids existed, the target must equal the stage
        private static async Task<PublishResult> PublishLegacy(IFileStorage storage, IOPath stagedPath, IOPath commitPath, DeltaLakePendingCommit pending, CommitBytes? target)
        {
            var stage = await CommitBytes.Read(storage, stagedPath);
            if (target == null)
            {
                if (stage == null)
                {
                    return Failed(pending, "was never published and its staged commit is missing");
                }
                await WriteBytes(storage, commitPath, stage, 0);
                await storage.Rm(stagedPath);
                return new PublishResult(PublishOutcome.Published);
            }
            // The stage is removed right after the copy, the target is the published commit
            if (stage == null)
            {
                return new PublishResult(PublishOutcome.AlreadyPublished);
            }
            if (!target.ContentEquals(stage))
            {
                return Failed(pending, "was written by another writer or is damaged");
            }
            await storage.Rm(stagedPath);
            return new PublishResult(PublishOutcome.AlreadyPublished);
        }

        // Commits, checkpoints and other version files at the version or later are another writer's history
        private static async Task<bool> HistoryFrom(IFileStorage storage, IOPath tablePath, long version)
        {
            var log = await DeltaTransactionReader.ListLog(storage, tablePath);
            return log.Head >= version || log.Checkpoints.Any(x => x.Version >= version) || log.MaxOtherVersion >= version;
        }

        private static PublishResult Failed(DeltaLakePendingCommit pending, string reason)
        {
            return new PublishResult(PublishOutcome.FailedClosed, $"Commit {CommitFileName(pending.Version)} {reason}");
        }

        private enum StageState
        {
            Missing,
            Damaged,
            Valid
        }

        private static async Task<(StageState, CommitBytes?)> ReadStage(IFileStorage storage, IOPath stagedPath, DeltaLakePendingCommit pending, byte[] prefix, CommitBytes? stageBytes)
        {
            var stage = stageBytes ?? await CommitBytes.Read(storage, stagedPath);
            if (stage == null)
            {
                return (StageState.Missing, null);
            }
            if (stage.Length != pending.Length || !stage.StartsWith(prefix))
            {
                return (StageState.Damaged, null);
            }
            return (StageState.Valid, stage);
        }

        // The bytes a commit staged by this sink starts with
        internal static byte[] StagePrefix(string stageId)
        {
            return Encoding.UTF8.GetBytes($"{{\"commitInfo\":{{\"flowtide.stageId\":\"{stageId}\"");
        }

        private static bool FirstLineHasStageId(CommitBytes commit, string stageId)
        {
            var line = commit.FirstLine();
            if (line == null)
            {
                return false;
            }
            try
            {
                var action = JsonSerializer.Deserialize<DeltaAction>(line);
                return action?.CommitInfo?.StageId == stageId;
            }
            catch (JsonException)
            {
                return false;
            }
        }

        internal static CommitBytes Serialize(List<DeltaAction> actions)
        {
            var bytes = new CommitBytes();
            using var stream = bytes.AsAppendStream();
            foreach (var action in actions)
            {
                JsonSerializer.Serialize(stream, action, s_jsonOptions);
                stream.WriteByte((byte)'\n');
            }
            return bytes;
        }

        private static async Task WriteBytes(IFileStorage storage, IOPath path, CommitBytes bytes, int flushAfter)
        {
            using var stream = await storage.OpenWrite(path);
            if (stream == null)
            {
                throw new InvalidOperationException($"Failed to open {path.Full} for writing");
            }
            await bytes.WriteTo(stream, flushAfter);
        }

        private static string CommitFileName(long version)
        {
            return version.ToString("D20") + ".json";
        }
    }
}
