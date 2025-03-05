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
using FlowtideDotNet.Connector.DeltaLake.Internal.Delta.Schema;
using FlowtideDotNet.Connector.DeltaLake.Internal.Delta.Schema.Converters;
using FlowtideDotNet.Connector.DeltaLake.Internal.Delta.Schema.Types;
using FlowtideDotNet.Connector.DeltaLake.Internal.Delta.Stats;
using FlowtideDotNet.Connector.DeltaLake.Internal.Delta.Utils;
using Stowage;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Text.Json;
using System.Threading.Tasks;

namespace FlowtideDotNet.Connector.DeltaLake.Internal.Delta
{
    internal static class DeltaTransactionReader
    {
        public const string DeltaLogDirName = "_delta_log/";

        public static async Task<DeltaTable?> ReadTable(IFileStorage storage, IOPath tableName, long maxVersion = long.MaxValue)
        {
            var logs = await ReadTransactionLog(storage, tableName);

            if (logs.Count == 0)
            {
                return null;
            }

            List<DeltaBaseAction> actions = new List<DeltaBaseAction>();

            // Check if there is a checkpoint file that is less than or equal to the max version
            var checkpoint = logs.FirstOrDefault(l => l.IsCheckpoint && l.Version <= maxVersion);

            DeltaMetadataAction? metadata = null;
            DeltaProtocolAction? protocol = null;
            Dictionary<DeltaFileKey, DeltaAddAction> addFiles = new Dictionary<DeltaFileKey, DeltaAddAction>();

            long startVersion = 0;

            // Wait with parquet checkpoint reading
            //if (checkpoint != null)
            //{
            //    startVersion = checkpoint.Version;
            //    await ReadCheckpoint(storage, checkpoint, actions, addFiles);
            //}

            

            long currentVersion = startVersion;

            foreach (var log in logs)
            {
                if (log.Version < startVersion)
                {
                    continue;
                }

                if (log.Version > maxVersion)
                {
                    break;
                }

                if (log.Version > currentVersion)
                {
                    currentVersion = log.Version;
                }

                if (log.IsCheckpoint)
                {
                    continue;
                }

                if (log.IsCompacted)
                {
                    continue;
                }

                if (log.IsJson)
                {
                    // Read the json file
                    using var logData = await storage.OpenRead(log.IOEntry.Path);

                    if (logData == null)
                    {
                        throw new Exception("Failed to open log file");
                    }

                    using var textReader = new StreamReader(logData);

                    var line = await textReader.ReadLineAsync();

                    while (line != null)
                    {
                        var action = JsonSerializer.Deserialize<DeltaAction>(line);
                        if (action == null)
                        {
                            throw new Exception("Failed to deserialize action");
                        }

                        if (action.Add != null)
                        {
                            addFiles.Add(action.Add.GetKey(), action.Add);
                        }
                        if (action.Remove != null)
                        {
                            addFiles.Remove(action.Remove.GetKey());
                        }
                        if (action.MetaData != null)
                        {
                            metadata = action.MetaData;
                        }
                        if (action.Protocol != null)
                        {
                            protocol = action.Protocol;
                        }

                        var genericAction = ToGenericAction(action);

                        if (genericAction != null)
                        {
                            actions.Add(genericAction);
                        }

                        if (textReader.EndOfStream)
                        {
                            break;
                        }
                        line = await textReader.ReadLineAsync();
                    }
                }
            }

            if (metadata == null)
            {
                throw new Exception("No metadata found for the table");

            }

            var schemaJsonOptions = new JsonSerializerOptions();
            schemaJsonOptions.Converters.Add(new TypeConverter());
            var schema = JsonSerializer.Deserialize<SchemaBaseType>(metadata.SchemaString!, schemaJsonOptions);

            if (schema!.Type != SchemaType.Struct)
            {
                throw new Exception("Schema type must be struct");
            }

            var structSchema = (schema as StructType)!;

            JsonSerializerOptions statisticsJsonOptions = new JsonSerializerOptions();
            statisticsJsonOptions.Converters.Add(new DeltaStatisticsConverter(structSchema));

            List<DeltaFile> deltaFiles = new List<DeltaFile>();
            foreach(var addFile in addFiles)
            {
                var stats = JsonSerializer.Deserialize<DeltaStatistics>(addFile.Value.Statistics!, statisticsJsonOptions);
                deltaFiles.Add(new DeltaFile(addFile.Value, stats!));
            }


            return new DeltaTable(metadata!, protocol!, addFiles.Values.ToList(), structSchema, deltaFiles, currentVersion);
        }

        public static async Task<DeltaCommit?> ReadVersionCommit(IFileStorage storage, IOPath table, long version)
        {
            var fileName = $"{version.ToString("D20")}.json";

            var deltaLogDir = table.Combine(DeltaLogDirName);
            if (!await storage.Exists(new IOPath(deltaLogDir, fileName)))
            {
                return null;
            }

            using var commitData = await storage.OpenRead(new IOPath(deltaLogDir, fileName));

            using var textReader = new StreamReader(commitData!);

            var line = await textReader.ReadLineAsync();

            List<DeltaAddAction> addedFiles = new List<DeltaAddAction>();
            List<DeltaRemoveFileAction> removedFiles = new List<DeltaRemoveFileAction>();
            List<DeltaCdcAction> cdcActions = new List<DeltaCdcAction>();

            DeltaMetadataAction? metadata = null;

            while (line != null)
            {

                var action = JsonSerializer.Deserialize<DeltaAction>(line);
                if (action == null)
                {
                    throw new Exception("Failed to deserialize action");
                }

                if (action.Add != null)
                {
                    addedFiles.Add(action.Add);
                }
                if (action.Remove != null)
                {
                    removedFiles.Add(action.Remove);
                }
                if (action.MetaData != null)
                {
                    metadata = action.MetaData;
                }
                if (action.Cdc != null)
                {
                    cdcActions.Add(action.Cdc);
                }

                if (textReader.EndOfStream)
                {
                    break;
                }
                line = await textReader.ReadLineAsync();
            }

            return new DeltaCommit(addedFiles, removedFiles, cdcActions, metadata);
        }

        private static DeltaBaseAction? ToGenericAction(DeltaAction action)
        {
            if (action.Add != null)
            {
                return action.Add;
            }
            else if (action.MetaData != null)
            {
                return action.MetaData;
            }
            else if (action.Protocol != null)
            {
                return action.Protocol;
            }
            else if (action.CommitInfo != null)
            {
                return action.CommitInfo;
            }

            return null;
        }

        public static async Task<IReadOnlyList<LogTransactionFile>> ReadTransactionLog(IFileStorage storage, IOPath tableName)
        {
            // Read the transaction log
            var files = await storage.Ls(tableName.Combine(DeltaLogDirName));

            List<LogTransactionFile> logs = new List<LogTransactionFile>();
            foreach (var file in files)
            {
                if (file.Name.EndsWith(".crc"))
                {
                    continue;
                }

                var dotIndex = file.Name.LastIndexOf('.');

                long version = 0;
                if (dotIndex >= 0)
                {
                    long.TryParse(file.Name.Substring(0, dotIndex), out version);
                }
                else
                {

                }

                var isJson = file.Name.EndsWith(".json");
                var isCheckpoint = file.Name.EndsWith(".checkpoint.parquet");
                var isCompacted = file.Name.Contains(".compacted.");

                logs.Add(new LogTransactionFile(file.Name, isCheckpoint, isJson, version, file, isCompacted));
            }

            return logs;
        }
    }
}
