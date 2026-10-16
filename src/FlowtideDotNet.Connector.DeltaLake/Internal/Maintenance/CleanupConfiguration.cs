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

namespace FlowtideDotNet.Connector.DeltaLake.Internal.Maintenance
{
    /// <summary>
    /// The cleanup features a user opted into; null turns one off.
    /// </summary>
    internal readonly record struct CleanupOptIns(TimeSpan? Retention, TimeSpan? OrphanSweepInterval)
    {
        public static CleanupOptIns Off => default;

        public bool Any => Retention != null || OrphanSweepInterval != null;
    }

    /// <summary>
    /// Cleanup settings validated at sink construction and kept for the sink's lifetime, so restore, commit and the worker never see a configuration error.
    /// </summary>
    internal sealed class CleanupConfiguration
    {
        internal const string RetentionOption = "HistoryRetention";
        internal const string OrphanOption = "OrphanFileSweepInterval";
        internal const string AssertionOption = nameof(DeltaLakeOptions.AssumeCompleteStorageListing);
        internal const string StorageOption = nameof(DeltaLakeOptions.StorageLocation);

        // Reads the sink's storage now
        private readonly Func<IFileStorage?> _current;

        private CleanupConfiguration(CleanupOptIns optIns, CleanupStoreContract store, Func<IFileStorage?> current)
        {
            OptIns = optIns;
            Store = store;
            _current = current;
        }

        public CleanupOptIns OptIns { get; }

        public CleanupStoreContract Store { get; }

        /// <summary>
        /// Validates opt-in configuration for a table; throws only for an invalid opt-in, never with both opt-ins off, and makes no storage request.
        /// </summary>
        public static CleanupConfiguration Validate(IFileStorage? storage, string originalTableName, bool assumeCompleteListing, CleanupOptIns optIns)
        {
            return Validate(CleanupStoreContract.Classify(storage, originalTableName, assumeCompleteListing), optIns);
        }

        /// <summary>
        /// Validates opt-in configuration for a table whose storage the sink holds in a setting; the setting is read again whenever cleanup checks for a replaced instance.
        /// </summary>
        public static CleanupConfiguration Validate(Func<IFileStorage?> current, string originalTableName, bool assumeCompleteListing, CleanupOptIns optIns)
        {
            return Validate(CleanupStoreContract.Classify(Read(current), originalTableName, assumeCompleteListing), optIns, current);
        }

        /// <summary>
        /// Validates opt-in configuration against a classified store.
        /// </summary>
        public static CleanupConfiguration Validate(CleanupStoreContract store, CleanupOptIns optIns)
        {
            var raw = store.Raw;
            return Validate(store, optIns, () => raw);
        }

        private static CleanupConfiguration Validate(CleanupStoreContract store, CleanupOptIns optIns, Func<IFileStorage?> current)
        {
            if (!optIns.Any)
            {
                return new CleanupConfiguration(optIns, store, current);
            }
            if (optIns.Retention is not { } retention || !IsWholeHoursOfAtLeastADay(retention))
            {
                var problem = optIns.Retention == null ? "is required by orphan sweeping" : $"is {optIns.Retention}";
                throw new ArgumentException($"{RetentionOption} {problem}; use whole hours of at least one day, or disable history retention and orphan sweeping.", RetentionOption);
            }
            if (optIns.OrphanSweepInterval is { } interval && interval.Ticks < TimeSpan.TicksPerDay)
            {
                throw new ArgumentException($"{OrphanOption} is {interval}; use at least one day (seven or more is recommended), or disable orphan sweeping.", OrphanOption);
            }
            var descriptor = store.Store;
            if (descriptor.KnownFailed)
            {
                throw new ArgumentException($"History retention and orphan sweeping need a storage whose listings and deletes are qualified, but {descriptor.Detail}. Use Azure Blob Storage, Amazon S3 or local disk, or disable both; {AssertionOption} cannot repair this storage.", StorageOption);
            }
            if (descriptor.NeedsAssertion && !store.Asserted)
            {
                throw new ArgumentException($"History retention and orphan sweeping are not qualified for this storage: {descriptor.Detail}. Verify that the whole storage stack lists completely and in order, reports an object absent only when it does not exist and deletes exactly the named object, then set {AssertionOption}, use a known storage implementation, or disable both.", AssertionOption);
            }
            if (store.PathProblem != null)
            {
                var characters = descriptor.QualifiedDomain ? $" of {CleanupStoreContract.QualifiedCharacters}" : "";
                throw new ArgumentException($"History retention and orphan sweeping need a qualified table path, but the table name {store.PathProblem}. Use nonempty slash-separated segments{characters} without '.' or '..' below the bucket or container, or disable both.", StorageOption);
            }
            if (optIns.OrphanSweepInterval != null && !descriptor.DirectoryExclusion)
            {
                throw new ArgumentException($"Orphan sweeping needs a storage whose directories are told apart from files, which is not qualified for {descriptor.Detail}. Disable orphan sweeping or use a qualified storage; history retention alone is assessed separately.", OrphanOption);
            }
            return new CleanupConfiguration(optIns, store, current);
        }

        // A setting that cannot be read holds no storage
        private static IFileStorage? Read(Func<IFileStorage?> current)
        {
            try
            {
                return current();
            }
            catch
            {
                return null;
            }
        }

        // Integer tick arithmetic, so no rounding admits a fraction of an hour
        private static bool IsWholeHoursOfAtLeastADay(TimeSpan value)
        {
            return value.Ticks >= TimeSpan.TicksPerDay && value.Ticks % TimeSpan.TicksPerHour == 0;
        }

        /// <summary>
        /// Whether the storage below connector-owned wrappers is no longer the instance classified at construction; never throws.
        /// </summary>
        public bool StorageChanged(IFileStorage? current)
        {
            try
            {
                return !ReferenceEquals(current == null ? null : RawStorage.Unwrap(current), Store.Raw);
            }
            catch
            {
                return true;
            }
        }

        /// <summary>
        /// Whether the storage the sink holds now is no longer the instance classified at construction; never throws.
        /// </summary>
        public bool StorageReplaced()
        {
            try
            {
                return StorageChanged(_current());
            }
            catch
            {
                return true;
            }
        }
    }
}
