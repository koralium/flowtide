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
using System.Diagnostics;
using System.Reflection;

namespace FlowtideDotNet.Connector.DeltaLake.Internal.Maintenance
{
    /// <summary>
    /// The raw storage implementation that holds a table, known only by its pinned concrete type.
    /// </summary>
    internal enum StoreKind : byte
    {
        Unknown = 0,
        LocalDisk = 1,
        S3 = 2,
        Azure = 3,
        Gcs = 4,
        InMemory = 5
    }

    /// <summary>
    /// How a store delivers a listing, which decides how its deletion evidence is checked.
    /// </summary>
    internal enum ListingDelivery : byte
    {
        // The delivered order is the evidence order; nothing is removed or sorted
        Ordered = 0,
        // A pinned S3 or Azure client, whose recursive listing starts with its own synthetic folder block
        PinnedCloud = 1,
        // One unpaged snapshot without an order, checked through a sorted view
        UnpagedSnapshot = 2
    }

    /// <summary>
    /// What cleanup may rely on for one storage implementation.
    /// </summary>
    internal sealed record StoreDescriptor
    {
        public required StoreKind Kind { get; init; }

        // The concrete type of the pinned Stowage build
        public bool Pinned { get; init; }

        public ListingDelivery Delivery { get; init; }

        // Table paths and targets stay within [A-Za-z0-9._/-], since an object store's client may send them unencoded
        public bool QualifiedDomain { get; init; }

        // A known defect that asserting the listing contract cannot repair
        public bool KnownFailed { get; init; }

        // Eligible only when the user asserts the complete stack's listing contract
        public bool NeedsAssertion { get; init; }

        // A delete addresses exactly the named object
        public bool ExactDeletes { get; init; }

        // Directories can be told apart from files, which orphan sweeping requires
        public bool DirectoryExclusion { get; init; }

        // Why, for logs and configuration errors
        public required string Detail { get; init; }

        // How listings page, for metering; a store not known to page otherwise is estimated as S3
        public ListingPaging Paging => Kind switch
        {
            StoreKind.LocalDisk or StoreKind.InMemory => ListingPaging.LocalDisk,
            StoreKind.Azure when !KnownFailed => ListingPaging.Azure,
            _ => ListingPaging.S3
        };

        /// <summary>
        /// Recognizes the concrete implementation of a raw storage; never throws and makes no request.
        /// </summary>
        public static StoreDescriptor Of(IFileStorage? raw)
        {
            try
            {
                return Classify(raw);
            }
            catch (Exception e)
            {
                return new StoreDescriptor { Kind = StoreKind.Unknown, KnownFailed = true, Detail = $"the storage could not be classified: {e.GetType().Name}" };
            }
        }

        private static StoreDescriptor Classify(IFileStorage? raw)
        {
            if (raw == null)
            {
                return new StoreDescriptor { Kind = StoreKind.Unknown, KnownFailed = true, Detail = "no storage is configured" };
            }
            var type = raw.GetType();
            var name = type.FullName;
            var pinned = PinnedStowage.Assembly != null && type.Assembly == PinnedStowage.Assembly;
            // Every build this connector knows reads one page and has no Stat
            if (name == PinnedStowage.GcsType && type.Assembly == typeof(IFileStorage).Assembly)
            {
                return Gcs(pinned);
            }
            if (!pinned)
            {
                return new StoreDescriptor { Kind = StoreKind.Unknown, QualifiedDomain = true, NeedsAssertion = true, ExactDeletes = true, Detail = $"{type.Name} is not a pinned Stowage {PinnedStowage.Version} implementation" };
            }
            switch (name)
            {
                case PinnedStowage.LocalDiskType:
                    return new StoreDescriptor { Kind = StoreKind.LocalDisk, Pinned = true, Delivery = ListingDelivery.UnpagedSnapshot, ExactDeletes = true, DirectoryExclusion = true, Detail = "local disk" };
                case PinnedStowage.S3Type:
                    return new StoreDescriptor { Kind = StoreKind.S3, Pinned = true, Delivery = ListingDelivery.PinnedCloud, QualifiedDomain = true, ExactDeletes = true, DirectoryExclusion = true, Detail = "Amazon S3" };
                case PinnedStowage.AzureType:
                    if (!PinnedStowage.AzureEndpointHasNoPath(raw))
                    {
                        // A delete drops the endpoint's path segment and its not-found is swallowed
                        return new StoreDescriptor { Kind = StoreKind.Azure, Pinned = true, Delivery = ListingDelivery.PinnedCloud, QualifiedDomain = true, KnownFailed = true, DirectoryExclusion = true, Detail = "the Azure endpoint carries a path segment, which deletes drop" };
                    }
                    return new StoreDescriptor { Kind = StoreKind.Azure, Pinned = true, Delivery = ListingDelivery.PinnedCloud, QualifiedDomain = true, ExactDeletes = true, DirectoryExclusion = true, Detail = "Azure Blob Storage" };
                case PinnedStowage.InMemoryType:
                    return new StoreDescriptor { Kind = StoreKind.InMemory, Pinned = true, Delivery = ListingDelivery.UnpagedSnapshot, QualifiedDomain = true, NeedsAssertion = true, ExactDeletes = true, DirectoryExclusion = true, Detail = "in-memory storage" };
                default:
                    return new StoreDescriptor { Kind = StoreKind.Unknown, Pinned = true, QualifiedDomain = true, NeedsAssertion = true, ExactDeletes = true, Detail = $"{type.Name} is not a qualified Stowage implementation" };
            }
        }

        /// <summary>
        /// Google Cloud Storage of the loaded Stowage build, whose deletes are exact only in the pinned build.
        /// </summary>
        internal static StoreDescriptor Gcs(bool pinned)
        {
            return new StoreDescriptor { Kind = StoreKind.Gcs, Pinned = pinned, QualifiedDomain = true, KnownFailed = true, ExactDeletes = pinned, Detail = "Google Cloud Storage lists only its first page and has no Stat" };
        }
    }

    /// <summary>
    /// The Stowage build whose clients were qualified, and the facts read from it.
    /// </summary>
    internal static class PinnedStowage
    {
        public const string Version = "2.0.1";
        public const string InformationalVersion = "2.0.1+92612a6d12c0f78c93d38b65f3bb280bc07f6f1a";
        public const string LocalDiskType = "Stowage.Impl.LocalDiskFileStorage";
        public const string S3Type = "Stowage.Impl.Amazon.AwsS3FileStorage";
        public const string AzureType = "Stowage.Impl.Microsoft.AzureBlobFileStorage";
        public const string GcsType = "Stowage.Impl.Google.GoogleCloudStorage";
        public const string InMemoryType = "Stowage.Impl.InMemoryFileStorage";
        private const string PublicKeyToken = "0544266807c76150";

        // The loaded Stowage assembly when it is the pinned build, null otherwise
        public static readonly Assembly? Assembly = Identify(typeof(IFileStorage).Assembly);

        private static readonly FieldInfo? s_http = Assembly?.GetType("Stowage.PolyfilledHttpFileStorage")?.GetField("_http", BindingFlags.Instance | BindingFlags.NonPublic);

        private static Assembly? Identify(Assembly assembly)
        {
            try
            {
                var name = assembly.GetName();
                var token = Convert.ToHexString(name.GetPublicKeyToken() ?? Array.Empty<byte>());
                var version = assembly.GetCustomAttribute<AssemblyInformationalVersionAttribute>()?.InformationalVersion;
                return name.Name == "Stowage" && token.Equals(PublicKeyToken, StringComparison.OrdinalIgnoreCase) && version == InformationalVersion ? assembly : null;
            }
            catch
            {
                return null;
            }
        }

        /// <summary>
        /// Whether a pinned Azure client's endpoint is a bare authority; false when it cannot be read.
        /// </summary>
        public static bool AzureEndpointHasNoPath(IFileStorage azure)
        {
            try
            {
                return s_http?.GetValue(azure) is HttpClient http && http.BaseAddress?.AbsolutePath == "/";
            }
            catch
            {
                return false;
            }
        }
    }

    /// <summary>
    /// The cleanup contract of one table on its raw storage: whether listings may serve as deletion evidence and which names may be deleted.
    /// </summary>
    internal sealed class CleanupStoreContract
    {
        public const string QualifiedCharacters = "[A-Za-z0-9._/-]";

        private CleanupStoreContract(IFileStorage? raw, StoreDescriptor store, IOPath table, bool asserted, string? container, string? pathProblem)
        {
            Raw = raw;
            Store = store;
            Table = table;
            Asserted = asserted;
            Container = container;
            PathProblem = pathProblem;
        }

        /// <summary>
        /// The storage below every connector-owned wrapper, compared by reference to detect a replaced instance.
        /// </summary>
        public IFileStorage? Raw { get; }

        public StoreDescriptor Store { get; }

        /// <summary>
        /// The table's normalized path, relied on only when <see cref="PathProblem"/> is null.
        /// </summary>
        public IOPath Table { get; }

        /// <summary>
        /// Whether the user asserted the complete stack's listing contract.
        /// </summary>
        public bool Asserted { get; }

        /// <summary>
        /// The bucket or container of a pinned cloud store.
        /// </summary>
        public string? Container { get; }

        /// <summary>
        /// Why the configured table name is not qualified, null when it is.
        /// </summary>
        public string? PathProblem { get; }

        /// <summary>
        /// Whether listings and absence may authorize deletion at all; each listing still passes its own checks.
        /// </summary>
        public bool Eligible => !Store.KnownFailed && PathProblem == null && (!Store.NeedsAssertion || Asserted);

        /// <summary>
        /// Classifies the raw storage below connector-owned wrappers and checks the original table name before normalization; never throws and makes no request.
        /// </summary>
        public static CleanupStoreContract Classify(IFileStorage? storage, string originalTableName, bool assumeCompleteListing)
        {
            IFileStorage? raw;
            try
            {
                raw = storage == null ? null : RawStorage.Unwrap(storage);
            }
            catch
            {
                raw = null;
            }
            return Create(raw, StoreDescriptor.Of(raw), originalTableName, assumeCompleteListing);
        }

        /// <summary>
        /// A contract for a given descriptor, so tests can use descriptors no real store produces.
        /// </summary>
        public static CleanupStoreContract Create(IFileStorage? raw, StoreDescriptor store, string originalTableName, bool assumeCompleteListing)
        {
            originalTableName ??= string.Empty;
            string? container = null;
            var problem = CheckSegments(originalTableName, InDomain(store));
            if (problem == null && store.Delivery == ListingDelivery.PinnedCloud)
            {
                var slash = originalTableName.IndexOf('/');
                if (slash < 0)
                {
                    problem = "names a bucket or container without a table path inside it";
                }
                else
                {
                    container = originalTableName.Substring(0, slash);
                }
            }
            var table = new IOPath(originalTableName);
            // Normalization rewrites only empty and '..' segments, which a qualified name has none of
            Debug.Assert(problem != null || table.Full == IOPath.PathSeparatorString + originalTableName);
            return new CleanupStoreContract(raw, store, table, assumeCompleteListing, container, problem);
        }

        /// <summary>
        /// The exact full path of a table-relative deletion target, when the whole key is in this store's transmission domain; native name rules are not checked here.
        /// </summary>
        public bool TryQualifyTarget(string tableRelative, out IOPath target)
        {
            target = IOPath.Root;
            // An unknown stack's deletes are exact only when the user asserted it
            if (PathProblem != null || !Store.ExactDeletes || (Store.Kind == StoreKind.Unknown && !Asserted) || CheckSegments(tableRelative, InDomain(Store)) != null)
            {
                return false;
            }
            // The whole key, whose table path passed the same domain at classification
            var full = Table.Full + IOPath.PathSeparatorString + tableRelative;
            // Azure drops a segment's trailing dot, and an unknown stack may be Azure underneath
            if ((Store.Kind is StoreKind.Azure or StoreKind.Unknown) && HasTrailingDotSegment(full))
            {
                return false;
            }
            target = new IOPath(full);
            Debug.Assert(target.Full == full);
            return true;
        }

        /// <summary>
        /// Whether a listing prefix within the bucket or container is qualified: a qualified name plus the single terminal slash a listing adds.
        /// </summary>
        public bool QualifiesPrefix(string containerRelativePrefix)
        {
            if (string.IsNullOrEmpty(containerRelativePrefix) || containerRelativePrefix[^1] != '/')
            {
                return false;
            }
            return CheckSegments(containerRelativePrefix.Substring(0, containerRelativePrefix.Length - 1), InDomain(Store)) == null;
        }

        // Every store but local disk deletes by object key, so its table path and targets stay within the qualified characters
        private static bool InDomain(StoreDescriptor store) => store.QualifiedDomain || store.Kind != StoreKind.LocalDisk;

        /// <summary>
        /// Whether a slash-separated name has a segment ending in a dot.
        /// </summary>
        internal static bool HasTrailingDotSegment(string path)
        {
            for (int i = 0; i < path.Length; i++)
            {
                if (path[i] == '.' && (i + 1 == path.Length || path[i + 1] == '/'))
                {
                    return true;
                }
            }
            return false;
        }

        /// <summary>
        /// Why a slash-separated name is not qualified, null when it is: nonempty segments, no '.' or '..', no backslash or control character, and the qualified characters when required.
        /// </summary>
        internal static string? CheckSegments(string? path, bool qualifiedDomain)
        {
            if (string.IsNullOrEmpty(path))
            {
                return "is empty";
            }
            var segmentStart = 0;
            for (int i = 0; i <= path.Length; i++)
            {
                if (i == path.Length || path[i] == '/')
                {
                    var length = i - segmentStart;
                    if (length == 0)
                    {
                        return "has an empty segment";
                    }
                    if (path[segmentStart] == '.' && (length == 1 || (length == 2 && path[segmentStart + 1] == '.')))
                    {
                        return "has a '.' or '..' segment";
                    }
                    segmentStart = i + 1;
                    continue;
                }
                var c = path[i];
                if (c == '\\')
                {
                    return "has a backslash";
                }
                if (char.IsControl(c))
                {
                    return "has a control character";
                }
                if (qualifiedDomain && !(char.IsAsciiLetterOrDigit(c) || c == '.' || c == '_' || c == '-'))
                {
                    return $"has a character outside {QualifiedCharacters}";
                }
            }
            return null;
        }
    }
}
