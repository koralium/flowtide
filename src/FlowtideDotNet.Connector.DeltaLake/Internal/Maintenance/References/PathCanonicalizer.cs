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
using Stowage.Impl;
using System.Buffers;
using System.Runtime.InteropServices;
using System.Text;

namespace FlowtideDotNet.Connector.DeltaLake.Internal.Maintenance.References
{
    /// <summary>
    /// Whether a path protects what it names or offers it for deletion.
    /// </summary>
    internal enum PathRole : byte
    {
        Protection = 0,
        Candidate = 1
    }

    /// <summary>
    /// How a store's physical names compare.
    /// </summary>
    internal enum PathIdentity : byte
    {
        // Ordinal UTF-8 object keys, case preserved
        Cloud = 0,
        // Windows local disk: existing paths in long form, ASCII names, case-folded keys
        WindowsNative = 1,
        // Other local disk: ASCII names without alias spellings, case-folded keys
        PortableNative = 2,
        // An asserted store of unknown semantics: ASCII names without alias spellings, case-folded keys
        Folded = 3,
        // No relative identity: protection declines and candidates are kept
        Unsupported = 4
    }

    /// <summary>
    /// The keys one canonicalizer makes: its identity on one classified table store, whose keys compare only with each other.
    /// </summary>
    internal readonly record struct PathKeyDomain(PathIdentity Identity, CleanupStoreContract Store);

    /// <summary>
    /// Why a path has no identity or is no target; protection declines on any, a candidate is kept and counted.
    /// </summary>
    internal enum PathRejection : byte
    {
        None = 0,
        // The action names no path, or its line or record cannot be read
        Malformed = 1,
        // A malformed percent escape, or text that is not valid UTF-8
        Encoding = 2,
        // An unescaped '#' or '?', a backslash or a control character
        Syntax = 3,
        // An empty, '.' or '..' segment
        Traversal = 4,
        // Over 64 KiB of decoded UTF-8
        TooLong = 5,
        // An absolute reference, which protects but is never a target
        Absolute = 6,
        // An absolute reference not placed inside the table
        Outside = 7,
        // A deletion vector storage type other than u, p or i
        UnknownVectorType = 8,
        // A deletion vector whose file cannot be named
        InvalidVector = 9,
        // Outside the store's exact transmission domain
        Unqualified = 10,
        // A native or folded name outside printable ASCII
        NonAscii = 11,
        // A trailing dot or space, a colon, a wildcard, a reserved device name or short-name syntax
        NameSyntax = 12,
        // A device path or a namespace the identity does not support
        Namespace = 13,
        // A reparse point or symbolic link on the path
        Reparse = 14,
        // The path names a directory
        Directory = 15,
        // Resolution was refused
        AccessDenied = 16,
        // Resolution failed otherwise
        Unresolved = 17
    }

    /// <summary>
    /// A path's comparison key and, for a candidate, its exact deletion path, or why it has neither.
    /// </summary>
    internal readonly struct PathKey
    {
        private PathKey(string? key, string? deletionPath, PathRejection rejection)
        {
            Key = key;
            DeletionPath = deletionPath;
            Rejection = rejection;
        }

        // Ordinal; case preserved on cloud stores and folded otherwise
        public string? Key { get; }

        // A candidate's exact full path within the store, kept apart from its key; DeletionExecutor.TargetOf makes it a target
        public string? DeletionPath { get; }

        public PathRejection Rejection { get; }

        public bool Accepted => Rejection == PathRejection.None;

        public static PathKey Protect(string key) => new PathKey(key, null, PathRejection.None);

        public static PathKey Target(string key, string deletionPath) => new PathKey(key, deletionPath, PathRejection.None);

        public static PathKey Reject(PathRejection rejection) => new PathKey(null, null, rejection);
    }

    /// <summary>
    /// Comparison keys and exact deletion paths for one table, by its store's provider identity; used from one thread at a time.
    /// </summary>
    internal sealed class PathCanonicalizer
    {
        // Decoded UTF-8 of one selected maintenance path
        public const int MaxPathBytes = 64 << 10;

        // The longest Windows extended path
        internal const int MaxWindowsPath = 32767;

        private const string ExtendedPrefix = @"\\?\";

        private static readonly UTF8Encoding s_strict = new UTF8Encoding(false, true);

        private readonly CleanupStoreContract _store;
        private readonly IFileStorage? _meter;
        private readonly NativeNames _native;
        // The table's native folder, extended on Windows; resolved once to its long form
        private readonly string? _root;
        private string? _longRoot;
        private PathRejection _rootProblem;
        private bool _rootChecked;

        private PathCanonicalizer(CleanupStoreContract store, PathIdentity identity, IFileStorage? meter, NativeNames native, string? root, PathRejection rootProblem)
        {
            _store = store;
            Identity = identity;
            _meter = meter;
            _native = native;
            _root = root;
            _rootProblem = rootProblem;
        }

        public PathIdentity Identity { get; }

        // The store by reference: a contract classified again is another domain
        public PathKeyDomain Domain => new PathKeyDomain(Identity, _store);

        // Native path resolutions made, each one request
        public long NativeResolutions { get; private set; }

        // Resolutions not yet charged
        public long Owed { get; private set; }

        /// <summary>
        /// The identity of a table on its classified store; never throws and makes no request.
        /// </summary>
        public static PathCanonicalizer Create(CleanupStoreContract store, IFileStorage? meter = null, NativeNames? native = null, bool portableForTests = false)
        {
            switch (store.Store.Kind)
            {
                case StoreKind.S3:
                case StoreKind.Azure:
                case StoreKind.Gcs:
                    return new PathCanonicalizer(store, PathIdentity.Cloud, meter, native ?? NativeNames.Portable, null, PathRejection.None);
                case StoreKind.LocalDisk:
                    return CreateNative(store, meter, native, portableForTests);
                default:
                    return new PathCanonicalizer(store, PathIdentity.Folded, meter, native ?? NativeNames.Portable, null, PathRejection.None);
            }
        }

        private static PathCanonicalizer CreateNative(CleanupStoreContract store, IFileStorage? meter, NativeNames? native, bool portableForTests)
        {
            string? root = null;
            try
            {
                root = (store.Raw as ILocalDiskFileStorage)?.ToNativeLocalPath(store.Table);
            }
            catch
            {
                root = null;
            }
            var windows = OperatingSystem.IsWindows() && !portableForTests;
            native ??= windows ? new WindowsNativeNames() : NativeNames.Portable;
            if (root == null)
            {
                return new PathCanonicalizer(store, PathIdentity.Unsupported, meter, native, null, PathRejection.Namespace);
            }
            if (windows)
            {
                var problem = ExtendWindowsRoot(root, out var extended);
                return new PathCanonicalizer(store, problem == PathRejection.None ? PathIdentity.WindowsNative : PathIdentity.Unsupported, meter, native, extended, problem);
            }
            root = Path.TrimEndingDirectorySeparator(root);
            var qualified = Path.IsPathFullyQualified(root);
            return new PathCanonicalizer(store, qualified ? PathIdentity.PortableNative : PathIdentity.Unsupported, meter, native, root, qualified ? PathRejection.None : PathRejection.Namespace);
        }

        /// <summary>
        /// The key of a reference read from the log: a URI decoded exactly once, or a uuid vector's plain name; an absolute reference only protects.
        /// </summary>
        public PathKey Reference(in PathReference reference, PathRole role)
        {
            if (reference.Rejection != PathRejection.None)
            {
                return PathKey.Reject(reference.Rejection);
            }
            var value = reference.Value!;
            if (!reference.IsUri)
            {
                return Relative(value, role);
            }
            if (value.Length == 0)
            {
                return PathKey.Reject(PathRejection.Traversal);
            }
            var syntax = CheckUri(value);
            if (syntax != PathRejection.None)
            {
                return PathKey.Reject(syntax);
            }
            var scheme = SchemeLength(value);
            if (scheme > 0 || value[0] == '/')
            {
                return role == PathRole.Candidate ? PathKey.Reject(PathRejection.Absolute) : Absolute(value, scheme);
            }
            if (reference.AbsoluteOnly)
            {
                return PathKey.Reject(PathRejection.InvalidVector);
            }
            var decoded = DecodeOnce(value, out var problem);
            return decoded == null ? PathKey.Reject(problem) : Relative(decoded, role);
        }

        /// <summary>
        /// The key of a physical table-relative name from a listing or an ownership record, never decoded.
        /// </summary>
        public PathKey Name(string name, PathRole role) => Relative(name, role);

        /// <summary>
        /// Charges the resolutions owed so far, one request per FIFO turn; free when the storage is not metered.
        /// </summary>
        public async ValueTask ChargeOwedAsync(CancellationToken cancellationToken)
        {
            if (_meter == null)
            {
                Owed = 0;
                return;
            }
            while (Owed > 0)
            {
                await MeteredFileStorage.ChargeNativeAsync(_meter, 1, 0, cancellationToken);
                Owed--;
            }
        }

        private void Touch()
        {
            NativeResolutions++;
            Owed++;
        }

        private PathKey Relative(string relative, PathRole role)
        {
            var problem = CheckRelative(relative);
            if (problem != PathRejection.None)
            {
                return PathKey.Reject(problem);
            }
            switch (Identity)
            {
                case PathIdentity.Cloud:
                    return CloudKey(relative, role);
                case PathIdentity.Folded:
                    return FoldedKey(relative, role);
                case PathIdentity.WindowsNative:
                case PathIdentity.PortableNative:
                    return NativeKey(relative, role);
                default:
                    return PathKey.Reject(_rootProblem == PathRejection.None ? PathRejection.Namespace : _rootProblem);
            }
        }

        private PathKey CloudKey(string relative, PathRole role)
        {
            // Azure may drop a segment's trailing dot
            if (_store.Store.Kind == StoreKind.Azure && CleanupStoreContract.HasTrailingDotSegment(relative))
            {
                return PathKey.Reject(PathRejection.NameSyntax);
            }
            return Keyed(relative, relative, role);
        }

        private PathKey FoldedKey(string relative, PathRole role)
        {
            if (!IsPrintableAscii(relative))
            {
                return PathKey.Reject(PathRejection.NonAscii);
            }
            var syntax = CheckAliasSyntax(relative);
            if (syntax != PathRejection.None)
            {
                return PathKey.Reject(syntax);
            }
            return Keyed(AsciiUpper(relative), relative, role);
        }

        private PathKey NativeKey(string relative, PathRole role)
        {
            if (!IsPrintableAscii(relative))
            {
                return PathKey.Reject(PathRejection.NonAscii);
            }
            var windows = Identity == PathIdentity.WindowsNative;
            // Spellings a stack may alias; Windows resolves the short ones below
            var syntax = windows ? CheckWindowsSegments(relative, '/') : CheckAliasSyntax(relative);
            if (syntax != PathRejection.None)
            {
                return PathKey.Reject(syntax);
            }
            var root = CheckRoot();
            if (root != PathRejection.None)
            {
                return PathKey.Reject(root);
            }
            var resolved = windows ? ResolveWindows(relative, out var physical) : ResolvePortable(relative, out physical);
            if (resolved != PathRejection.None)
            {
                return PathKey.Reject(resolved);
            }
            // A short alias can stand for a long name outside the supported domain
            if (!IsPrintableAscii(physical))
            {
                return PathKey.Reject(PathRejection.NonAscii);
            }
            if (windows)
            {
                syntax = CheckWindowsSegments(physical, '/');
                if (syntax != PathRejection.None)
                {
                    return PathKey.Reject(syntax);
                }
            }
            return Keyed(AsciiUpper(physical), physical, role);
        }

        private PathKey Keyed(string key, string relative, PathRole role)
        {
            if (role == PathRole.Protection)
            {
                return PathKey.Protect(key);
            }
            return _store.TryQualifyTarget(relative, out var target) ? PathKey.Target(key, target.Full) : PathKey.Reject(PathRejection.Unqualified);
        }

        private PathKey Absolute(string value, int scheme)
        {
            switch (Identity)
            {
                case PathIdentity.Cloud:
                    return CloudAbsolute(value, scheme);
                case PathIdentity.WindowsNative:
                    return WindowsAbsolute(value, scheme);
                case PathIdentity.PortableNative:
                    return PortableAbsolute(value, scheme);
                default:
                    return PathKey.Reject(PathRejection.Outside);
            }
        }

        // An absolute key inside the table protects the same relative name
        private PathKey CloudAbsolute(string value, int scheme)
        {
            string? container;
            string path;
            if (scheme == 0)
            {
                // Resolved against the bucket or container that holds the table
                container = _store.Container;
                path = value;
            }
            else
            {
                SplitUri(value, scheme, out var authority, out path);
                var name = value.Substring(0, scheme);
                container = null;
                if (authority != null && _store.Store.Kind == StoreKind.S3 && IsAny(name, "s3", "s3a", "s3n"))
                {
                    container = authority;
                }
                else if (authority != null && _store.Store.Kind == StoreKind.Azure && IsAny(name, "abfs", "abfss", "wasb", "wasbs"))
                {
                    var at = authority.IndexOf('@');
                    container = at > 0 ? authority.Substring(0, at) : null;
                }
            }
            if (container == null)
            {
                return PathKey.Reject(PathRejection.Outside);
            }
            var decoded = DecodeOnce(path, out var problem);
            if (decoded == null)
            {
                return PathKey.Reject(problem);
            }
            // The table path starts with its bucket or container
            var full = "/" + container + decoded;
            var prefix = _store.Table.Full + "/";
            if (full.Length <= prefix.Length || !full.StartsWith(prefix, StringComparison.Ordinal))
            {
                return PathKey.Reject(PathRejection.Outside);
            }
            return Relative(full.Substring(prefix.Length), PathRole.Protection);
        }

        // A drive path or local file URI that resolves inside the table protects its long relative name
        private PathKey WindowsAbsolute(string value, int scheme)
        {
            string raw;
            if (scheme == 1)
            {
                raw = value;
            }
            else if (scheme > 1 && IsAny(value.Substring(0, scheme), "file"))
            {
                SplitUri(value, scheme, out var authority, out var path);
                if (!string.IsNullOrEmpty(authority) && !authority.Equals("localhost", StringComparison.OrdinalIgnoreCase))
                {
                    return PathKey.Reject(PathRejection.Namespace);
                }
                if (path.Length < 4 || path[0] != '/')
                {
                    return PathKey.Reject(PathRejection.Outside);
                }
                raw = path.Substring(1);
            }
            else
            {
                return PathKey.Reject(PathRejection.Outside);
            }
            var decoded = DecodeOnce(raw, out var problem);
            if (decoded == null)
            {
                return PathKey.Reject(problem);
            }
            if (decoded.Length < 4 || !char.IsAsciiLetter(decoded[0]) || decoded[1] != ':' || decoded[2] != '/')
            {
                return PathKey.Reject(PathRejection.Outside);
            }
            var body = decoded.Substring(3);
            problem = CheckRelative(body);
            if (problem == PathRejection.None)
            {
                problem = CheckWindowsSegments(body, '/');
            }
            if (problem == PathRejection.None)
            {
                problem = CheckRoot();
            }
            if (problem != PathRejection.None)
            {
                return PathKey.Reject(problem);
            }
            var extended = ExtendedPrefix + decoded.Substring(0, 2) + "\\" + body.Replace('/', '\\');
            if (extended.Length > MaxWindowsPath)
            {
                return PathKey.Reject(PathRejection.TooLong);
            }
            Touch();
            // Nothing above the drive's root is probed
            problem = LongForm(extended, ExtendedPrefix.Length + 2, out var resolved, out _);
            if (problem != PathRejection.None)
            {
                return PathKey.Reject(problem);
            }
            var prefix = _longRoot + "\\";
            if (resolved.Length <= prefix.Length || !resolved.StartsWith(prefix, StringComparison.OrdinalIgnoreCase))
            {
                return PathKey.Reject(PathRejection.Outside);
            }
            return Relative(resolved.Substring(prefix.Length).Replace('\\', '/'), PathRole.Protection);
        }

        // An absolute path or local file URI under the table's folder protects its relative name
        private PathKey PortableAbsolute(string value, int scheme)
        {
            string raw;
            if (scheme == 0)
            {
                raw = value;
            }
            else if (IsAny(value.Substring(0, scheme), "file"))
            {
                SplitUri(value, scheme, out var authority, out raw);
                if (!string.IsNullOrEmpty(authority) && !authority.Equals("localhost", StringComparison.OrdinalIgnoreCase))
                {
                    return PathKey.Reject(PathRejection.Namespace);
                }
            }
            else
            {
                return PathKey.Reject(PathRejection.Outside);
            }
            var decoded = DecodeOnce(raw, out var problem);
            if (decoded == null)
            {
                return PathKey.Reject(problem);
            }
            var prefix = _root + "/";
            if (Path.DirectorySeparatorChar != '/' || decoded.Length <= prefix.Length || !decoded.StartsWith(prefix, StringComparison.Ordinal))
            {
                return PathKey.Reject(PathRejection.Outside);
            }
            return Relative(decoded.Substring(prefix.Length), PathRole.Protection);
        }

        private PathRejection CheckRoot()
        {
            if (_rootChecked)
            {
                return _rootProblem;
            }
            _rootChecked = true;
            if (_rootProblem != PathRejection.None)
            {
                return _rootProblem;
            }
            Touch();
            var status = _native.LongPath(_root!, out var longRoot);
            var attributes = default(FileAttributes);
            if (status == NativeStatus.Found)
            {
                status = _native.Attributes(longRoot, out attributes);
            }
            _rootProblem = status switch
            {
                NativeStatus.Found => (attributes & FileAttributes.Directory) != 0 ? PathRejection.None : PathRejection.Unresolved,
                NativeStatus.AccessDenied => PathRejection.AccessDenied,
                _ => PathRejection.Unresolved
            };
            _longRoot = longRoot;
            return _rootProblem;
        }

        // Every existing component below the root in long form, checked for reparse points and a directory leaf
        private PathRejection ResolveWindows(string relative, out string physical)
        {
            physical = relative;
            var extended = _longRoot + "\\" + relative.Replace('/', '\\');
            if (extended.Length > MaxWindowsPath)
            {
                return PathRejection.TooLong;
            }
            Touch();
            var problem = LongForm(extended, _longRoot!.Length - 1, out var resolved, out var missing);
            if (problem != PathRejection.None)
            {
                return problem;
            }
            if (resolved.Length <= _longRoot.Length + 1 || resolved[_longRoot.Length] != '\\' || !resolved.StartsWith(_longRoot, StringComparison.OrdinalIgnoreCase))
            {
                return PathRejection.Unresolved;
            }
            var parts = resolved.Substring(_longRoot.Length + 1).Split('\\');
            var expected = relative.Split('/').Length;
            if (parts.Length != expected)
            {
                return PathRejection.Unresolved;
            }
            var path = _longRoot;
            for (int i = 0; i < parts.Length - missing; i++)
            {
                path = path + "\\" + parts[i];
                problem = CheckComponent(path, i == parts.Length - 1);
                if (problem != PathRejection.None)
                {
                    return problem;
                }
            }
            physical = string.Join('/', parts);
            return PathRejection.None;
        }

        // Existing components checked for links and a directory leaf; nothing below a missing component exists
        private PathRejection ResolvePortable(string relative, out string physical)
        {
            physical = relative;
            Touch();
            var parts = relative.Split('/');
            var path = _longRoot!;
            for (int i = 0; i < parts.Length; i++)
            {
                path = path + Path.DirectorySeparatorChar + parts[i];
                var leaf = i == parts.Length - 1;
                var status = _native.Attributes(path, out var attributes);
                if (status == NativeStatus.Missing)
                {
                    return PathRejection.None;
                }
                var problem = Classify(status, attributes, leaf);
                if (problem != PathRejection.None)
                {
                    return problem;
                }
                if (!leaf && (attributes & FileAttributes.Directory) == 0)
                {
                    return PathRejection.None;
                }
            }
            return PathRejection.None;
        }

        private PathRejection CheckComponent(string path, bool leaf)
        {
            var status = _native.Attributes(path, out var attributes);
            // It existed a moment ago
            return status == NativeStatus.Missing ? PathRejection.Unresolved : Classify(status, attributes, leaf);
        }

        private static PathRejection Classify(NativeStatus status, FileAttributes attributes, bool leaf)
        {
            return status switch
            {
                NativeStatus.Found when (attributes & FileAttributes.ReparsePoint) != 0 => PathRejection.Reparse,
                NativeStatus.Found when leaf && (attributes & FileAttributes.Directory) != 0 => PathRejection.Directory,
                NativeStatus.Found => PathRejection.None,
                NativeStatus.AccessDenied => PathRejection.AccessDenied,
                _ => PathRejection.Unresolved
            };
        }

        /// <summary>
        /// The long form of the longest existing prefix, with the missing segments appended as given; nothing at or above the floor is probed.
        /// </summary>
        private PathRejection LongForm(string path, int floor, out string resolved, out int missing)
        {
            resolved = path;
            missing = 0;
            var probe = path;
            var tail = "";
            while (true)
            {
                var status = _native.LongPath(probe, out var found);
                if (status == NativeStatus.Found)
                {
                    resolved = found + tail;
                    return PathRejection.None;
                }
                if (status == NativeStatus.AccessDenied)
                {
                    return PathRejection.AccessDenied;
                }
                if (status != NativeStatus.Missing)
                {
                    return PathRejection.Unresolved;
                }
                var cut = probe.LastIndexOf('\\');
                if (cut <= floor)
                {
                    return PathRejection.Unresolved;
                }
                tail = probe.Substring(cut) + tail;
                probe = probe.Substring(0, cut);
                missing++;
            }
        }

        /// <summary>
        /// Why a relative name is unusable, before any identity rule: empty, '.' and '..' segments, backslashes, control characters, unpaired surrogates and length.
        /// </summary>
        internal static PathRejection CheckRelative(string relative)
        {
            if (relative.Length == 0)
            {
                return PathRejection.Traversal;
            }
            long bytes = 0;
            var segmentStart = 0;
            for (int i = 0; i <= relative.Length; i++)
            {
                if (i == relative.Length || relative[i] == '/')
                {
                    var length = i - segmentStart;
                    if (length == 0 || (relative[segmentStart] == '.' && (length == 1 || (length == 2 && relative[segmentStart + 1] == '.'))))
                    {
                        return PathRejection.Traversal;
                    }
                    segmentStart = i + 1;
                    bytes++;
                    continue;
                }
                var c = relative[i];
                if (c == '\\' || char.IsControl(c))
                {
                    return PathRejection.Syntax;
                }
                if (char.IsHighSurrogate(c))
                {
                    if (i + 1 >= relative.Length || !char.IsLowSurrogate(relative[i + 1]))
                    {
                        return PathRejection.Encoding;
                    }
                    bytes += 4;
                    i++;
                    continue;
                }
                if (char.IsLowSurrogate(c))
                {
                    return PathRejection.Encoding;
                }
                bytes += c < 0x80 ? 1 : c < 0x800 ? 2 : 3;
            }
            // The counted separators include one past the end
            return bytes - 1 > MaxPathBytes ? PathRejection.TooLong : PathRejection.None;
        }

        /// <summary>
        /// Why a Windows path's segments are unusable: trailing dots or spaces, colons, wildcards, invalid characters and reserved device names.
        /// </summary>
        internal static PathRejection CheckWindowsSegments(string path, char separator)
        {
            var start = 0;
            for (int i = 0; i <= path.Length; i++)
            {
                if (i < path.Length && path[i] != separator)
                {
                    continue;
                }
                var problem = CheckWindowsSegment(path.AsSpan(start, i - start));
                if (problem != PathRejection.None)
                {
                    return problem;
                }
                start = i + 1;
            }
            return PathRejection.None;
        }

        /// <summary>
        /// Why a slash-separated name may alias another where its lookups are not resolved: a segment ending in a dot or space, stream syntax or short-name syntax.
        /// </summary>
        internal static PathRejection CheckAliasSyntax(string relative)
        {
            for (int i = 0; i < relative.Length; i++)
            {
                var c = relative[i];
                // ':' opens a stream and '~' marks a generated short name
                if (c is ':' or '~')
                {
                    return PathRejection.NameSyntax;
                }
                // Win32 and Azure drop a segment's trailing dot, Win32 its trailing space
                if ((c is '.' or ' ') && (i + 1 == relative.Length || relative[i + 1] == '/'))
                {
                    return PathRejection.NameSyntax;
                }
            }
            return PathRejection.None;
        }

        private static PathRejection CheckWindowsSegment(ReadOnlySpan<char> segment)
        {
            if (segment.Length == 0)
            {
                return PathRejection.Traversal;
            }
            // Win32 drops a trailing dot or space
            if (segment[^1] == '.' || segment[^1] == ' ')
            {
                return PathRejection.NameSyntax;
            }
            foreach (var c in segment)
            {
                if (c < 0x20)
                {
                    return PathRejection.Syntax;
                }
                // ':' opens an alternate data stream; '?' and '*' match other names
                if (c is '<' or '>' or ':' or '"' or '|' or '?' or '*' or '\\' or '/')
                {
                    return PathRejection.NameSyntax;
                }
            }
            var dot = segment.IndexOf('.');
            var stem = (dot < 0 ? segment : segment.Slice(0, dot)).TrimEnd(' ');
            return IsDevice(stem) ? PathRejection.NameSyntax : PathRejection.None;
        }

        private static bool IsDevice(ReadOnlySpan<char> stem)
        {
            if (stem.Length == 3)
            {
                return stem.Equals("CON", StringComparison.OrdinalIgnoreCase) || stem.Equals("PRN", StringComparison.OrdinalIgnoreCase)
                    || stem.Equals("AUX", StringComparison.OrdinalIgnoreCase) || stem.Equals("NUL", StringComparison.OrdinalIgnoreCase);
            }
            if (stem.Length == 4 && (stem.StartsWith("COM", StringComparison.OrdinalIgnoreCase) || stem.StartsWith("LPT", StringComparison.OrdinalIgnoreCase)))
            {
                return char.IsAsciiDigit(stem[3]) || stem[3] is '\u00B9' or '\u00B2' or '\u00B3';
            }
            return stem.Equals("CONIN$", StringComparison.OrdinalIgnoreCase) || stem.Equals("CONOUT$", StringComparison.OrdinalIgnoreCase)
                || stem.Equals("CLOCK$", StringComparison.OrdinalIgnoreCase);
        }

        // A drive or UNC folder as an extended path; device paths and other namespaces are refused
        private static PathRejection ExtendWindowsRoot(string root, out string extended)
        {
            extended = "";
            root = root.Replace('/', '\\');
            if (root.StartsWith(@"\\?\", StringComparison.Ordinal) || root.StartsWith(@"\\.\", StringComparison.Ordinal) || root.StartsWith(@"\??\", StringComparison.Ordinal))
            {
                return PathRejection.Namespace;
            }
            string prefix;
            string body;
            if (root.Length > 3 && char.IsAsciiLetter(root[0]) && root[1] == ':' && root[2] == '\\')
            {
                prefix = ExtendedPrefix + root.Substring(0, 3);
                body = root.Substring(3);
            }
            else if (root.Length > 2 && root[0] == '\\' && root[1] == '\\')
            {
                prefix = ExtendedPrefix + @"UNC\";
                body = root.Substring(2);
            }
            else
            {
                return PathRejection.Namespace;
            }
            body = body.TrimEnd('\\');
            var problem = CheckWindowsSegments(body, '\\');
            if (problem != PathRejection.None)
            {
                return problem;
            }
            extended = prefix + body;
            return PathRejection.None;
        }

        /// <summary>
        /// Why a log URI is unusable before decoding: an unescaped '#', '?', backslash or control character, or a malformed percent escape.
        /// </summary>
        internal static PathRejection CheckUri(string value)
        {
            for (int i = 0; i < value.Length; i++)
            {
                var c = value[i];
                if (c == '%')
                {
                    if (i + 2 >= value.Length || !char.IsAsciiHexDigit(value[i + 1]) || !char.IsAsciiHexDigit(value[i + 2]))
                    {
                        return PathRejection.Encoding;
                    }
                    i += 2;
                }
                else if (c is '#' or '?' or '\\' || char.IsControl(c))
                {
                    return PathRejection.Syntax;
                }
            }
            return PathRejection.None;
        }

        /// <summary>
        /// Percent-decodes once into strict UTF-8; '+' stays '+', and a result past the path limit or invalid UTF-8 is refused.
        /// </summary>
        internal static string? DecodeOnce(string value, out PathRejection problem)
        {
            problem = PathRejection.None;
            if (value.IndexOf('%') < 0)
            {
                return value;
            }
            int rawBytes;
            try
            {
                rawBytes = s_strict.GetByteCount(value);
            }
            catch (ArgumentException)
            {
                problem = PathRejection.Encoding;
                return null;
            }
            // Each escape decodes three bytes into one
            if (rawBytes > 3L * MaxPathBytes)
            {
                problem = PathRejection.TooLong;
                return null;
            }
            var buffer = ArrayPool<byte>.Shared.Rent(rawBytes);
            try
            {
                var length = 0;
                var start = 0;
                for (int i = 0; i < value.Length; i++)
                {
                    if (value[i] != '%')
                    {
                        continue;
                    }
                    length += s_strict.GetBytes(value.AsSpan(start, i - start), buffer.AsSpan(length));
                    buffer[length++] = (byte)((HexValue(value[i + 1]) << 4) | HexValue(value[i + 2]));
                    i += 2;
                    start = i + 1;
                }
                length += s_strict.GetBytes(value.AsSpan(start), buffer.AsSpan(length));
                return s_strict.GetString(buffer, 0, length);
            }
            catch (ArgumentException)
            {
                problem = PathRejection.Encoding;
                return null;
            }
            finally
            {
                ArrayPool<byte>.Shared.Return(buffer);
            }
        }

        private static int HexValue(char c) => c <= '9' ? c - '0' : (c | 0x20) - 'a' + 10;

        // The length of an RFC 3986 scheme before its ':', zero when there is none
        private static int SchemeLength(string value)
        {
            if (!char.IsAsciiLetter(value[0]))
            {
                return 0;
            }
            for (int i = 1; i < value.Length; i++)
            {
                var c = value[i];
                if (c == ':')
                {
                    return i;
                }
                if (!(char.IsAsciiLetterOrDigit(c) || c is '+' or '-' or '.'))
                {
                    return 0;
                }
            }
            return 0;
        }

        // The authority after '//', null without one, and the still-encoded path
        private static void SplitUri(string value, int scheme, out string? authority, out string path)
        {
            var rest = value.Substring(scheme + 1);
            if (!rest.StartsWith("//", StringComparison.Ordinal))
            {
                authority = null;
                path = rest;
                return;
            }
            var end = rest.IndexOf('/', 2);
            authority = end < 0 ? rest.Substring(2) : rest.Substring(2, end - 2);
            path = end < 0 ? "" : rest.Substring(end);
        }

        private static bool IsAny(string value, params string[] options)
        {
            foreach (var option in options)
            {
                if (value.Equals(option, StringComparison.OrdinalIgnoreCase))
                {
                    return true;
                }
            }
            return false;
        }

        private static bool IsPrintableAscii(string value)
        {
            foreach (var c in value)
            {
                if (c < 0x20 || c > 0x7E)
                {
                    return false;
                }
            }
            return true;
        }

        private static string AsciiUpper(string value)
        {
            return string.Create(value.Length, value, static (span, source) =>
            {
                for (int i = 0; i < source.Length; i++)
                {
                    var c = source[i];
                    span[i] = c is >= 'a' and <= 'z' ? (char)(c - 32) : c;
                }
            });
        }
    }

    /// <summary>
    /// What a native name lookup found.
    /// </summary>
    internal enum NativeStatus : byte
    {
        Found = 0,
        Missing = 1,
        AccessDenied = 2,
        Failed = 3
    }

    /// <summary>
    /// Native name lookups without links followed; this portable form has no aliases to expand.
    /// </summary>
    internal class NativeNames
    {
        public static NativeNames Portable { get; } = new NativeNames();

        // The long form of an existing path
        public virtual NativeStatus LongPath(string path, out string longPath)
        {
            longPath = path;
            return Attributes(path, out _);
        }

        // The attributes of the path itself, a final link not followed
        public virtual NativeStatus Attributes(string path, out FileAttributes attributes)
        {
            attributes = default;
            try
            {
                attributes = File.GetAttributes(path);
                return NativeStatus.Found;
            }
            catch (FileNotFoundException)
            {
                return NativeStatus.Missing;
            }
            catch (DirectoryNotFoundException)
            {
                return NativeStatus.Missing;
            }
            catch (UnauthorizedAccessException)
            {
                return NativeStatus.AccessDenied;
            }
            catch (Exception e) when (e is IOException or ArgumentException or NotSupportedException)
            {
                return NativeStatus.Failed;
            }
        }
    }

    /// <summary>
    /// Windows lookups on extended paths, which expand every short alias, with or without a tilde.
    /// </summary>
    internal class WindowsNativeNames : NativeNames
    {
        private const uint InvalidFileAttributes = 0xFFFFFFFF;

        // Grows once to the longest extended path
        private char[] _buffer = new char[1024];

        public override NativeStatus LongPath(string path, out string longPath)
        {
            longPath = path;
            if (!OperatingSystem.IsWindows())
            {
                return NativeStatus.Failed;
            }
            for (int attempt = 0; attempt < 2; attempt++)
            {
                var length = GetLongPathNameW(path, _buffer, (uint)_buffer.Length);
                if (length == 0)
                {
                    return StatusOf(Marshal.GetLastPInvokeError());
                }
                if (length < _buffer.Length)
                {
                    longPath = new string(_buffer, 0, (int)length);
                    return NativeStatus.Found;
                }
                if (length > PathCanonicalizer.MaxWindowsPath + 1)
                {
                    return NativeStatus.Failed;
                }
                _buffer = new char[length];
            }
            return NativeStatus.Failed;
        }

        public override NativeStatus Attributes(string path, out FileAttributes attributes)
        {
            attributes = default;
            if (!OperatingSystem.IsWindows())
            {
                return NativeStatus.Failed;
            }
            var value = GetFileAttributesW(path);
            if (value == InvalidFileAttributes)
            {
                return StatusOf(Marshal.GetLastPInvokeError());
            }
            attributes = (FileAttributes)value;
            return NativeStatus.Found;
        }

        // File, path and directory-name errors mean absent; anything else is a failed lookup
        private static NativeStatus StatusOf(int error)
        {
            return error switch
            {
                2 or 3 or 267 => NativeStatus.Missing,
                5 => NativeStatus.AccessDenied,
                _ => NativeStatus.Failed
            };
        }

        [DllImport("kernel32.dll", CharSet = CharSet.Unicode, SetLastError = true, ExactSpelling = true)]
        private static extern uint GetLongPathNameW(string shortPath, [Out] char[] longPath, uint bufferLength);

        [DllImport("kernel32.dll", CharSet = CharSet.Unicode, SetLastError = true, ExactSpelling = true)]
        private static extern uint GetFileAttributesW(string fileName);
    }
}
