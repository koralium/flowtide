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

using FlowtideDotNet.Connector.DeltaLake.Internal.Maintenance;
using FlowtideDotNet.Connector.DeltaLake.Internal.Maintenance.References;
using Stowage;
using Stowage.Impl;

namespace FlowtideDotNet.Connector.DeltaLake.Tests
{
    public class AssertedAliasProtectionTests
    {
        [Fact]
        public async Task AssertedNativeWrapperProtectsTrailingDotReadAlias()
        {
            var directory = Path.Combine(Path.GetTempPath(), "aa-" + ScratchOwners.ShortId());
            Directory.CreateDirectory(Path.Combine(directory, "t"));
            File.WriteAllText(Path.Combine(directory, "t", "part-1.parquet"), "published payload");
            try
            {
                using var storage = new OrderedFileStorage(Files.Of.LocalDisk(directory));
                if (OperatingSystem.IsWindows())
                {
                    // The published spelling reads the physical candidate through the configured store
                    using var referenced = await storage.OpenRead("/t/part-1.parquet.");
                    Assert.NotNull(referenced);
                    using var reader = new StreamReader(referenced!);
                    Assert.Equal("published payload", await reader.ReadToEndAsync());
                }
                var configuration = CleanupConfiguration.Validate(storage, "t", true, CleanupOptIns.Off);
                Assert.True(configuration.Store.Eligible);
                var identity = PathCanonicalizer.Create(configuration.Store);
                Assert.Equal(PathIdentity.Folded, identity.Identity);

                var owners = new ScratchOwners();
                try
                {
                    using var scratch = owners.Add(new ScratchDirectory(Path.Combine(directory, "scratch"))).CreateJob();
                    var memory = new JobMemory();
                    using var candidates = new PathSet(scratch, memory, PathRole.Candidate);
                    using var protection = new PathSet(scratch, memory, PathRole.Protection);
                    var own = new CommitReferenceExtractor(identity, candidates, PathRole.Candidate);
                    var live = new CommitReferenceExtractor(identity, protection, PathRole.Protection);
                    Assert.True(own.AddName("part-1.parquet"));
                    // Either refusing an unsupported protecting alias or canonicalizing it is safe
                    if (!live.AddLine("""{"add":{"path":"part-1.parquet."}}"""u8))
                    {
                        Assert.Equal((PathRejection.NameSyntax, ReferenceSource.Add), (live.Declined, live.DeclinedSource));
                        return;
                    }
                    candidates.Seal();
                    protection.Seal();
                    using var difference = PathSet.Difference(candidates, protection);
                    Assert.False(difference.MoveNext(), "A spelling readable as the published file must protect that physical candidate.");
                }
                finally
                {
                    owners.Release();
                }
            }
            finally
            {
                try
                {
                    Directory.Delete(directory, recursive: true);
                }
                catch (IOException)
                {
                }
                catch (UnauthorizedAccessException)
                {
                }
            }
        }

        // A user wrapper with complete ordered delivery and file-only exact qualified deletes
        internal sealed class OrderedFileStorage : ForwardingStorage, IFileStorage
        {
            public OrderedFileStorage(IFileStorage inner) : base(inner)
            {
            }

            public new async Task<IReadOnlyCollection<IOEntry>> Ls(IOPath? path = null, bool recurse = false, CancellationToken cancellationToken = default)
            {
                return (await Inner.Ls(path, recurse, cancellationToken)).OrderBy(x => x.Path.Full, StringComparer.Ordinal).ToArray();
            }

            public new Task Rm(IOPath path, CancellationToken cancellationToken = default)
            {
                cancellationToken.ThrowIfCancellationRequested();
                if (CleanupStoreContract.CheckSegments(path.Full.TrimStart('/'), qualifiedDomain: true) != null
                    || CleanupStoreContract.HasTrailingDotSegment(path.Full))
                {
                    throw new ArgumentException("The wrapper refuses inexact target syntax", nameof(path));
                }
                File.Delete(((ILocalDiskFileStorage)Inner).ToNativeLocalPath(path));
                return Task.CompletedTask;
            }
        }
    }
}
