# Listing evidence for maintenance deletes

Deletion decisions rely on listings and exact-name requests made through Stowage. This file records what the pinned clients do, how it was established, and what remains an assumption. Every row names the test that reproduces it; `EveryEligibleStoreHasAnEvidenceRow` fails when a classification lacks its row or a named test does not exist.

The pinned package is Stowage 2.0.1, built from source revision `92612a6d12c0f78c93d38b65f3bb280bc07f6f1a`. `CleanupStoreContract` recognizes a store only by its concrete type in that exact build (assembly `Stowage`, public key token `0544266807c76150`, informational version `2.0.1+92612a6d12c0f78c93d38b65f3bb280bc07f6f1a`). It unwraps connector-owned wrappers and nothing else, so a marker interface such as `ILocalDiskFileStorage` on a foreign wrapper qualifies nothing. The qualification tests drive the real pinned DLL against a loopback service (`tests/.../LoopbackObjectStore.cs`, a BCL `TcpListener`). They use no emulator and no extra package.

## Concrete implementations

Eligibility here is the store's; every listing still passes the self-checks below before it decides a deletion, and with history retention and orphan sweeping both off nothing on this page throws.

| Factory | Implementation type | Cleanup eligibility | Notes | Evidence |
|---|---|---|---|---|
| `Files.Of.LocalDisk` | `Stowage.Impl.LocalDiskFileStorage` | Eligible, for names without empty, `.` or `..` segments or backslashes | Unpaged, unordered enumeration, checked through a sorted view that keeps repeated names. `Rm` on a folder recurses, so maintenance deletes with native `File.Delete`. | `LocalUnorderedSnapshotGetsOrderedEvidenceView`, `FilesOfClassificationIsPinned` |
| `Files.Of.AmazonS3`, `Files.Of.Minio` | `Stowage.Impl.Amazon.AwsS3FileStorage` | Eligible within the qualified domain | Path or virtual-host addressing. Every request uses only the endpoint's authority, so listings and deletes address the same objects. | `S3SafePrefixesRoundTripSemantically`, `S3ContinuationTokensRoundTripSemantically`, `S3Utf8OrderHandlesSupplementaryForeignNames` |
| `Files.Of.AzureBlobStorage` with a bare endpoint | `Stowage.Impl.Microsoft.AzureBlobFileStorage` | Eligible within the qualified domain | Shared key or Entra ID. The endpoint is read from the pinned client and must have the path `/`. | `AzureQualifiedPrefixesRoundTripSemantically`, `AzureQualifiedDeleteRoundTripsAndPreservesNeighbor`, `AzureQualifiedMultiPageListingIncludesShortAndEmptyPages` |
| `Files.Of.AzureBlobStorage` with an endpoint path, `Files.Of.AzureBlobStorageWithLocalEmulator` | `Stowage.Impl.Microsoft.AzureBlobFileStorage` (endpoint path) | Ineligible, even with `AssumeCompleteStorageListing` | Deletes drop the endpoint's path segment. An endpoint that cannot be read is treated the same way. | `AzurePathPrefixedEndpointDeleteMissesTheObject`, `KnownFailedProviderCannotBeVouched` |
| `Files.Of.GoogleCloudStorage` | `Stowage.Impl.Google.GoogleCloudStorage` | Ineligible, even with `AssumeCompleteStorageListing` | Reads only the first listing page, and `Stat` is not implemented. | `GcsRefusedAtConstructionEvenVouchedWhenOptedIn` |
| `Files.Of.InternalMemory` | `Stowage.Impl.InMemoryFileStorage` | Asserted only | Tests only. Unpaged, checked through a sorted view. Disposing a read stream re-adds a removed file. | `FilesOfClassificationIsPinned` |
| Any other storage or wrapper, including another Stowage build | | Asserted only | The assertion covers the complete stack, which must list completely and in order and reach an object only by its exact name, so a reference can only protect the object it spells; no synthetic entries are removed and nothing is sorted. Orphan sweeping is not qualified. | `UnknownWrapperOptInRequiresVouchAtConstruction`, `OrphanOptInRequiresDirectoryExclusion` |

The qualified domain is nonempty ASCII `[A-Za-z0-9._/-]` below the bucket or container, with no empty, `.` or `..` segment. It is checked on the original configured name before path normalization, on every listing prefix (which may add one terminal slash), and on the full key of every deletion target. Other names are kept and counted.

| Behavior | Evidence |
|---|---|
| Names normalization would rewrite, such as `c/x/../t` or `c//t`, are refused on the original string. A listing prefix may end in one slash; a second one is an empty segment. | `OriginalDotDotAndEmptySegmentsCannotNormalizeIntoEligibility` |
| Names with characters the clients send unencoded (`+ % space ? # & = ~ :` and non-ASCII) are refused when an opt-in is set, and only make cleanup ineligible otherwise. | `S3ReservedPrefixesRefusedAtConstructionWhenOptedIn`, `AzureReservedPrefixesRefusedAtConstructionWhenOptedIn`, `OptInsOffNeverThrowNewStoreOrDomainConfigurationExceptions` |
| A deletion target qualifies only when its exact full key, table prefix included, is in the domain and unchanged by normalization. | `DeletionTargetsMustBeExactQualifiedNames` |
| Classification and validation make no storage request. | `ConstructionValidationIssuesNoStorageRequests`, `ConstructionValidationIssuesNoRequestsToPinnedClients` |
| A storage instance replaced after construction declines cleanup without a configuration error. | `ChangedStorageInstanceDeclinesWithoutConfigurationException`, `RestoreCommitAndWorkerDoNotThrowConfigurationExceptions` |

## Client transmission

| Behavior | Evidence |
|---|---|
| Azure sends the prefix, delimiter and marker without percent-encoding. A table path in `[A-Za-z0-9._/-]` reaches the service unchanged. | `AzureQualifiedPrefixesRoundTripSemantically` |
| An Azure marker in the observed public format (`2!<len>!<base64 of the next name and a fixed tail>`, with `=` padding written as `-`) goes out byte-identical. For ASCII names, base64 produces `+` only from `>` or `~`, which the qualified domain excludes. | `AzureObservedPublicMarkerRoundTripsByteIdentically` |
| S3 sends the prefix and delimiter unencoded but percent-encodes continuation tokens. Tokens holding `+ / = & %` round-trip. | `S3ContinuationTokensRoundTripSemantically`, `S3SafePrefixesRoundTripSemantically` |
| Azure and S3 send a DELETE to exactly the object's name: a neighbor sharing the name as a prefix survives, and a later `Stat` is absent. | `AzureQualifiedDeleteRoundTripsAndPreservesNeighbor` |
| **Endpoint limitation:** with an Azure endpoint whose URL carries a path segment, such as an emulator's `/devstoreaccount1/`, the DELETE drops that segment and reaches another account. `Rm` swallows the resulting not-found, so the object silently survives. Such endpoints are classified as known failed. | `AzurePathPrefixedEndpointDeleteMissesTheObject` |

## Paging and errors

| Behavior | Evidence |
|---|---|
| Azure follows `NextMarker` until it is empty, through short pages and empty pages that are not the last. | `AzureQualifiedMultiPageListingIncludesShortAndEmptyPages` |
| On Azure and S3, an HTTP error (400, 403, 404, 500 or 503) on the second or third listing page throws, also when every retry fails. The pages already read are never returned as a shorter list. | `AzurePageErrorNeverReturnsShortenedList`, `S3PageErrorNeverReturnsShortenedList` |
| Azure and S3 `Stat` return null only for a 404. A 400, 403, 500 or 503 on the HEAD request throws. | `AzureStatDistinguishesAbsentFromHttpError`, `S3StatDistinguishesAbsentFromHttpError` |
| Azure and S3 `Stat` answer a successor probe: only a 404 is absence, every other status is a probe error. | `SuccessorStatErrorIsNotAbsence` |

## Client-generated entries

| Behavior | Evidence |
|---|---|
| A recursive Azure or S3 listing inserts the immediate parent folders at index 0. They are taken relative to the listed root, without root parents, in first-occurrence order, and then prefixed with the container or bucket. These synthetic entries are slash-terminated folders with no size, and they lie outside the requested folder. Nothing is sorted afterward. | `PinnedRecursiveDeliveryUnderNonRootPrefix` |
| The evidence check recomputes that block with the client's own `IOPath` functions and requires an exact ordered match; only that block is removed. A missing, extra, reordered, renamed, reshaped, repeated or misplaced synthetic folder declines, and so does any later synthetic-shaped entry. | `RecursiveImplicitFolderBlockIsRecomputedAndExcluded`, `TamperedOrMisplacedImplicitFolderBlockDeclines` |
| Over several pages under a non-root prefix, with `_delta_log/`, `_change_data/`, `.flowtide-orphan-sweep`, a nested folder and a zero-size folder marker, the provider entries pass and stay in delivered order. | `PinnedRecursiveInventoryPassesUnderNonRootPrefix` |
| The pinned S3 client drops every key that ends in a slash while it parses a page, so S3 folder marker objects never appear; Azure keeps them as zero-size entries. | `PinnedRecursiveInventoryPassesUnderNonRootPrefix` |
| A hierarchical-namespace Azure account lists its directories as entries. The pinned client ignores `ResourceType` and reports them as files of size zero, in the service's order, where slash sorts lowest. They stay in the evidence sequence. | `PinnedRecursiveDeliveryUnderNonRootPrefix`, `PinnedRecursiveInventoryPassesUnderNonRootPrefix` |
| A non-recursive listing keeps the provider's grouping: S3 delivers files then common prefixes, and Azure delivers each page's prefixes before its blobs. A folder holding subfolders is therefore not one sorted sequence, and its listing declines as evidence. | `NonrecursiveListingKeepsProviderGroups`, `NonrecursiveEvidenceSubfoldersDeclineAcrossPages` |

## Listing self-checks

Every listing that decides a deletion passes these checks on the delivered sequence, after the synthetic block is removed and before any filtering, sorting or dictionary insertion. Failures decline with a `ListingCheck` reason; nothing throws to the engine.

| Behavior | Evidence |
|---|---|
| Every entry lies strictly inside the requested folder, bounded by a separator; the folder itself and a sibling sharing its name as a prefix decline. | `EntryOutsideFolderDeclines` |
| Full physical names strictly increase with no repeats. S3 and asserted stores use UTF-8 byte order; local snapshots are sorted first and keep repeats. | `ListingDuplicateOrRegressionDeclinesBeforeClassification`, `S3Utf8OrderHandlesSupplementaryForeignNames`, `LocalUnorderedSnapshotGetsOrderedEvidenceView` |
| A recursive Azure listing may follow the ordinary order or the hierarchical order with slash lowest, but one of them must hold for the whole sequence. | `HnsSlashLowestWholeSequenceAccepted`, `MixedComparatorOnlyPerPairWouldPassButWholeSequenceDeclines` |
| Every commit JSON in each range a decision relies on is listed; a checkpoint or compaction above the newest JSON declines, and stage files never set the head. | `MissingInteriorCommitDeclinesEveryReliedRange`, `CheckpointAboveNewestCommitDeclines` |
| A `.flowtide-cleanup.*` record this version cannot interpret, a folder under that prefix included, declines every deletion before the successor probe. A started advance's reservation travels with the evidence as its fence. | `FutureOrUnreadableControlFencesPr1Mutation`, `UnknownCleanupBoundaryReservesLongMaxValue` |
| A direct `Stat` of `{head+1:D20}.json` must be a valid absence. An empty log probes version 0, a head of `long.MaxValue` declines without a probe, and a commit published between the listing and the probe declines that attempt. | `ListedHeadPlusOneExistsDeclines`, `EmptyListingProbesVersionZero`, `HeadOverflowDeclines`, `SuccessorPublishedBetweenListAndStatRetriesWithoutDeletion` |
| An ineligible store whose listing passes every check, the successor probe included, yields stage-only evidence. It shows only stages whose target commit is listed and can authorize no absence decision, so payload, below-O stages, temporaries and controls stay out of reach. A store without `Stat` declines at the probe. | `IneligibleListableStoreGivesStageOnlyEvidence` |
| A gate refuses a listing whose successor probe started before the previous gate ended, so every gate needs its own listing; a foreign commit between two gates stops volatile work. | `GatesNeverPassOnAStaleListing` |

## Provider guarantees and remaining assumptions

- **S3:** strongly consistent listings after writes and deletes. Keys are listed in UTF-8 byte order. [S3 consistency](https://docs.aws.amazon.com/AmazonS3/latest/userguide/Welcome.html#ConsistencyModel)
- **Azure Blob:** documents strong consistency for reads and lists, opaque markers, and short or empty pages that are not the last. [List Blobs](https://learn.microsoft.com/en-us/rest/api/storageservices/list-blobs)
- **Unverified premise:** Azure keeps the observed marker format, or rejects a damaged marker loudly. A public sample rejects an invalid marker with `InvalidQueryParameterValue`. The marker format on hierarchical-namespace accounts was never sampled.
- **Unverified premise:** an eligible listing returns every object present for its whole duration, across all pages, or throws. The listing self-checks supplement this premise; they do not prove it. An omission confined to dot-prefixed control and stage entries is not detected by them.
- **Observed, not reproduced deterministically:** the pinned S3 signer hashes payloads with one static `SHA256` (`System.ByteArrayExtensions._sha256`), shared by every S3 client in the process. Concurrent requests can throw `CryptographicException` ("Concurrent operations from multiple threads"); seen when loopback S3 tests ran in parallel. The failure throws, so a listing or probe declines rather than passing. The tests that drive the pinned clients share one xUnit collection for this reason.
- **Unverified premise:** the loopback fixture drives Azure with shared-key authentication. SAS and Entra ID change how a request is signed and add query parameters for SAS, and they are assumed not to change the prefix, delimiter, marker or delete target the client sends.
- **Asserted stacks, exact names:** syntax cannot establish physical identity (a Windows short name needs no `~`). Asserted stacks therefore vouch that a name reaches only its own object; the folded identity still declines trailing dots and spaces, `:` and `~` as a guard, not as proof.
- **Pinned read side (pre-existing):** the Azure and S3 clients send a name with `#` or a trailing space as the name before it. Protection keys follow the Delta spec, where such a reference names a different object, so only Flowtide's own reader through these clients could reach the shorter name. Candidates never contain these characters.
- **Disclosed exception, one request per discarded file in SaveData:** the inline fallback (`InlineDiscards` through `DeletionExecutor.DeleteOwnAsync`) issues a `Stat` before the `Rm` of each discarded change-data file whose key passes its checks on a store of unknown kind, since its `Rm` may remove a folder; main issued the `Rm` alone. It applies only in inline mode: always on an unvouched unknown store, and on a vouched one only when maintenance failed to start or the kill switch was on. Queued mode issues no request in SaveData. The docs' "Inline fallback" bullet states the `Stat`.
- **Limits of a loopback fixture:** it fixes how a service answers. It cannot show how a real service decodes a malformed request. Those observations come from public samples and stay premises.
