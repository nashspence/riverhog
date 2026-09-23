# riverhog: HTTP Schemas: evidence gaps

[Atlas](../../../index.md) · [Reference navigation](index.md)

This view covers **37 affected contract elements** within riverhog: HTTP Schemas. Only their recorded evidence groups are included.

The named contract groups have recorded evidence gaps in the following guarantees. Each group's page identifies its exact open guarantees and candidate tests:

- Each step stays within its declared limits.
- Continuing the work makes progress toward its declared completion.
- The operation works across multiple pages or chunks.
- Required data or work is not silently left out.
- Work can resume after a restart as its contract requires.

These guarantees let large tasks proceed in smaller steps: a limit on one page or chunk must not become a hidden limit on the whole task. Returning a first page correctly does not establish that continuation or recovery works. Capacity may explicitly reject, defer, or throttle work; it must not silently omit work.

Existing tests may establish individual cases. The gaps retain their recorded group-wide scope and do not establish a bug in every linked contract. Completion follows each contract's rules; mutable browsing carries no implied snapshot guarantee.

The table identifies the exact evidence groups for each element. Its element link opens the local explanation and governing rules; each evidence group gives its specific open guarantees and candidate tests.

| Contract element | Evidence group |
|---|---|
| [riverhog: schemas: AddCollectionUploadTagsRequest](schemas-addcollectionuploadtagsrequest.md#evidence-gaps) | [riverhog-upload-tag-staging-progression/v1](../../../evidence/qualifications/riverhog-upload-tag-staging-progression-v1/index.md) |
| [riverhog: schemas: AppAccessListOut](schemas-appaccesslistout.md#evidence-gaps) | [riverhog-read-collection-progression/v1](../../../evidence/qualifications/riverhog-read-collection-progression-v1/index.md) |
| [riverhog: schemas: AppKeyListOut](schemas-appkeylistout.md#evidence-gaps) | [riverhog-read-collection-progression/v1](../../../evidence/qualifications/riverhog-read-collection-progression-v1/index.md) |
| [riverhog: schemas: AppListOut](schemas-applistout.md#evidence-gaps) | [riverhog-read-collection-progression/v1](../../../evidence/qualifications/riverhog-read-collection-progression-v1/index.md) |
| [riverhog: schemas: ArchiveCopyJobListOut](schemas-archivecopyjoblistout.md#evidence-gaps) | [riverhog-read-collection-progression/v1](../../../evidence/qualifications/riverhog-read-collection-progression-v1/index.md) |
| [riverhog: schemas: ArchiveStoreListOut](schemas-archivestorelistout.md#evidence-gaps) | [riverhog-read-collection-progression/v1](../../../evidence/qualifications/riverhog-read-collection-progression-v1/index.md) |
| [riverhog: schemas: ArtifactDispositionBatchDocument](schemas-artifactdispositionbatchdocument.md#evidence-gaps) | [riverhog-work-disposition-append/v1](../../../evidence/qualifications/riverhog-work-disposition-append-v1/index.md) |
| [riverhog: schemas: ArtifactDispositionOutputBatchDocument](schemas-artifactdispositionoutputbatchdocument.md#evidence-gaps) | [riverhog-work-disposition-append/v1](../../../evidence/qualifications/riverhog-work-disposition-append-v1/index.md) |
| [riverhog: schemas: ArtifactDispositionOutputPageDocument](schemas-artifactdispositionoutputpagedocument.md#evidence-gaps) | [riverhog-read-collection-progression/v1](../../../evidence/qualifications/riverhog-read-collection-progression-v1/index.md) |
| [riverhog: schemas: ArtifactDispositionPageDocument](schemas-artifactdispositionpagedocument.md#evidence-gaps) | [riverhog-read-collection-progression/v1](../../../evidence/qualifications/riverhog-read-collection-progression-v1/index.md) |
| [riverhog: schemas: CatalogSyncChangePage](schemas-catalogsyncchangepage.md#evidence-gaps) | [riverhog-read-collection-progression/v1](../../../evidence/qualifications/riverhog-read-collection-progression-v1/index.md) |
| [riverhog: schemas: CatalogSyncCollectionPage](schemas-catalogsynccollectionpage.md#evidence-gaps) | [riverhog-read-collection-progression/v1](../../../evidence/qualifications/riverhog-read-collection-progression-v1/index.md) |
| [riverhog: schemas: CollectionArchiveCopyListOut](schemas-collectionarchivecopylistout.md#evidence-gaps) | [riverhog-read-collection-progression/v1](../../../evidence/qualifications/riverhog-read-collection-progression-v1/index.md) |
| [riverhog: schemas: CollectionArtifactBatchDocument](schemas-collectionartifactbatchdocument.md#evidence-gaps) | [riverhog-work-set-append/v1](../../../evidence/qualifications/riverhog-work-set-append-v1/index.md) |
| [riverhog: schemas: CollectionArtifactPageDocument](schemas-collectionartifactpagedocument.md#evidence-gaps) | [riverhog-read-collection-progression/v1](../../../evidence/qualifications/riverhog-read-collection-progression-v1/index.md) |
| [riverhog: schemas: CollectionRootBatchDocument](schemas-collectionrootbatchdocument.md#evidence-gaps) | [riverhog-work-set-append/v1](../../../evidence/qualifications/riverhog-work-set-append-v1/index.md) |
| [riverhog: schemas: CollectionRootPageDocument](schemas-collectionrootpagedocument.md#evidence-gaps) | [riverhog-read-collection-progression/v1](../../../evidence/qualifications/riverhog-read-collection-progression-v1/index.md) |
| [riverhog: schemas: CollectionTagListOut](schemas-collectiontaglistout.md#evidence-gaps) | [riverhog-read-collection-progression/v1](../../../evidence/qualifications/riverhog-read-collection-progression-v1/index.md) |
| [riverhog: schemas: CollectionUploadRawDigestBatchDocument](schemas-collectionuploadrawdigestbatchdocument.md#evidence-gaps) | [riverhog-raw-digest-progression/v1](../../../evidence/qualifications/riverhog-raw-digest-progression-v1/index.md) |
| [riverhog: schemas: CollectionUploadUnitWorkDocument](schemas-collectionuploadunitworkdocument.md#evidence-gaps) | [riverhog-upload-unit-source-progression/v1](../../../evidence/qualifications/riverhog-upload-unit-source-progression-v1/index.md) |
| [riverhog: schemas: CollectionUploadWorkBatchDocument](schemas-collectionuploadworkbatchdocument.md#evidence-gaps) | [riverhog-upload-work-progression/v1](../../../evidence/qualifications/riverhog-upload-work-progression-v1/index.md) |
| [riverhog: schemas: CreateOrResumeCollectionUploadSessionRequest](schemas-createorresumecollectionuploadsessionrequest.md#evidence-gaps) | [riverhog-upload-tag-staging-progression/v1](../../../evidence/qualifications/riverhog-upload-tag-staging-progression-v1/index.md) |
| [riverhog: schemas: KeyDownloadQuotaListOut](schemas-keydownloadquotalistout.md#evidence-gaps) | [riverhog-read-collection-progression/v1](../../../evidence/qualifications/riverhog-read-collection-progression-v1/index.md) |
| [riverhog: schemas: ListCollectionUploadSessionFilesResponse](schemas-listcollectionuploadsessionfilesresponse.md#evidence-gaps) | [riverhog-read-collection-progression/v1](../../../evidence/qualifications/riverhog-read-collection-progression-v1/index.md) |
| [riverhog: schemas: ListCollectionUploadSessionsResponse](schemas-listcollectionuploadsessionsresponse.md#evidence-gaps) | [riverhog-read-collection-progression/v1](../../../evidence/qualifications/riverhog-read-collection-progression-v1/index.md) |
| [riverhog: schemas: ListCollectionsResponse](schemas-listcollectionsresponse.md#evidence-gaps) | [riverhog-read-collection-progression/v1](../../../evidence/qualifications/riverhog-read-collection-progression-v1/index.md) |
| [riverhog: schemas: ListProvenanceJournalAgentsResponse](schemas-listprovenancejournalagentsresponse.md#evidence-gaps) | [riverhog-read-collection-progression/v1](../../../evidence/qualifications/riverhog-read-collection-progression-v1/index.md) |
| [riverhog: schemas: PortableCollectionInventoryPage](schemas-portablecollectioninventorypage.md#evidence-gaps) | [riverhog-read-collection-progression/v1](../../../evidence/qualifications/riverhog-read-collection-progression-v1/index.md) |
| [riverhog: schemas: ProcessingClaimPageDocument](schemas-processingclaimpagedocument.md#evidence-gaps) | [riverhog-read-collection-progression/v1](../../../evidence/qualifications/riverhog-read-collection-progression-v1/index.md) |
| [riverhog: schemas: ProcessingOutcomePageDocument](schemas-processingoutcomepagedocument.md#evidence-gaps) | [riverhog-read-collection-progression/v1](../../../evidence/qualifications/riverhog-read-collection-progression-v1/index.md) |
| [riverhog: schemas: RegisterCollectionUploadSessionFilesRequest](schemas-registercollectionuploadsessionfilesrequest.md#evidence-gaps) | [riverhog-upload-registration-progression/v1](../../../evidence/qualifications/riverhog-upload-registration-progression-v1/index.md) |
| [riverhog: schemas: RetrievalCacheObjectListOut](schemas-retrievalcacheobjectlistout.md#evidence-gaps) | [riverhog-read-collection-progression/v1](../../../evidence/qualifications/riverhog-read-collection-progression-v1/index.md) |
| [riverhog: schemas: RetrievalPlanFilePageOut](schemas-retrievalplanfilepageout.md#evidence-gaps) | [riverhog-read-collection-progression/v1](../../../evidence/qualifications/riverhog-read-collection-progression-v1/index.md) |
| [riverhog: schemas: RetrievalPlanRequest](schemas-retrievalplanrequest.md#evidence-gaps) | [riverhog-retrieval-work-progression/v1](../../../evidence/qualifications/riverhog-retrieval-work-progression-v1/index.md) |
| [riverhog: schemas: RiverhogEventPage](schemas-riverhogeventpage.md#evidence-gaps) | [riverhog-read-collection-progression/v1](../../../evidence/qualifications/riverhog-read-collection-progression-v1/index.md) |
| [riverhog: schemas: SearchResponse](schemas-searchresponse.md#evidence-gaps) | [riverhog-read-collection-progression/v1](../../../evidence/qualifications/riverhog-read-collection-progression-v1/index.md) |
| [riverhog: schemas: TagListOut](schemas-taglistout.md#evidence-gaps) | [riverhog-read-collection-progression/v1](../../../evidence/qualifications/riverhog-read-collection-progression-v1/index.md) |
