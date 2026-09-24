# riverhog_protocol.RiverhogEventPage

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-protocol:riverhog-protocol-riverhogeventpage:b756e6c28e -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-34f582a2ca"></a>
- <a id="s-70c2b349dc"></a>`distribution`: `riverhog-protocol`
- <a id="s-b261a446ec"></a>`module`: `riverhog_protocol`
- <a id="s-f0b2a3acab"></a>`name`: `RiverhogEventPage`
- <a id="s-79f392f105"></a>`unit`: `export`

### Declared structure

- <a id="s-7185cbf2f1"></a>`kind`: `"class"`
- <a id="s-2220813a11"></a>`signature`: `"'(*, events: list[RiverhogLifecycleEvent], next_cursor: LifecycleEventCursor, has_more: bool) -> None'"`

#### Validated model schema

<a id="s-1e0a8aa4fc"></a>

- <a id="s-424e2b5528"></a>`type`: `"object"`
- <a id="s-e7eca40285"></a>`additionalProperties`: `false`
- <a id="s-47e9884b98"></a>`required`: `["events","next_cursor","has_more"]`

##### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-75f3ba19a8"></a>`events` | yes | type="array"; items=([RiverhogLifecycleEvent](#s-41ab5ca15a)) |  |
| <a id="s-3e40fb765d"></a>`has_more` | yes | type="boolean" |  |
| <a id="s-b862758468"></a>`next_cursor` | yes | [LifecycleEventCursor](#s-99b2d14f05) |  |

##### Definitions

- [ArchiveCopyJobCanceledData](#s-9f4a07c6e6)
- [ArchiveCopyJobCanceledEvent](#s-31a0f3b0ea)
- [ArchiveCopyJobCompletedData](#s-696dcb49a9)
- [ArchiveCopyJobCompletedEvent](#s-d305e985f9)
- [ArchiveCopyJobFailedData](#s-e826ef642d)
- [ArchiveCopyJobFailedEvent](#s-ddec468949)
- [ArchiveCopyJobRequestedData](#s-f7f1999027)
- [ArchiveCopyJobRequestedEvent](#s-08328a8233)
- [ArchiveStoreName](#s-2e2a7afe9a)
- [CollectionDeletedData](#s-04e497d16f)
- [CollectionDeletedEvent](#s-e18a1ebf85)
- [CollectionFinalizedData](#s-caf8e4a999)
- [CollectionFinalizedEvent](#s-dcb5f23831)
- [CollectionId](#s-3b7d7e3c2c)
- [LifecycleEventCursor](#s-99b2d14f05)
- [PrincipalId](#s-26dc5f6284)
- [RetrievalCanceledData](#s-8f780dd62d)
- [RetrievalCanceledEvent](#s-e80951e313)
- [RetrievalCompletedData](#s-e680525602)
- [RetrievalCompletedEvent](#s-e763e9511f)
- [RetrievalExpiredData](#s-901491f41d)
- [RetrievalExpiredEvent](#s-cd3863d78c)
- [RetrievalFailedData](#s-c2a74431c4)
- [RetrievalFailedEvent](#s-f7876916d6)
- [RetrievalIssueData](#s-5d019111c7)
- [RetrievalIssueEvent](#s-061dcaf609)
- [RetrievalReadyData](#s-94276b17d8)
- [RetrievalReadyEvent](#s-f23d561333)
- [RetrievalRenewedData](#s-d1bc1f3107)
- [RetrievalRenewedEvent](#s-45fb1a016c)
- [RetrievalRequestedData](#s-64049bf6f9)
- [RetrievalRequestedEvent](#s-9dad1abe42)
- [RiverhogActor](#s-3ec07c9031)
- [RiverhogLifecycleEvent](#s-41ab5ca15a)

##### <a id="s-9f4a07c6e6"></a>definition `ArchiveCopyJobCanceledData`

- <a id="s-bb58b88f42"></a>`type`: `"object"`
- <a id="s-25a65f4cb3"></a>`additionalProperties`: `false`
- <a id="s-569356c997"></a>`required`: `["actor","initiator","collection_id","collection_created_at","source_store","destination_store","state"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-a2bf311030"></a>`actor` | yes | [RiverhogActor](#s-3ec07c9031) |  |
| <a id="s-3776852b57"></a>`collection_created_at` | yes | type="string"; maxLength=30; minLength=30; pattern="^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}\\.[0-9]{9}Z$" |  |
| <a id="s-3187d6cbeb"></a>`collection_id` | yes | [CollectionId](#s-3b7d7e3c2c) |  |
| <a id="s-e3c6214c81"></a>`context` | no | anyOf=[(type="object"; additionalProperties=(any JSON value); x-riverhog-encoded-bytes-max=4096; x-riverhog-extent={"policy":"contract_max","reason":"bounded-lifecycle-event-context"}); (type="null")]; default=null |  |
| <a id="s-d82a6cc7e8"></a>`destination_store` | yes | [ArchiveStoreName](#s-2e2a7afe9a) |  |
| <a id="s-e4a3817b39"></a>`initiator` | yes | [RiverhogActor](#s-3ec07c9031) |  |
| <a id="s-5972adef13"></a>`source_store` | yes | [ArchiveStoreName](#s-2e2a7afe9a) |  |
| <a id="s-39a2756f55"></a>`state` | yes | type="string"; const="canceled" |  |

##### <a id="s-31a0f3b0ea"></a>definition `ArchiveCopyJobCanceledEvent`

- <a id="s-61d8338ff9"></a>`type`: `"object"`
- <a id="s-2c298c66b7"></a>`additionalProperties`: `false`
- <a id="s-28679ea513"></a>`required`: `["id","type","occurred_at","payload"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-570c1b52e3"></a>`id` | yes | type="string"; minLength=1 |  |
| <a id="s-350ffb001e"></a>`occurred_at` | yes | type="string"; maxLength=30; minLength=30; pattern="^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}\\.[0-9]{9}Z$" |  |
| <a id="s-ac800eda94"></a>`payload` | yes | [ArchiveCopyJobCanceledData](#s-9f4a07c6e6) |  |
| <a id="s-5744ae2800"></a>`subject` | no | anyOf=[(type="string"; minLength=1); (type="null")]; default=null |  |
| <a id="s-a39126293c"></a>`type` | yes | type="string"; const="io.riverhog.riverhog.archive_copy_job.canceled" |  |

##### <a id="s-696dcb49a9"></a>definition `ArchiveCopyJobCompletedData`

- <a id="s-6a9a1bb21d"></a>`type`: `"object"`
- <a id="s-54996bd56e"></a>`additionalProperties`: `false`
- <a id="s-333a530815"></a>`required`: `["actor","initiator","collection_id","collection_created_at","source_store","destination_store","state"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-cb82bfea88"></a>`actor` | yes | [RiverhogActor](#s-3ec07c9031) |  |
| <a id="s-a989bad623"></a>`collection_created_at` | yes | type="string"; maxLength=30; minLength=30; pattern="^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}\\.[0-9]{9}Z$" |  |
| <a id="s-560e29b4d8"></a>`collection_id` | yes | [CollectionId](#s-3b7d7e3c2c) |  |
| <a id="s-f3b1eb0038"></a>`context` | no | anyOf=[(type="object"; additionalProperties=(any JSON value); x-riverhog-encoded-bytes-max=4096; x-riverhog-extent={"policy":"contract_max","reason":"bounded-lifecycle-event-context"}); (type="null")]; default=null |  |
| <a id="s-18359b47fa"></a>`destination_store` | yes | [ArchiveStoreName](#s-2e2a7afe9a) |  |
| <a id="s-c8fe3985d9"></a>`initiator` | yes | [RiverhogActor](#s-3ec07c9031) |  |
| <a id="s-5013a5dce5"></a>`source_store` | yes | [ArchiveStoreName](#s-2e2a7afe9a) |  |
| <a id="s-06a2715d75"></a>`state` | yes | type="string"; const="completed" |  |

##### <a id="s-d305e985f9"></a>definition `ArchiveCopyJobCompletedEvent`

- <a id="s-d9e2609242"></a>`type`: `"object"`
- <a id="s-7ff0832d29"></a>`additionalProperties`: `false`
- <a id="s-e4ae0d632e"></a>`required`: `["id","type","occurred_at","payload"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-92c039b755"></a>`id` | yes | type="string"; minLength=1 |  |
| <a id="s-688b56eefc"></a>`occurred_at` | yes | type="string"; maxLength=30; minLength=30; pattern="^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}\\.[0-9]{9}Z$" |  |
| <a id="s-505fe7b192"></a>`payload` | yes | [ArchiveCopyJobCompletedData](#s-696dcb49a9) |  |
| <a id="s-eab5955dae"></a>`subject` | no | anyOf=[(type="string"; minLength=1); (type="null")]; default=null |  |
| <a id="s-ece152b09c"></a>`type` | yes | type="string"; const="io.riverhog.riverhog.archive_copy_job.completed" |  |

##### <a id="s-e826ef642d"></a>definition `ArchiveCopyJobFailedData`

- <a id="s-7e4205182f"></a>`type`: `"object"`
- <a id="s-bc0761a12e"></a>`additionalProperties`: `false`
- <a id="s-2f279c4a05"></a>`required`: `["actor","initiator","collection_id","collection_created_at","source_store","destination_store","state","error"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-4f2680291c"></a>`actor` | yes | [RiverhogActor](#s-3ec07c9031) |  |
| <a id="s-344f8bd3f5"></a>`collection_created_at` | yes | type="string"; maxLength=30; minLength=30; pattern="^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}\\.[0-9]{9}Z$" |  |
| <a id="s-7cf9aa154e"></a>`collection_id` | yes | [CollectionId](#s-3b7d7e3c2c) |  |
| <a id="s-433b77401b"></a>`context` | no | anyOf=[(type="object"; additionalProperties=(any JSON value); x-riverhog-encoded-bytes-max=4096; x-riverhog-extent={"policy":"contract_max","reason":"bounded-lifecycle-event-context"}); (type="null")]; default=null |  |
| <a id="s-c3373ca00f"></a>`destination_store` | yes | [ArchiveStoreName](#s-2e2a7afe9a) |  |
| <a id="s-ccd122a88e"></a>`error` | yes | type="string"; maxLength=16384; minLength=1 |  |
| <a id="s-6bbe2e7fa0"></a>`initiator` | yes | [RiverhogActor](#s-3ec07c9031) |  |
| <a id="s-7d2cdbdb7c"></a>`source_store` | yes | [ArchiveStoreName](#s-2e2a7afe9a) |  |
| <a id="s-f01019f4bb"></a>`state` | yes | type="string"; const="failed" |  |

##### <a id="s-ddec468949"></a>definition `ArchiveCopyJobFailedEvent`

- <a id="s-0c47942447"></a>`type`: `"object"`
- <a id="s-f9f63f57b1"></a>`additionalProperties`: `false`
- <a id="s-e55638c969"></a>`required`: `["id","type","occurred_at","payload"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-bef3bd40f9"></a>`id` | yes | type="string"; minLength=1 |  |
| <a id="s-e2b3f2ec35"></a>`occurred_at` | yes | type="string"; maxLength=30; minLength=30; pattern="^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}\\.[0-9]{9}Z$" |  |
| <a id="s-e8441e345e"></a>`payload` | yes | [ArchiveCopyJobFailedData](#s-e826ef642d) |  |
| <a id="s-4b256775ee"></a>`subject` | no | anyOf=[(type="string"; minLength=1); (type="null")]; default=null |  |
| <a id="s-8560870a04"></a>`type` | yes | type="string"; const="io.riverhog.riverhog.archive_copy_job.failed" |  |

##### <a id="s-f7f1999027"></a>definition `ArchiveCopyJobRequestedData`

- <a id="s-002a184ce9"></a>`type`: `"object"`
- <a id="s-328b3c37ea"></a>`additionalProperties`: `false`
- <a id="s-76084a666d"></a>`required`: `["actor","initiator","collection_id","collection_created_at","source_store","destination_store","state"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-ad5f1a1ece"></a>`actor` | yes | [RiverhogActor](#s-3ec07c9031) |  |
| <a id="s-b22c159690"></a>`collection_created_at` | yes | type="string"; maxLength=30; minLength=30; pattern="^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}\\.[0-9]{9}Z$" |  |
| <a id="s-519a45279f"></a>`collection_id` | yes | [CollectionId](#s-3b7d7e3c2c) |  |
| <a id="s-5f45a158b6"></a>`context` | no | anyOf=[(type="object"; additionalProperties=(any JSON value); x-riverhog-encoded-bytes-max=4096; x-riverhog-extent={"policy":"contract_max","reason":"bounded-lifecycle-event-context"}); (type="null")]; default=null |  |
| <a id="s-1477505de9"></a>`destination_store` | yes | [ArchiveStoreName](#s-2e2a7afe9a) |  |
| <a id="s-0ed3e6943f"></a>`initiator` | yes | [RiverhogActor](#s-3ec07c9031) |  |
| <a id="s-56e091a390"></a>`source_store` | yes | [ArchiveStoreName](#s-2e2a7afe9a) |  |
| <a id="s-ae83ee15eb"></a>`state` | yes | type="string"; const="requested" |  |

##### <a id="s-08328a8233"></a>definition `ArchiveCopyJobRequestedEvent`

- <a id="s-38ddd6f421"></a>`type`: `"object"`
- <a id="s-55322f295a"></a>`additionalProperties`: `false`
- <a id="s-2f67536fc9"></a>`required`: `["id","type","occurred_at","payload"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-825a62ceda"></a>`id` | yes | type="string"; minLength=1 |  |
| <a id="s-eff8e679a9"></a>`occurred_at` | yes | type="string"; maxLength=30; minLength=30; pattern="^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}\\.[0-9]{9}Z$" |  |
| <a id="s-398e80c2ef"></a>`payload` | yes | [ArchiveCopyJobRequestedData](#s-f7f1999027) |  |
| <a id="s-e98d810109"></a>`subject` | no | anyOf=[(type="string"; minLength=1); (type="null")]; default=null |  |
| <a id="s-3558bfd0b3"></a>`type` | yes | type="string"; const="io.riverhog.riverhog.archive_copy_job.requested" |  |

##### <a id="s-2e2a7afe9a"></a>definition `ArchiveStoreName`

- <a id="s-041a7e89fa"></a>`type`: `"string"`
- <a id="s-46af0098c8"></a>`pattern`: `"^[a-z0-9]+(?:-[a-z0-9]+)*$"`

##### <a id="s-04e497d16f"></a>definition `CollectionDeletedData`

- <a id="s-8129acf144"></a>`type`: `"object"`
- <a id="s-9d79aabcf4"></a>`additionalProperties`: `false`
- <a id="s-77a0aad4f4"></a>`required`: `["actor","initiator","collection_id","collection_created_at","files","bytes","remote_storage_bytes"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-57da1f3b82"></a>`actor` | yes | [RiverhogActor](#s-3ec07c9031) |  |
| <a id="s-522d26df29"></a>`bytes` | yes | type="integer"; minimum=0 |  |
| <a id="s-8dae7de57b"></a>`collection_created_at` | yes | type="string"; maxLength=30; minLength=30; pattern="^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}\\.[0-9]{9}Z$" |  |
| <a id="s-ac2d1c2565"></a>`collection_id` | yes | [CollectionId](#s-3b7d7e3c2c) |  |
| <a id="s-30c4f9b5e9"></a>`context` | no | anyOf=[(type="object"; additionalProperties=(any JSON value); x-riverhog-encoded-bytes-max=4096; x-riverhog-extent={"policy":"contract_max","reason":"bounded-lifecycle-event-context"}); (type="null")]; default=null |  |
| <a id="s-5bc7b30fe2"></a>`files` | yes | type="integer"; minimum=0 |  |
| <a id="s-65bb659349"></a>`initiator` | yes | [RiverhogActor](#s-3ec07c9031) |  |
| <a id="s-c1bfb9d0ef"></a>`remote_storage_bytes` | yes | type="integer"; minimum=0 |  |

##### <a id="s-e18a1ebf85"></a>definition `CollectionDeletedEvent`

- <a id="s-0c3545da3d"></a>`type`: `"object"`
- <a id="s-077f0fd07b"></a>`additionalProperties`: `false`
- <a id="s-b315fc9f5e"></a>`required`: `["id","type","occurred_at","payload"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-1246c40f7b"></a>`id` | yes | type="string"; minLength=1 |  |
| <a id="s-ba7c8f9b57"></a>`occurred_at` | yes | type="string"; maxLength=30; minLength=30; pattern="^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}\\.[0-9]{9}Z$" |  |
| <a id="s-21c1496888"></a>`payload` | yes | [CollectionDeletedData](#s-04e497d16f) |  |
| <a id="s-29f8fe627a"></a>`subject` | no | anyOf=[(type="string"; minLength=1); (type="null")]; default=null |  |
| <a id="s-84a576dc14"></a>`type` | yes | type="string"; const="io.riverhog.riverhog.collection.deleted" |  |

##### <a id="s-caf8e4a999"></a>definition `CollectionFinalizedData`

- <a id="s-21b31a26c4"></a>`type`: `"object"`
- <a id="s-2afb7fe44f"></a>`additionalProperties`: `false`
- <a id="s-ebc32cd4ae"></a>`required`: `["actor","initiator","collection_id","collection_created_at","files_total","bytes_total","archive_root_sha256"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-74a74b1c5b"></a>`actor` | yes | [RiverhogActor](#s-3ec07c9031) |  |
| <a id="s-a4ae429085"></a>`archive_root_sha256` | yes | type="string"; pattern="^[0-9a-f]{64}$" |  |
| <a id="s-ae2c7918f4"></a>`bytes_total` | yes | type="integer"; minimum=0 |  |
| <a id="s-36de94ee67"></a>`collection_created_at` | yes | type="string"; maxLength=30; minLength=30; pattern="^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}\\.[0-9]{9}Z$" |  |
| <a id="s-8c1457cfa0"></a>`collection_id` | yes | [CollectionId](#s-3b7d7e3c2c) |  |
| <a id="s-6c21ec8a3d"></a>`context` | no | anyOf=[(type="object"; additionalProperties=(any JSON value); x-riverhog-encoded-bytes-max=4096; x-riverhog-extent={"policy":"contract_max","reason":"bounded-lifecycle-event-context"}); (type="null")]; default=null |  |
| <a id="s-65413559df"></a>`files_total` | yes | type="integer"; minimum=0 |  |
| <a id="s-c3ea18e378"></a>`initiator` | yes | [RiverhogActor](#s-3ec07c9031) |  |

##### <a id="s-dcb5f23831"></a>definition `CollectionFinalizedEvent`

- <a id="s-3d619a313b"></a>`type`: `"object"`
- <a id="s-bd88c00517"></a>`additionalProperties`: `false`
- <a id="s-3748804a5e"></a>`required`: `["id","type","occurred_at","payload"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-51d7b31342"></a>`id` | yes | type="string"; minLength=1 |  |
| <a id="s-85d2de574a"></a>`occurred_at` | yes | type="string"; maxLength=30; minLength=30; pattern="^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}\\.[0-9]{9}Z$" |  |
| <a id="s-dd77b8656b"></a>`payload` | yes | [CollectionFinalizedData](#s-caf8e4a999) |  |
| <a id="s-ef095a513a"></a>`subject` | no | anyOf=[(type="string"; minLength=1); (type="null")]; default=null |  |
| <a id="s-c170710dc4"></a>`type` | yes | type="string"; const="io.riverhog.riverhog.collection.finalized" |  |

##### <a id="s-3b7d7e3c2c"></a>definition `CollectionId`


###### All must match (`allOf`)

| Alternative | Schema |
|---|---|
| <a id="s-0e71aa0311"></a>1 | type="string"; pattern="^(?:0\|[1-9][0-9]{0,17}\|[1-8][0-9]{18}\|9[0-1][0-9]{17}\|92[0-1][0-9]{16}\|922[0-2][0-9]{15}\|9223[0-2][0-9]{14}\|92233[0-6][0-9]{13}\|922337[0-1][0-9]{12}\|92233720[0-2][0-9]{10}\|922337203[0-5][0-9]{9}\|9223372036[0-7][0-9]{8}\|92233720368[0-4][0-9]{7}\|922337203685[0-3][0-9]{6}\|9223372036854[0-6][0-9]{5}\|92233720368547[0-6][0-9]{4}\|922337203685477[0-4][0-9]{3}\|9223372036854775[0-7][0-9]{2}\|922337203685477580[0-6][0-9]{0}\|9223372036854775807)(?![\\s\\S])" |
| <a id="s-81247693ea"></a>2 | not=(const="0") |

##### <a id="s-99b2d14f05"></a>definition `LifecycleEventCursor`

- <a id="s-d22168b174"></a>`type`: `"string"`
- <a id="s-fd1803775d"></a>`maxLength`: `19`
- <a id="s-9e4214b970"></a>`minLength`: `1`
- <a id="s-967f676da4"></a>`pattern`: `"^(?:0\|[1-9][0-9]*)$"`

##### <a id="s-26dc5f6284"></a>definition `PrincipalId`

- <a id="s-c00aa6f1e9"></a>`type`: `"string"`
- <a id="s-bdd25d3b74"></a>`pattern`: `"^(?:[a-z0-9]+(?:-[a-z0-9]+)*\|claim:[0-9a-f]{64}\|processing:[0-9a-f]{64})$"`

##### <a id="s-8f780dd62d"></a>definition `RetrievalCanceledData`

- <a id="s-9c3dc1f93c"></a>`type`: `"object"`
- <a id="s-92ea2a30f2"></a>`additionalProperties`: `false`
- <a id="s-aa2c63a88a"></a>`required`: `["actor","initiator","retrieval_id","collection_ids","state"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-9caf87ec0f"></a>`actor` | yes | [RiverhogActor](#s-3ec07c9031) |  |
| <a id="s-17ba014898"></a>`collection_created_at` | no | anyOf=[(type="string"; maxLength=30; minLength=30; pattern="^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}\\.[0-9]{9}Z$"); (type="null")]; default=null |  |
| <a id="s-6ad3117fab"></a>`collection_id` | no | anyOf=[([CollectionId](#s-3b7d7e3c2c)); (type="null")]; default=null |  |
| <a id="s-086bd0ec77"></a>`collection_ids` | yes | type="array"; items=([CollectionId](#s-3b7d7e3c2c)); minItems=1 |  |
| <a id="s-d4094959ac"></a>`context` | no | anyOf=[(type="object"; additionalProperties=(any JSON value); x-riverhog-encoded-bytes-max=4096; x-riverhog-extent={"policy":"contract_max","reason":"bounded-lifecycle-event-context"}); (type="null")]; default=null |  |
| <a id="s-7af0933b09"></a>`initiator` | yes | [RiverhogActor](#s-3ec07c9031) |  |
| <a id="s-b67d8635ea"></a>`reason` | no | anyOf=[(type="string"; maxLength=1000; minLength=1); (type="null")]; default=null |  |
| <a id="s-23350bec21"></a>`retrieval_id` | yes | type="string"; maxLength=300; minLength=1 |  |
| <a id="s-2390813b85"></a>`state` | yes | type="string"; const="canceled" |  |

##### <a id="s-e80951e313"></a>definition `RetrievalCanceledEvent`

- <a id="s-39e95e33c4"></a>`type`: `"object"`
- <a id="s-500ea3c209"></a>`additionalProperties`: `false`
- <a id="s-fccedfe566"></a>`required`: `["id","type","occurred_at","payload"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-6e0c6be23f"></a>`id` | yes | type="string"; minLength=1 |  |
| <a id="s-f03d9b306b"></a>`occurred_at` | yes | type="string"; maxLength=30; minLength=30; pattern="^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}\\.[0-9]{9}Z$" |  |
| <a id="s-b646c092f6"></a>`payload` | yes | [RetrievalCanceledData](#s-8f780dd62d) |  |
| <a id="s-9173f5b0df"></a>`subject` | no | anyOf=[(type="string"; minLength=1); (type="null")]; default=null |  |
| <a id="s-2835564e78"></a>`type` | yes | type="string"; const="io.riverhog.riverhog.retrieval.canceled" |  |

##### <a id="s-e680525602"></a>definition `RetrievalCompletedData`

- <a id="s-6e207becee"></a>`type`: `"object"`
- <a id="s-48a748ff15"></a>`additionalProperties`: `false`
- <a id="s-327db209e8"></a>`required`: `["actor","initiator","retrieval_id","collection_ids","state"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-d633b25199"></a>`actor` | yes | [RiverhogActor](#s-3ec07c9031) |  |
| <a id="s-483430fb94"></a>`collection_created_at` | no | anyOf=[(type="string"; maxLength=30; minLength=30; pattern="^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}\\.[0-9]{9}Z$"); (type="null")]; default=null |  |
| <a id="s-4be8917304"></a>`collection_id` | no | anyOf=[([CollectionId](#s-3b7d7e3c2c)); (type="null")]; default=null |  |
| <a id="s-bb1b254e9e"></a>`collection_ids` | yes | type="array"; items=([CollectionId](#s-3b7d7e3c2c)); minItems=1 |  |
| <a id="s-dfcb1fcb34"></a>`context` | no | anyOf=[(type="object"; additionalProperties=(any JSON value); x-riverhog-encoded-bytes-max=4096; x-riverhog-extent={"policy":"contract_max","reason":"bounded-lifecycle-event-context"}); (type="null")]; default=null |  |
| <a id="s-0b7290ed8e"></a>`initiator` | yes | [RiverhogActor](#s-3ec07c9031) |  |
| <a id="s-034055a483"></a>`retrieval_id` | yes | type="string"; maxLength=300; minLength=1 |  |
| <a id="s-8e4aa554ab"></a>`state` | yes | type="string"; const="completed" |  |

##### <a id="s-e763e9511f"></a>definition `RetrievalCompletedEvent`

- <a id="s-ae7ea3d8f7"></a>`type`: `"object"`
- <a id="s-ad230f078e"></a>`additionalProperties`: `false`
- <a id="s-8e6cfd5a88"></a>`required`: `["id","type","occurred_at","payload"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-1e88b7ff6e"></a>`id` | yes | type="string"; minLength=1 |  |
| <a id="s-687d77a669"></a>`occurred_at` | yes | type="string"; maxLength=30; minLength=30; pattern="^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}\\.[0-9]{9}Z$" |  |
| <a id="s-d8dd22547a"></a>`payload` | yes | [RetrievalCompletedData](#s-e680525602) |  |
| <a id="s-101aec1767"></a>`subject` | no | anyOf=[(type="string"; minLength=1); (type="null")]; default=null |  |
| <a id="s-880eb05bbe"></a>`type` | yes | type="string"; const="io.riverhog.riverhog.retrieval.completed" |  |

##### <a id="s-901491f41d"></a>definition `RetrievalExpiredData`

- <a id="s-2fbb8f7831"></a>`type`: `"object"`
- <a id="s-b7662ed3ea"></a>`additionalProperties`: `false`
- <a id="s-f2e526214f"></a>`required`: `["actor","initiator","retrieval_id","collection_ids","state"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-ed336aa0ab"></a>`actor` | yes | [RiverhogActor](#s-3ec07c9031) |  |
| <a id="s-208b5a2bcc"></a>`collection_created_at` | no | anyOf=[(type="string"; maxLength=30; minLength=30; pattern="^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}\\.[0-9]{9}Z$"); (type="null")]; default=null |  |
| <a id="s-4fe2097994"></a>`collection_id` | no | anyOf=[([CollectionId](#s-3b7d7e3c2c)); (type="null")]; default=null |  |
| <a id="s-015acc72f6"></a>`collection_ids` | yes | type="array"; items=([CollectionId](#s-3b7d7e3c2c)); minItems=1 |  |
| <a id="s-16112840b3"></a>`context` | no | anyOf=[(type="object"; additionalProperties=(any JSON value); x-riverhog-encoded-bytes-max=4096; x-riverhog-extent={"policy":"contract_max","reason":"bounded-lifecycle-event-context"}); (type="null")]; default=null |  |
| <a id="s-a6011e4483"></a>`initiator` | yes | [RiverhogActor](#s-3ec07c9031) |  |
| <a id="s-425de01a61"></a>`retrieval_id` | yes | type="string"; maxLength=300; minLength=1 |  |
| <a id="s-e6ff329283"></a>`state` | yes | type="string"; const="expired" |  |

##### <a id="s-cd3863d78c"></a>definition `RetrievalExpiredEvent`

- <a id="s-e5aafe422b"></a>`type`: `"object"`
- <a id="s-e87bbcfefa"></a>`additionalProperties`: `false`
- <a id="s-a5ee615f80"></a>`required`: `["id","type","occurred_at","payload"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-469120a2d7"></a>`id` | yes | type="string"; minLength=1 |  |
| <a id="s-3dad2493da"></a>`occurred_at` | yes | type="string"; maxLength=30; minLength=30; pattern="^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}\\.[0-9]{9}Z$" |  |
| <a id="s-3d28991e94"></a>`payload` | yes | [RetrievalExpiredData](#s-901491f41d) |  |
| <a id="s-356d4325e2"></a>`subject` | no | anyOf=[(type="string"; minLength=1); (type="null")]; default=null |  |
| <a id="s-c9e0511c11"></a>`type` | yes | type="string"; const="io.riverhog.riverhog.retrieval.expired" |  |

##### <a id="s-c2a74431c4"></a>definition `RetrievalFailedData`

- <a id="s-d7d1f236b0"></a>`type`: `"object"`
- <a id="s-d13f4451cc"></a>`additionalProperties`: `false`
- <a id="s-8624bf85ca"></a>`required`: `["actor","initiator","retrieval_id","collection_ids","state","error"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-9e0fcb7c1f"></a>`actor` | yes | [RiverhogActor](#s-3ec07c9031) |  |
| <a id="s-68c0974906"></a>`collection_created_at` | no | anyOf=[(type="string"; maxLength=30; minLength=30; pattern="^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}\\.[0-9]{9}Z$"); (type="null")]; default=null |  |
| <a id="s-3698954897"></a>`collection_id` | no | anyOf=[([CollectionId](#s-3b7d7e3c2c)); (type="null")]; default=null |  |
| <a id="s-349277b22f"></a>`collection_ids` | yes | type="array"; items=([CollectionId](#s-3b7d7e3c2c)); minItems=1 |  |
| <a id="s-7e901ba46c"></a>`context` | no | anyOf=[(type="object"; additionalProperties=(any JSON value); x-riverhog-encoded-bytes-max=4096; x-riverhog-extent={"policy":"contract_max","reason":"bounded-lifecycle-event-context"}); (type="null")]; default=null |  |
| <a id="s-058a5b7e74"></a>`error` | yes | type="string"; maxLength=16384; minLength=1 |  |
| <a id="s-62b1da16d4"></a>`initiator` | yes | [RiverhogActor](#s-3ec07c9031) |  |
| <a id="s-4ddab02cda"></a>`retrieval_id` | yes | type="string"; maxLength=300; minLength=1 |  |
| <a id="s-4e5f4b2bed"></a>`state` | yes | type="string"; const="failed" |  |

##### <a id="s-f7876916d6"></a>definition `RetrievalFailedEvent`

- <a id="s-642cadce06"></a>`type`: `"object"`
- <a id="s-8795473cfa"></a>`additionalProperties`: `false`
- <a id="s-82dacfdf97"></a>`required`: `["id","type","occurred_at","payload"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-bb02cf5b5c"></a>`id` | yes | type="string"; minLength=1 |  |
| <a id="s-373174de1e"></a>`occurred_at` | yes | type="string"; maxLength=30; minLength=30; pattern="^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}\\.[0-9]{9}Z$" |  |
| <a id="s-e2853de264"></a>`payload` | yes | [RetrievalFailedData](#s-c2a74431c4) |  |
| <a id="s-c91911f642"></a>`subject` | no | anyOf=[(type="string"; minLength=1); (type="null")]; default=null |  |
| <a id="s-7069924984"></a>`type` | yes | type="string"; const="io.riverhog.riverhog.retrieval.failed" |  |

##### <a id="s-5d019111c7"></a>definition `RetrievalIssueData`

- <a id="s-5c147f3572"></a>`type`: `"object"`
- <a id="s-14f2570df6"></a>`additionalProperties`: `false`
- <a id="s-ad7baa4e72"></a>`required`: `["actor","initiator","retrieval_id","collection_ids","state","error"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-9c895201e2"></a>`actor` | yes | [RiverhogActor](#s-3ec07c9031) |  |
| <a id="s-fadaa0dc64"></a>`collection_created_at` | no | anyOf=[(type="string"; maxLength=30; minLength=30; pattern="^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}\\.[0-9]{9}Z$"); (type="null")]; default=null |  |
| <a id="s-6a4aeb6735"></a>`collection_id` | no | anyOf=[([CollectionId](#s-3b7d7e3c2c)); (type="null")]; default=null |  |
| <a id="s-395f72ee9d"></a>`collection_ids` | yes | type="array"; items=([CollectionId](#s-3b7d7e3c2c)); minItems=1 |  |
| <a id="s-90c00dfff4"></a>`context` | no | anyOf=[(type="object"; additionalProperties=(any JSON value); x-riverhog-encoded-bytes-max=4096; x-riverhog-extent={"policy":"contract_max","reason":"bounded-lifecycle-event-context"}); (type="null")]; default=null |  |
| <a id="s-41b89421be"></a>`error` | yes | type="string"; maxLength=16384; minLength=1 |  |
| <a id="s-ed8c877e0b"></a>`initiator` | yes | [RiverhogActor](#s-3ec07c9031) |  |
| <a id="s-4e754880bb"></a>`retrieval_id` | yes | type="string"; maxLength=300; minLength=1 |  |
| <a id="s-ffebf843e4"></a>`state` | yes | type="string"; const="requested" |  |

##### <a id="s-061dcaf609"></a>definition `RetrievalIssueEvent`

- <a id="s-049243cae4"></a>`type`: `"object"`
- <a id="s-5a12b0b4da"></a>`additionalProperties`: `false`
- <a id="s-86f0873a33"></a>`required`: `["id","type","occurred_at","payload"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-4009580dfc"></a>`id` | yes | type="string"; minLength=1 |  |
| <a id="s-b97bfa064b"></a>`occurred_at` | yes | type="string"; maxLength=30; minLength=30; pattern="^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}\\.[0-9]{9}Z$" |  |
| <a id="s-004fdce27e"></a>`payload` | yes | [RetrievalIssueData](#s-5d019111c7) |  |
| <a id="s-29dccbde08"></a>`subject` | no | anyOf=[(type="string"; minLength=1); (type="null")]; default=null |  |
| <a id="s-8467f20f19"></a>`type` | yes | type="string"; const="io.riverhog.riverhog.retrieval.issue" |  |

##### <a id="s-94276b17d8"></a>definition `RetrievalReadyData`

- <a id="s-e1eabb007b"></a>`type`: `"object"`
- <a id="s-1b2cf44f95"></a>`additionalProperties`: `false`
- <a id="s-f145937c0c"></a>`required`: `["actor","initiator","retrieval_id","collection_ids","state","expires_at"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-a42a913481"></a>`actor` | yes | [RiverhogActor](#s-3ec07c9031) |  |
| <a id="s-31ea966e9e"></a>`collection_created_at` | no | anyOf=[(type="string"; maxLength=30; minLength=30; pattern="^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}\\.[0-9]{9}Z$"); (type="null")]; default=null |  |
| <a id="s-d268e8c028"></a>`collection_id` | no | anyOf=[([CollectionId](#s-3b7d7e3c2c)); (type="null")]; default=null |  |
| <a id="s-ddd3bb8bc2"></a>`collection_ids` | yes | type="array"; items=([CollectionId](#s-3b7d7e3c2c)); minItems=1 |  |
| <a id="s-ed8038fbe3"></a>`context` | no | anyOf=[(type="object"; additionalProperties=(any JSON value); x-riverhog-encoded-bytes-max=4096; x-riverhog-extent={"policy":"contract_max","reason":"bounded-lifecycle-event-context"}); (type="null")]; default=null |  |
| <a id="s-3aa854894c"></a>`expires_at` | yes | type="string"; maxLength=30; minLength=30; pattern="^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}\\.[0-9]{9}Z$" |  |
| <a id="s-63d865e6bb"></a>`initiator` | yes | [RiverhogActor](#s-3ec07c9031) |  |
| <a id="s-d42e5eca9a"></a>`retrieval_id` | yes | type="string"; maxLength=300; minLength=1 |  |
| <a id="s-1a3f96afcd"></a>`state` | yes | type="string"; const="ready" |  |

##### <a id="s-f23d561333"></a>definition `RetrievalReadyEvent`

- <a id="s-f8df7b497b"></a>`type`: `"object"`
- <a id="s-c2992f732d"></a>`additionalProperties`: `false`
- <a id="s-d147d3d0a5"></a>`required`: `["id","type","occurred_at","payload"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-69b39dcc7b"></a>`id` | yes | type="string"; minLength=1 |  |
| <a id="s-e09a1dbba6"></a>`occurred_at` | yes | type="string"; maxLength=30; minLength=30; pattern="^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}\\.[0-9]{9}Z$" |  |
| <a id="s-0ca5fbee40"></a>`payload` | yes | [RetrievalReadyData](#s-94276b17d8) |  |
| <a id="s-e373935a4e"></a>`subject` | no | anyOf=[(type="string"; minLength=1); (type="null")]; default=null |  |
| <a id="s-c3c2577807"></a>`type` | yes | type="string"; const="io.riverhog.riverhog.retrieval.ready" |  |

##### <a id="s-d1bc1f3107"></a>definition `RetrievalRenewedData`

- <a id="s-34981f3a5d"></a>`type`: `"object"`
- <a id="s-cc5ba33e14"></a>`additionalProperties`: `false`
- <a id="s-f9c9a4d574"></a>`required`: `["actor","initiator","retrieval_id","collection_ids","state","expires_at"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-cf63af7b77"></a>`actor` | yes | [RiverhogActor](#s-3ec07c9031) |  |
| <a id="s-c0cc9568d5"></a>`collection_created_at` | no | anyOf=[(type="string"; maxLength=30; minLength=30; pattern="^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}\\.[0-9]{9}Z$"); (type="null")]; default=null |  |
| <a id="s-17488d8589"></a>`collection_id` | no | anyOf=[([CollectionId](#s-3b7d7e3c2c)); (type="null")]; default=null |  |
| <a id="s-1115efd158"></a>`collection_ids` | yes | type="array"; items=([CollectionId](#s-3b7d7e3c2c)); minItems=1 |  |
| <a id="s-90f064dc62"></a>`context` | no | anyOf=[(type="object"; additionalProperties=(any JSON value); x-riverhog-encoded-bytes-max=4096; x-riverhog-extent={"policy":"contract_max","reason":"bounded-lifecycle-event-context"}); (type="null")]; default=null |  |
| <a id="s-3b00b3651c"></a>`expires_at` | yes | type="string"; maxLength=30; minLength=30; pattern="^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}\\.[0-9]{9}Z$" |  |
| <a id="s-ee8bf80162"></a>`initiator` | yes | [RiverhogActor](#s-3ec07c9031) |  |
| <a id="s-b6de652492"></a>`retrieval_id` | yes | type="string"; maxLength=300; minLength=1 |  |
| <a id="s-4cb4ac0a11"></a>`state` | yes | type="string"; const="ready" |  |

##### <a id="s-45fb1a016c"></a>definition `RetrievalRenewedEvent`

- <a id="s-f6935ffc2f"></a>`type`: `"object"`
- <a id="s-49deb12a22"></a>`additionalProperties`: `false`
- <a id="s-ed986274fe"></a>`required`: `["id","type","occurred_at","payload"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-dd138eafd1"></a>`id` | yes | type="string"; minLength=1 |  |
| <a id="s-4fe32a8db7"></a>`occurred_at` | yes | type="string"; maxLength=30; minLength=30; pattern="^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}\\.[0-9]{9}Z$" |  |
| <a id="s-dd62a7b092"></a>`payload` | yes | [RetrievalRenewedData](#s-d1bc1f3107) |  |
| <a id="s-3c44160f20"></a>`subject` | no | anyOf=[(type="string"; minLength=1); (type="null")]; default=null |  |
| <a id="s-6a4f1e6158"></a>`type` | yes | type="string"; const="io.riverhog.riverhog.retrieval.renewed" |  |

##### <a id="s-64049bf6f9"></a>definition `RetrievalRequestedData`

- <a id="s-2a3fae73f5"></a>`type`: `"object"`
- <a id="s-c8ef2fb970"></a>`additionalProperties`: `false`
- <a id="s-5c327336af"></a>`required`: `["actor","initiator","retrieval_id","collection_ids","state","files","objects","restore_required"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-3a82345d45"></a>`actor` | yes | [RiverhogActor](#s-3ec07c9031) |  |
| <a id="s-e32c279383"></a>`collection_created_at` | no | anyOf=[(type="string"; maxLength=30; minLength=30; pattern="^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}\\.[0-9]{9}Z$"); (type="null")]; default=null |  |
| <a id="s-94c50d4b6f"></a>`collection_id` | no | anyOf=[([CollectionId](#s-3b7d7e3c2c)); (type="null")]; default=null |  |
| <a id="s-ef5fe00caf"></a>`collection_ids` | yes | type="array"; items=([CollectionId](#s-3b7d7e3c2c)); minItems=1 |  |
| <a id="s-39eaf089f3"></a>`context` | no | anyOf=[(type="object"; additionalProperties=(any JSON value); x-riverhog-encoded-bytes-max=4096; x-riverhog-extent={"policy":"contract_max","reason":"bounded-lifecycle-event-context"}); (type="null")]; default=null |  |
| <a id="s-87cabb3b1a"></a>`files` | yes | type="integer"; minimum=1 |  |
| <a id="s-972078372d"></a>`initiator` | yes | [RiverhogActor](#s-3ec07c9031) |  |
| <a id="s-838367382a"></a>`objects` | yes | type="integer"; minimum=1 |  |
| <a id="s-5ee2cbf755"></a>`restore_required` | yes | type="boolean" |  |
| <a id="s-5c037d7f30"></a>`retrieval_id` | yes | type="string"; maxLength=300; minLength=1 |  |
| <a id="s-403b6293bd"></a>`state` | yes | type="string"; enum=["requested","ready"] |  |

##### <a id="s-9dad1abe42"></a>definition `RetrievalRequestedEvent`

- <a id="s-c2fba1c2a5"></a>`type`: `"object"`
- <a id="s-ce16f3d4b3"></a>`additionalProperties`: `false`
- <a id="s-c306891d1a"></a>`required`: `["id","type","occurred_at","payload"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-381822e8b4"></a>`id` | yes | type="string"; minLength=1 |  |
| <a id="s-45e5cc4fc4"></a>`occurred_at` | yes | type="string"; maxLength=30; minLength=30; pattern="^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}\\.[0-9]{9}Z$" |  |
| <a id="s-92c6054ab8"></a>`payload` | yes | [RetrievalRequestedData](#s-64049bf6f9) |  |
| <a id="s-1e9333c620"></a>`subject` | no | anyOf=[(type="string"; minLength=1); (type="null")]; default=null |  |
| <a id="s-52578520a5"></a>`type` | yes | type="string"; const="io.riverhog.riverhog.retrieval.requested" |  |

##### <a id="s-3ec07c9031"></a>definition `RiverhogActor`

- <a id="s-5670f78f7d"></a>`type`: `"object"`
- <a id="s-268c8df8e0"></a>`additionalProperties`: `false`
- <a id="s-c53cac5a10"></a>`required`: `["principal_id"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-87ad259a07"></a>`key_id` | no | anyOf=[(type="string"; maxLength=300; minLength=1); (type="null")]; default=null |  |
| <a id="s-747b21f7c9"></a>`principal_id` | yes | [PrincipalId](#s-26dc5f6284); maxLength=160 |  |

##### <a id="s-41ab5ca15a"></a>definition `RiverhogLifecycleEvent`

- <a id="s-2661c0e417"></a>`discriminator`: `{"mapping":{"io.riverhog.riverhog.archive_copy_job.canceled":"#/$defs/ArchiveCopyJobCanceledEvent","io.riverhog.riverhog.archive_copy_job.completed":"#/$defs/ArchiveCopyJobCompletedEvent","io.riverhog.riverhog.archive_copy_job.failed":"#/$defs/ArchiveCopyJobFailedEvent","io.riverhog.riverhog.archive_copy_job.requested":"#/$defs/ArchiveCopyJobRequestedEvent","io.riverhog.riverhog.collection.deleted":"#/$defs/CollectionDeletedEvent","io.riverhog.riverhog.collection.finalized":"#/$defs/CollectionFinalizedEvent","io.riverhog.riverhog.retrieval.canceled":"#/$defs/RetrievalCanceledEvent","io.riverhog.riverhog.retrieval.completed":"#/$defs/RetrievalCompletedEvent","io.riverhog.riverhog.retrieval.expired":"#/$defs/RetrievalExpiredEvent","io.riverhog.riverhog.retrieval.failed":"#/$defs/RetrievalFailedEvent","io.riverhog.riverhog.retrieval.issue":"#/$defs/RetrievalIssueEvent","io.riverhog.riverhog.retrieval.ready":"#/$defs/RetrievalReadyEvent","io.riverhog.riverhog.retrieval.renewed":"#/$defs/RetrievalRenewedEvent","io.riverhog.riverhog.retrieval.requested":"#/$defs/RetrievalRequestedEvent"},"propertyName":"type"}`

###### Exactly one must match (`oneOf`)

| Alternative | Schema |
|---|---|
| <a id="s-957bd4c2c3"></a>1 | [CollectionFinalizedEvent](#s-dcb5f23831) |
| <a id="s-ec5169534a"></a>2 | [CollectionDeletedEvent](#s-e18a1ebf85) |
| <a id="s-41b9e81340"></a>3 | [ArchiveCopyJobRequestedEvent](#s-08328a8233) |
| <a id="s-8980bd2c00"></a>4 | [ArchiveCopyJobCompletedEvent](#s-d305e985f9) |
| <a id="s-a2ebed2009"></a>5 | [ArchiveCopyJobFailedEvent](#s-ddec468949) |
| <a id="s-543be3c82c"></a>6 | [ArchiveCopyJobCanceledEvent](#s-31a0f3b0ea) |
| <a id="s-d9d60a9507"></a>7 | [RetrievalRequestedEvent](#s-9dad1abe42) |
| <a id="s-9d3126212a"></a>8 | [RetrievalReadyEvent](#s-f23d561333) |
| <a id="s-b372983489"></a>9 | [RetrievalRenewedEvent](#s-45fb1a016c) |
| <a id="s-0772b6833c"></a>10 | [RetrievalCompletedEvent](#s-e763e9511f) |
| <a id="s-7d9414c8a6"></a>11 | [RetrievalCanceledEvent](#s-e80951e313) |
| <a id="s-b8f45a7fd2"></a>12 | [RetrievalExpiredEvent](#s-cd3863d78c) |
| <a id="s-4c3c8a7388"></a>13 | [RetrievalIssueEvent](#s-061dcaf609) |
| <a id="s-38de734469"></a>14 | [RetrievalFailedEvent](#s-f7876916d6) |

## Maintained corroboration

### Related interface records

- [get](riverhog-protocol-riverhogeventpage-get.md)
- [__getitem__](riverhog-protocol-riverhogeventpage-getitem.md)
- [require_progress_after](riverhog-protocol-riverhogeventpage-require-progress-after.md)

## Governing policies

- <a id="pa-962d15f316"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:riverhog-protocol:riverhog_protocol](../../../evidence/sources/authorities.md#src-19e35f15d9) — [packages/riverhog-protocol/src/riverhog\_protocol/\_\_init\_\_.py](../../../../../../packages/riverhog-protocol/src/riverhog_protocol/__init__.py)

### Machine authority

- `/external_contract/python/riverhog_protocol.RiverhogEventPage`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 5f7592c130afb535f3872bf62d58297506b4acaa9ad2ef94cf653c5649a4b6d3 -->

```json
{
  "contract": {
    "kind": "class",
    "schema": {
      "$defs": {
        "ArchiveCopyJobCanceledData": {
          "additionalProperties": false,
          "properties": {
            "actor": {
              "$ref": "#/$defs/RiverhogActor"
            },
            "collection_created_at": {
              "maxLength": 30,
              "minLength": 30,
              "pattern": "^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}\\.[0-9]{9}Z$",
              "type": "string"
            },
            "collection_id": {
              "$ref": "#/$defs/CollectionId"
            },
            "context": {
              "anyOf": [
                {
                  "additionalProperties": true,
                  "type": "object",
                  "x-riverhog-encoded-bytes-max": 4096,
                  "x-riverhog-extent": {
                    "policy": "contract_max",
                    "reason": "bounded-lifecycle-event-context"
                  }
                },
                {
                  "type": "null"
                }
              ],
              "default": null
            },
            "destination_store": {
              "$ref": "#/$defs/ArchiveStoreName"
            },
            "initiator": {
              "$ref": "#/$defs/RiverhogActor"
            },
            "source_store": {
              "$ref": "#/$defs/ArchiveStoreName"
            },
            "state": {
              "const": "canceled",
              "type": "string"
            }
          },
          "required": [
            "actor",
            "initiator",
            "collection_id",
            "collection_created_at",
            "source_store",
            "destination_store",
            "state"
          ],
          "type": "object"
        },
        "ArchiveCopyJobCanceledEvent": {
          "additionalProperties": false,
          "properties": {
            "id": {
              "minLength": 1,
              "type": "string"
            },
            "occurred_at": {
              "maxLength": 30,
              "minLength": 30,
              "pattern": "^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}\\.[0-9]{9}Z$",
              "type": "string"
            },
            "payload": {
              "$ref": "#/$defs/ArchiveCopyJobCanceledData"
            },
            "subject": {
              "anyOf": [
                {
                  "minLength": 1,
                  "type": "string"
                },
                {
                  "type": "null"
                }
              ],
              "default": null
            },
            "type": {
              "const": "io.riverhog.riverhog.archive_copy_job.canceled",
              "type": "string"
            }
          },
          "required": [
            "id",
            "type",
            "occurred_at",
            "payload"
          ],
          "type": "object"
        },
        "ArchiveCopyJobCompletedData": {
          "additionalProperties": false,
          "properties": {
            "actor": {
              "$ref": "#/$defs/RiverhogActor"
            },
            "collection_created_at": {
              "maxLength": 30,
              "minLength": 30,
              "pattern": "^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}\\.[0-9]{9}Z$",
              "type": "string"
            },
            "collection_id": {
              "$ref": "#/$defs/CollectionId"
            },
            "context": {
              "anyOf": [
                {
                  "additionalProperties": true,
                  "type": "object",
                  "x-riverhog-encoded-bytes-max": 4096,
                  "x-riverhog-extent": {
                    "policy": "contract_max",
                    "reason": "bounded-lifecycle-event-context"
                  }
                },
                {
                  "type": "null"
                }
              ],
              "default": null
            },
            "destination_store": {
              "$ref": "#/$defs/ArchiveStoreName"
            },
            "initiator": {
              "$ref": "#/$defs/RiverhogActor"
            },
            "source_store": {
              "$ref": "#/$defs/ArchiveStoreName"
            },
            "state": {
              "const": "completed",
              "type": "string"
            }
          },
          "required": [
            "actor",
            "initiator",
            "collection_id",
            "collection_created_at",
            "source_store",
            "destination_store",
            "state"
          ],
          "type": "object"
        },
        "ArchiveCopyJobCompletedEvent": {
          "additionalProperties": false,
          "properties": {
            "id": {
              "minLength": 1,
              "type": "string"
            },
            "occurred_at": {
              "maxLength": 30,
              "minLength": 30,
              "pattern": "^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}\\.[0-9]{9}Z$",
              "type": "string"
            },
            "payload": {
              "$ref": "#/$defs/ArchiveCopyJobCompletedData"
            },
            "subject": {
              "anyOf": [
                {
                  "minLength": 1,
                  "type": "string"
                },
                {
                  "type": "null"
                }
              ],
              "default": null
            },
            "type": {
              "const": "io.riverhog.riverhog.archive_copy_job.completed",
              "type": "string"
            }
          },
          "required": [
            "id",
            "type",
            "occurred_at",
            "payload"
          ],
          "type": "object"
        },
        "ArchiveCopyJobFailedData": {
          "additionalProperties": false,
          "properties": {
            "actor": {
              "$ref": "#/$defs/RiverhogActor"
            },
            "collection_created_at": {
              "maxLength": 30,
              "minLength": 30,
              "pattern": "^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}\\.[0-9]{9}Z$",
              "type": "string"
            },
            "collection_id": {
              "$ref": "#/$defs/CollectionId"
            },
            "context": {
              "anyOf": [
                {
                  "additionalProperties": true,
                  "type": "object",
                  "x-riverhog-encoded-bytes-max": 4096,
                  "x-riverhog-extent": {
                    "policy": "contract_max",
                    "reason": "bounded-lifecycle-event-context"
                  }
                },
                {
                  "type": "null"
                }
              ],
              "default": null
            },
            "destination_store": {
              "$ref": "#/$defs/ArchiveStoreName"
            },
            "error": {
              "maxLength": 16384,
              "minLength": 1,
              "type": "string"
            },
            "initiator": {
              "$ref": "#/$defs/RiverhogActor"
            },
            "source_store": {
              "$ref": "#/$defs/ArchiveStoreName"
            },
            "state": {
              "const": "failed",
              "type": "string"
            }
          },
          "required": [
            "actor",
            "initiator",
            "collection_id",
            "collection_created_at",
            "source_store",
            "destination_store",
            "state",
            "error"
          ],
          "type": "object"
        },
        "ArchiveCopyJobFailedEvent": {
          "additionalProperties": false,
          "properties": {
            "id": {
              "minLength": 1,
              "type": "string"
            },
            "occurred_at": {
              "maxLength": 30,
              "minLength": 30,
              "pattern": "^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}\\.[0-9]{9}Z$",
              "type": "string"
            },
            "payload": {
              "$ref": "#/$defs/ArchiveCopyJobFailedData"
            },
            "subject": {
              "anyOf": [
                {
                  "minLength": 1,
                  "type": "string"
                },
                {
                  "type": "null"
                }
              ],
              "default": null
            },
            "type": {
              "const": "io.riverhog.riverhog.archive_copy_job.failed",
              "type": "string"
            }
          },
          "required": [
            "id",
            "type",
            "occurred_at",
            "payload"
          ],
          "type": "object"
        },
        "ArchiveCopyJobRequestedData": {
          "additionalProperties": false,
          "properties": {
            "actor": {
              "$ref": "#/$defs/RiverhogActor"
            },
            "collection_created_at": {
              "maxLength": 30,
              "minLength": 30,
              "pattern": "^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}\\.[0-9]{9}Z$",
              "type": "string"
            },
            "collection_id": {
              "$ref": "#/$defs/CollectionId"
            },
            "context": {
              "anyOf": [
                {
                  "additionalProperties": true,
                  "type": "object",
                  "x-riverhog-encoded-bytes-max": 4096,
                  "x-riverhog-extent": {
                    "policy": "contract_max",
                    "reason": "bounded-lifecycle-event-context"
                  }
                },
                {
                  "type": "null"
                }
              ],
              "default": null
            },
            "destination_store": {
              "$ref": "#/$defs/ArchiveStoreName"
            },
            "initiator": {
              "$ref": "#/$defs/RiverhogActor"
            },
            "source_store": {
              "$ref": "#/$defs/ArchiveStoreName"
            },
            "state": {
              "const": "requested",
              "type": "string"
            }
          },
          "required": [
            "actor",
            "initiator",
            "collection_id",
            "collection_created_at",
            "source_store",
            "destination_store",
            "state"
          ],
          "type": "object"
        },
        "ArchiveCopyJobRequestedEvent": {
          "additionalProperties": false,
          "properties": {
            "id": {
              "minLength": 1,
              "type": "string"
            },
            "occurred_at": {
              "maxLength": 30,
              "minLength": 30,
              "pattern": "^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}\\.[0-9]{9}Z$",
              "type": "string"
            },
            "payload": {
              "$ref": "#/$defs/ArchiveCopyJobRequestedData"
            },
            "subject": {
              "anyOf": [
                {
                  "minLength": 1,
                  "type": "string"
                },
                {
                  "type": "null"
                }
              ],
              "default": null
            },
            "type": {
              "const": "io.riverhog.riverhog.archive_copy_job.requested",
              "type": "string"
            }
          },
          "required": [
            "id",
            "type",
            "occurred_at",
            "payload"
          ],
          "type": "object"
        },
        "ArchiveStoreName": {
          "pattern": "^[a-z0-9]+(?:-[a-z0-9]+)*$",
          "type": "string"
        },
        "CollectionDeletedData": {
          "additionalProperties": false,
          "properties": {
            "actor": {
              "$ref": "#/$defs/RiverhogActor"
            },
            "bytes": {
              "minimum": 0,
              "type": "integer"
            },
            "collection_created_at": {
              "maxLength": 30,
              "minLength": 30,
              "pattern": "^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}\\.[0-9]{9}Z$",
              "type": "string"
            },
            "collection_id": {
              "$ref": "#/$defs/CollectionId"
            },
            "context": {
              "anyOf": [
                {
                  "additionalProperties": true,
                  "type": "object",
                  "x-riverhog-encoded-bytes-max": 4096,
                  "x-riverhog-extent": {
                    "policy": "contract_max",
                    "reason": "bounded-lifecycle-event-context"
                  }
                },
                {
                  "type": "null"
                }
              ],
              "default": null
            },
            "files": {
              "minimum": 0,
              "type": "integer"
            },
            "initiator": {
              "$ref": "#/$defs/RiverhogActor"
            },
            "remote_storage_bytes": {
              "minimum": 0,
              "type": "integer"
            }
          },
          "required": [
            "actor",
            "initiator",
            "collection_id",
            "collection_created_at",
            "files",
            "bytes",
            "remote_storage_bytes"
          ],
          "type": "object"
        },
        "CollectionDeletedEvent": {
          "additionalProperties": false,
          "properties": {
            "id": {
              "minLength": 1,
              "type": "string"
            },
            "occurred_at": {
              "maxLength": 30,
              "minLength": 30,
              "pattern": "^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}\\.[0-9]{9}Z$",
              "type": "string"
            },
            "payload": {
              "$ref": "#/$defs/CollectionDeletedData"
            },
            "subject": {
              "anyOf": [
                {
                  "minLength": 1,
                  "type": "string"
                },
                {
                  "type": "null"
                }
              ],
              "default": null
            },
            "type": {
              "const": "io.riverhog.riverhog.collection.deleted",
              "type": "string"
            }
          },
          "required": [
            "id",
            "type",
            "occurred_at",
            "payload"
          ],
          "type": "object"
        },
        "CollectionFinalizedData": {
          "additionalProperties": false,
          "properties": {
            "actor": {
              "$ref": "#/$defs/RiverhogActor"
            },
            "archive_root_sha256": {
              "pattern": "^[0-9a-f]{64}$",
              "type": "string"
            },
            "bytes_total": {
              "minimum": 0,
              "type": "integer"
            },
            "collection_created_at": {
              "maxLength": 30,
              "minLength": 30,
              "pattern": "^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}\\.[0-9]{9}Z$",
              "type": "string"
            },
            "collection_id": {
              "$ref": "#/$defs/CollectionId"
            },
            "context": {
              "anyOf": [
                {
                  "additionalProperties": true,
                  "type": "object",
                  "x-riverhog-encoded-bytes-max": 4096,
                  "x-riverhog-extent": {
                    "policy": "contract_max",
                    "reason": "bounded-lifecycle-event-context"
                  }
                },
                {
                  "type": "null"
                }
              ],
              "default": null
            },
            "files_total": {
              "minimum": 0,
              "type": "integer"
            },
            "initiator": {
              "$ref": "#/$defs/RiverhogActor"
            }
          },
          "required": [
            "actor",
            "initiator",
            "collection_id",
            "collection_created_at",
            "files_total",
            "bytes_total",
            "archive_root_sha256"
          ],
          "type": "object"
        },
        "CollectionFinalizedEvent": {
          "additionalProperties": false,
          "properties": {
            "id": {
              "minLength": 1,
              "type": "string"
            },
            "occurred_at": {
              "maxLength": 30,
              "minLength": 30,
              "pattern": "^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}\\.[0-9]{9}Z$",
              "type": "string"
            },
            "payload": {
              "$ref": "#/$defs/CollectionFinalizedData"
            },
            "subject": {
              "anyOf": [
                {
                  "minLength": 1,
                  "type": "string"
                },
                {
                  "type": "null"
                }
              ],
              "default": null
            },
            "type": {
              "const": "io.riverhog.riverhog.collection.finalized",
              "type": "string"
            }
          },
          "required": [
            "id",
            "type",
            "occurred_at",
            "payload"
          ],
          "type": "object"
        },
        "CollectionId": {
          "allOf": [
            {
              "pattern": "^(?:0|[1-9][0-9]{0,17}|[1-8][0-9]{18}|9[0-1][0-9]{17}|92[0-1][0-9]{16}|922[0-2][0-9]{15}|9223[0-2][0-9]{14}|92233[0-6][0-9]{13}|922337[0-1][0-9]{12}|92233720[0-2][0-9]{10}|922337203[0-5][0-9]{9}|9223372036[0-7][0-9]{8}|92233720368[0-4][0-9]{7}|922337203685[0-3][0-9]{6}|9223372036854[0-6][0-9]{5}|92233720368547[0-6][0-9]{4}|922337203685477[0-4][0-9]{3}|9223372036854775[0-7][0-9]{2}|922337203685477580[0-6][0-9]{0}|9223372036854775807)(?![\\s\\S])",
              "type": "string"
            },
            {
              "not": {
                "const": "0"
              }
            }
          ]
        },
        "LifecycleEventCursor": {
          "maxLength": 19,
          "minLength": 1,
          "pattern": "^(?:0|[1-9][0-9]*)$",
          "type": "string"
        },
        "PrincipalId": {
          "pattern": "^(?:[a-z0-9]+(?:-[a-z0-9]+)*|claim:[0-9a-f]{64}|processing:[0-9a-f]{64})$",
          "type": "string"
        },
        "RetrievalCanceledData": {
          "additionalProperties": false,
          "properties": {
            "actor": {
              "$ref": "#/$defs/RiverhogActor"
            },
            "collection_created_at": {
              "anyOf": [
                {
                  "maxLength": 30,
                  "minLength": 30,
                  "pattern": "^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}\\.[0-9]{9}Z$",
                  "type": "string"
                },
                {
                  "type": "null"
                }
              ],
              "default": null
            },
            "collection_id": {
              "anyOf": [
                {
                  "$ref": "#/$defs/CollectionId"
                },
                {
                  "type": "null"
                }
              ],
              "default": null
            },
            "collection_ids": {
              "items": {
                "$ref": "#/$defs/CollectionId"
              },
              "minItems": 1,
              "type": "array"
            },
            "context": {
              "anyOf": [
                {
                  "additionalProperties": true,
                  "type": "object",
                  "x-riverhog-encoded-bytes-max": 4096,
                  "x-riverhog-extent": {
                    "policy": "contract_max",
                    "reason": "bounded-lifecycle-event-context"
                  }
                },
                {
                  "type": "null"
                }
              ],
              "default": null
            },
            "initiator": {
              "$ref": "#/$defs/RiverhogActor"
            },
            "reason": {
              "anyOf": [
                {
                  "maxLength": 1000,
                  "minLength": 1,
                  "type": "string"
                },
                {
                  "type": "null"
                }
              ],
              "default": null
            },
            "retrieval_id": {
              "maxLength": 300,
              "minLength": 1,
              "type": "string"
            },
            "state": {
              "const": "canceled",
              "type": "string"
            }
          },
          "required": [
            "actor",
            "initiator",
            "retrieval_id",
            "collection_ids",
            "state"
          ],
          "type": "object"
        },
        "RetrievalCanceledEvent": {
          "additionalProperties": false,
          "properties": {
            "id": {
              "minLength": 1,
              "type": "string"
            },
            "occurred_at": {
              "maxLength": 30,
              "minLength": 30,
              "pattern": "^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}\\.[0-9]{9}Z$",
              "type": "string"
            },
            "payload": {
              "$ref": "#/$defs/RetrievalCanceledData"
            },
            "subject": {
              "anyOf": [
                {
                  "minLength": 1,
                  "type": "string"
                },
                {
                  "type": "null"
                }
              ],
              "default": null
            },
            "type": {
              "const": "io.riverhog.riverhog.retrieval.canceled",
              "type": "string"
            }
          },
          "required": [
            "id",
            "type",
            "occurred_at",
            "payload"
          ],
          "type": "object"
        },
        "RetrievalCompletedData": {
          "additionalProperties": false,
          "properties": {
            "actor": {
              "$ref": "#/$defs/RiverhogActor"
            },
            "collection_created_at": {
              "anyOf": [
                {
                  "maxLength": 30,
                  "minLength": 30,
                  "pattern": "^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}\\.[0-9]{9}Z$",
                  "type": "string"
                },
                {
                  "type": "null"
                }
              ],
              "default": null
            },
            "collection_id": {
              "anyOf": [
                {
                  "$ref": "#/$defs/CollectionId"
                },
                {
                  "type": "null"
                }
              ],
              "default": null
            },
            "collection_ids": {
              "items": {
                "$ref": "#/$defs/CollectionId"
              },
              "minItems": 1,
              "type": "array"
            },
            "context": {
              "anyOf": [
                {
                  "additionalProperties": true,
                  "type": "object",
                  "x-riverhog-encoded-bytes-max": 4096,
                  "x-riverhog-extent": {
                    "policy": "contract_max",
                    "reason": "bounded-lifecycle-event-context"
                  }
                },
                {
                  "type": "null"
                }
              ],
              "default": null
            },
            "initiator": {
              "$ref": "#/$defs/RiverhogActor"
            },
            "retrieval_id": {
              "maxLength": 300,
              "minLength": 1,
              "type": "string"
            },
            "state": {
              "const": "completed",
              "type": "string"
            }
          },
          "required": [
            "actor",
            "initiator",
            "retrieval_id",
            "collection_ids",
            "state"
          ],
          "type": "object"
        },
        "RetrievalCompletedEvent": {
          "additionalProperties": false,
          "properties": {
            "id": {
              "minLength": 1,
              "type": "string"
            },
            "occurred_at": {
              "maxLength": 30,
              "minLength": 30,
              "pattern": "^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}\\.[0-9]{9}Z$",
              "type": "string"
            },
            "payload": {
              "$ref": "#/$defs/RetrievalCompletedData"
            },
            "subject": {
              "anyOf": [
                {
                  "minLength": 1,
                  "type": "string"
                },
                {
                  "type": "null"
                }
              ],
              "default": null
            },
            "type": {
              "const": "io.riverhog.riverhog.retrieval.completed",
              "type": "string"
            }
          },
          "required": [
            "id",
            "type",
            "occurred_at",
            "payload"
          ],
          "type": "object"
        },
        "RetrievalExpiredData": {
          "additionalProperties": false,
          "properties": {
            "actor": {
              "$ref": "#/$defs/RiverhogActor"
            },
            "collection_created_at": {
              "anyOf": [
                {
                  "maxLength": 30,
                  "minLength": 30,
                  "pattern": "^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}\\.[0-9]{9}Z$",
                  "type": "string"
                },
                {
                  "type": "null"
                }
              ],
              "default": null
            },
            "collection_id": {
              "anyOf": [
                {
                  "$ref": "#/$defs/CollectionId"
                },
                {
                  "type": "null"
                }
              ],
              "default": null
            },
            "collection_ids": {
              "items": {
                "$ref": "#/$defs/CollectionId"
              },
              "minItems": 1,
              "type": "array"
            },
            "context": {
              "anyOf": [
                {
                  "additionalProperties": true,
                  "type": "object",
                  "x-riverhog-encoded-bytes-max": 4096,
                  "x-riverhog-extent": {
                    "policy": "contract_max",
                    "reason": "bounded-lifecycle-event-context"
                  }
                },
                {
                  "type": "null"
                }
              ],
              "default": null
            },
            "initiator": {
              "$ref": "#/$defs/RiverhogActor"
            },
            "retrieval_id": {
              "maxLength": 300,
              "minLength": 1,
              "type": "string"
            },
            "state": {
              "const": "expired",
              "type": "string"
            }
          },
          "required": [
            "actor",
            "initiator",
            "retrieval_id",
            "collection_ids",
            "state"
          ],
          "type": "object"
        },
        "RetrievalExpiredEvent": {
          "additionalProperties": false,
          "properties": {
            "id": {
              "minLength": 1,
              "type": "string"
            },
            "occurred_at": {
              "maxLength": 30,
              "minLength": 30,
              "pattern": "^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}\\.[0-9]{9}Z$",
              "type": "string"
            },
            "payload": {
              "$ref": "#/$defs/RetrievalExpiredData"
            },
            "subject": {
              "anyOf": [
                {
                  "minLength": 1,
                  "type": "string"
                },
                {
                  "type": "null"
                }
              ],
              "default": null
            },
            "type": {
              "const": "io.riverhog.riverhog.retrieval.expired",
              "type": "string"
            }
          },
          "required": [
            "id",
            "type",
            "occurred_at",
            "payload"
          ],
          "type": "object"
        },
        "RetrievalFailedData": {
          "additionalProperties": false,
          "properties": {
            "actor": {
              "$ref": "#/$defs/RiverhogActor"
            },
            "collection_created_at": {
              "anyOf": [
                {
                  "maxLength": 30,
                  "minLength": 30,
                  "pattern": "^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}\\.[0-9]{9}Z$",
                  "type": "string"
                },
                {
                  "type": "null"
                }
              ],
              "default": null
            },
            "collection_id": {
              "anyOf": [
                {
                  "$ref": "#/$defs/CollectionId"
                },
                {
                  "type": "null"
                }
              ],
              "default": null
            },
            "collection_ids": {
              "items": {
                "$ref": "#/$defs/CollectionId"
              },
              "minItems": 1,
              "type": "array"
            },
            "context": {
              "anyOf": [
                {
                  "additionalProperties": true,
                  "type": "object",
                  "x-riverhog-encoded-bytes-max": 4096,
                  "x-riverhog-extent": {
                    "policy": "contract_max",
                    "reason": "bounded-lifecycle-event-context"
                  }
                },
                {
                  "type": "null"
                }
              ],
              "default": null
            },
            "error": {
              "maxLength": 16384,
              "minLength": 1,
              "type": "string"
            },
            "initiator": {
              "$ref": "#/$defs/RiverhogActor"
            },
            "retrieval_id": {
              "maxLength": 300,
              "minLength": 1,
              "type": "string"
            },
            "state": {
              "const": "failed",
              "type": "string"
            }
          },
          "required": [
            "actor",
            "initiator",
            "retrieval_id",
            "collection_ids",
            "state",
            "error"
          ],
          "type": "object"
        },
        "RetrievalFailedEvent": {
          "additionalProperties": false,
          "properties": {
            "id": {
              "minLength": 1,
              "type": "string"
            },
            "occurred_at": {
              "maxLength": 30,
              "minLength": 30,
              "pattern": "^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}\\.[0-9]{9}Z$",
              "type": "string"
            },
            "payload": {
              "$ref": "#/$defs/RetrievalFailedData"
            },
            "subject": {
              "anyOf": [
                {
                  "minLength": 1,
                  "type": "string"
                },
                {
                  "type": "null"
                }
              ],
              "default": null
            },
            "type": {
              "const": "io.riverhog.riverhog.retrieval.failed",
              "type": "string"
            }
          },
          "required": [
            "id",
            "type",
            "occurred_at",
            "payload"
          ],
          "type": "object"
        },
        "RetrievalIssueData": {
          "additionalProperties": false,
          "properties": {
            "actor": {
              "$ref": "#/$defs/RiverhogActor"
            },
            "collection_created_at": {
              "anyOf": [
                {
                  "maxLength": 30,
                  "minLength": 30,
                  "pattern": "^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}\\.[0-9]{9}Z$",
                  "type": "string"
                },
                {
                  "type": "null"
                }
              ],
              "default": null
            },
            "collection_id": {
              "anyOf": [
                {
                  "$ref": "#/$defs/CollectionId"
                },
                {
                  "type": "null"
                }
              ],
              "default": null
            },
            "collection_ids": {
              "items": {
                "$ref": "#/$defs/CollectionId"
              },
              "minItems": 1,
              "type": "array"
            },
            "context": {
              "anyOf": [
                {
                  "additionalProperties": true,
                  "type": "object",
                  "x-riverhog-encoded-bytes-max": 4096,
                  "x-riverhog-extent": {
                    "policy": "contract_max",
                    "reason": "bounded-lifecycle-event-context"
                  }
                },
                {
                  "type": "null"
                }
              ],
              "default": null
            },
            "error": {
              "maxLength": 16384,
              "minLength": 1,
              "type": "string"
            },
            "initiator": {
              "$ref": "#/$defs/RiverhogActor"
            },
            "retrieval_id": {
              "maxLength": 300,
              "minLength": 1,
              "type": "string"
            },
            "state": {
              "const": "requested",
              "type": "string"
            }
          },
          "required": [
            "actor",
            "initiator",
            "retrieval_id",
            "collection_ids",
            "state",
            "error"
          ],
          "type": "object"
        },
        "RetrievalIssueEvent": {
          "additionalProperties": false,
          "properties": {
            "id": {
              "minLength": 1,
              "type": "string"
            },
            "occurred_at": {
              "maxLength": 30,
              "minLength": 30,
              "pattern": "^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}\\.[0-9]{9}Z$",
              "type": "string"
            },
            "payload": {
              "$ref": "#/$defs/RetrievalIssueData"
            },
            "subject": {
              "anyOf": [
                {
                  "minLength": 1,
                  "type": "string"
                },
                {
                  "type": "null"
                }
              ],
              "default": null
            },
            "type": {
              "const": "io.riverhog.riverhog.retrieval.issue",
              "type": "string"
            }
          },
          "required": [
            "id",
            "type",
            "occurred_at",
            "payload"
          ],
          "type": "object"
        },
        "RetrievalReadyData": {
          "additionalProperties": false,
          "properties": {
            "actor": {
              "$ref": "#/$defs/RiverhogActor"
            },
            "collection_created_at": {
              "anyOf": [
                {
                  "maxLength": 30,
                  "minLength": 30,
                  "pattern": "^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}\\.[0-9]{9}Z$",
                  "type": "string"
                },
                {
                  "type": "null"
                }
              ],
              "default": null
            },
            "collection_id": {
              "anyOf": [
                {
                  "$ref": "#/$defs/CollectionId"
                },
                {
                  "type": "null"
                }
              ],
              "default": null
            },
            "collection_ids": {
              "items": {
                "$ref": "#/$defs/CollectionId"
              },
              "minItems": 1,
              "type": "array"
            },
            "context": {
              "anyOf": [
                {
                  "additionalProperties": true,
                  "type": "object",
                  "x-riverhog-encoded-bytes-max": 4096,
                  "x-riverhog-extent": {
                    "policy": "contract_max",
                    "reason": "bounded-lifecycle-event-context"
                  }
                },
                {
                  "type": "null"
                }
              ],
              "default": null
            },
            "expires_at": {
              "maxLength": 30,
              "minLength": 30,
              "pattern": "^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}\\.[0-9]{9}Z$",
              "type": "string"
            },
            "initiator": {
              "$ref": "#/$defs/RiverhogActor"
            },
            "retrieval_id": {
              "maxLength": 300,
              "minLength": 1,
              "type": "string"
            },
            "state": {
              "const": "ready",
              "type": "string"
            }
          },
          "required": [
            "actor",
            "initiator",
            "retrieval_id",
            "collection_ids",
            "state",
            "expires_at"
          ],
          "type": "object"
        },
        "RetrievalReadyEvent": {
          "additionalProperties": false,
          "properties": {
            "id": {
              "minLength": 1,
              "type": "string"
            },
            "occurred_at": {
              "maxLength": 30,
              "minLength": 30,
              "pattern": "^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}\\.[0-9]{9}Z$",
              "type": "string"
            },
            "payload": {
              "$ref": "#/$defs/RetrievalReadyData"
            },
            "subject": {
              "anyOf": [
                {
                  "minLength": 1,
                  "type": "string"
                },
                {
                  "type": "null"
                }
              ],
              "default": null
            },
            "type": {
              "const": "io.riverhog.riverhog.retrieval.ready",
              "type": "string"
            }
          },
          "required": [
            "id",
            "type",
            "occurred_at",
            "payload"
          ],
          "type": "object"
        },
        "RetrievalRenewedData": {
          "additionalProperties": false,
          "properties": {
            "actor": {
              "$ref": "#/$defs/RiverhogActor"
            },
            "collection_created_at": {
              "anyOf": [
                {
                  "maxLength": 30,
                  "minLength": 30,
                  "pattern": "^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}\\.[0-9]{9}Z$",
                  "type": "string"
                },
                {
                  "type": "null"
                }
              ],
              "default": null
            },
            "collection_id": {
              "anyOf": [
                {
                  "$ref": "#/$defs/CollectionId"
                },
                {
                  "type": "null"
                }
              ],
              "default": null
            },
            "collection_ids": {
              "items": {
                "$ref": "#/$defs/CollectionId"
              },
              "minItems": 1,
              "type": "array"
            },
            "context": {
              "anyOf": [
                {
                  "additionalProperties": true,
                  "type": "object",
                  "x-riverhog-encoded-bytes-max": 4096,
                  "x-riverhog-extent": {
                    "policy": "contract_max",
                    "reason": "bounded-lifecycle-event-context"
                  }
                },
                {
                  "type": "null"
                }
              ],
              "default": null
            },
            "expires_at": {
              "maxLength": 30,
              "minLength": 30,
              "pattern": "^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}\\.[0-9]{9}Z$",
              "type": "string"
            },
            "initiator": {
              "$ref": "#/$defs/RiverhogActor"
            },
            "retrieval_id": {
              "maxLength": 300,
              "minLength": 1,
              "type": "string"
            },
            "state": {
              "const": "ready",
              "type": "string"
            }
          },
          "required": [
            "actor",
            "initiator",
            "retrieval_id",
            "collection_ids",
            "state",
            "expires_at"
          ],
          "type": "object"
        },
        "RetrievalRenewedEvent": {
          "additionalProperties": false,
          "properties": {
            "id": {
              "minLength": 1,
              "type": "string"
            },
            "occurred_at": {
              "maxLength": 30,
              "minLength": 30,
              "pattern": "^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}\\.[0-9]{9}Z$",
              "type": "string"
            },
            "payload": {
              "$ref": "#/$defs/RetrievalRenewedData"
            },
            "subject": {
              "anyOf": [
                {
                  "minLength": 1,
                  "type": "string"
                },
                {
                  "type": "null"
                }
              ],
              "default": null
            },
            "type": {
              "const": "io.riverhog.riverhog.retrieval.renewed",
              "type": "string"
            }
          },
          "required": [
            "id",
            "type",
            "occurred_at",
            "payload"
          ],
          "type": "object"
        },
        "RetrievalRequestedData": {
          "additionalProperties": false,
          "properties": {
            "actor": {
              "$ref": "#/$defs/RiverhogActor"
            },
            "collection_created_at": {
              "anyOf": [
                {
                  "maxLength": 30,
                  "minLength": 30,
                  "pattern": "^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}\\.[0-9]{9}Z$",
                  "type": "string"
                },
                {
                  "type": "null"
                }
              ],
              "default": null
            },
            "collection_id": {
              "anyOf": [
                {
                  "$ref": "#/$defs/CollectionId"
                },
                {
                  "type": "null"
                }
              ],
              "default": null
            },
            "collection_ids": {
              "items": {
                "$ref": "#/$defs/CollectionId"
              },
              "minItems": 1,
              "type": "array"
            },
            "context": {
              "anyOf": [
                {
                  "additionalProperties": true,
                  "type": "object",
                  "x-riverhog-encoded-bytes-max": 4096,
                  "x-riverhog-extent": {
                    "policy": "contract_max",
                    "reason": "bounded-lifecycle-event-context"
                  }
                },
                {
                  "type": "null"
                }
              ],
              "default": null
            },
            "files": {
              "minimum": 1,
              "type": "integer"
            },
            "initiator": {
              "$ref": "#/$defs/RiverhogActor"
            },
            "objects": {
              "minimum": 1,
              "type": "integer"
            },
            "restore_required": {
              "type": "boolean"
            },
            "retrieval_id": {
              "maxLength": 300,
              "minLength": 1,
              "type": "string"
            },
            "state": {
              "enum": [
                "requested",
                "ready"
              ],
              "type": "string"
            }
          },
          "required": [
            "actor",
            "initiator",
            "retrieval_id",
            "collection_ids",
            "state",
            "files",
            "objects",
            "restore_required"
          ],
          "type": "object"
        },
        "RetrievalRequestedEvent": {
          "additionalProperties": false,
          "properties": {
            "id": {
              "minLength": 1,
              "type": "string"
            },
            "occurred_at": {
              "maxLength": 30,
              "minLength": 30,
              "pattern": "^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}\\.[0-9]{9}Z$",
              "type": "string"
            },
            "payload": {
              "$ref": "#/$defs/RetrievalRequestedData"
            },
            "subject": {
              "anyOf": [
                {
                  "minLength": 1,
                  "type": "string"
                },
                {
                  "type": "null"
                }
              ],
              "default": null
            },
            "type": {
              "const": "io.riverhog.riverhog.retrieval.requested",
              "type": "string"
            }
          },
          "required": [
            "id",
            "type",
            "occurred_at",
            "payload"
          ],
          "type": "object"
        },
        "RiverhogActor": {
          "additionalProperties": false,
          "properties": {
            "key_id": {
              "anyOf": [
                {
                  "maxLength": 300,
                  "minLength": 1,
                  "type": "string"
                },
                {
                  "type": "null"
                }
              ],
              "default": null
            },
            "principal_id": {
              "$ref": "#/$defs/PrincipalId",
              "maxLength": 160
            }
          },
          "required": [
            "principal_id"
          ],
          "type": "object"
        },
        "RiverhogLifecycleEvent": {
          "discriminator": {
            "mapping": {
              "io.riverhog.riverhog.archive_copy_job.canceled": "#/$defs/ArchiveCopyJobCanceledEvent",
              "io.riverhog.riverhog.archive_copy_job.completed": "#/$defs/ArchiveCopyJobCompletedEvent",
              "io.riverhog.riverhog.archive_copy_job.failed": "#/$defs/ArchiveCopyJobFailedEvent",
              "io.riverhog.riverhog.archive_copy_job.requested": "#/$defs/ArchiveCopyJobRequestedEvent",
              "io.riverhog.riverhog.collection.deleted": "#/$defs/CollectionDeletedEvent",
              "io.riverhog.riverhog.collection.finalized": "#/$defs/CollectionFinalizedEvent",
              "io.riverhog.riverhog.retrieval.canceled": "#/$defs/RetrievalCanceledEvent",
              "io.riverhog.riverhog.retrieval.completed": "#/$defs/RetrievalCompletedEvent",
              "io.riverhog.riverhog.retrieval.expired": "#/$defs/RetrievalExpiredEvent",
              "io.riverhog.riverhog.retrieval.failed": "#/$defs/RetrievalFailedEvent",
              "io.riverhog.riverhog.retrieval.issue": "#/$defs/RetrievalIssueEvent",
              "io.riverhog.riverhog.retrieval.ready": "#/$defs/RetrievalReadyEvent",
              "io.riverhog.riverhog.retrieval.renewed": "#/$defs/RetrievalRenewedEvent",
              "io.riverhog.riverhog.retrieval.requested": "#/$defs/RetrievalRequestedEvent"
            },
            "propertyName": "type"
          },
          "oneOf": [
            {
              "$ref": "#/$defs/CollectionFinalizedEvent"
            },
            {
              "$ref": "#/$defs/CollectionDeletedEvent"
            },
            {
              "$ref": "#/$defs/ArchiveCopyJobRequestedEvent"
            },
            {
              "$ref": "#/$defs/ArchiveCopyJobCompletedEvent"
            },
            {
              "$ref": "#/$defs/ArchiveCopyJobFailedEvent"
            },
            {
              "$ref": "#/$defs/ArchiveCopyJobCanceledEvent"
            },
            {
              "$ref": "#/$defs/RetrievalRequestedEvent"
            },
            {
              "$ref": "#/$defs/RetrievalReadyEvent"
            },
            {
              "$ref": "#/$defs/RetrievalRenewedEvent"
            },
            {
              "$ref": "#/$defs/RetrievalCompletedEvent"
            },
            {
              "$ref": "#/$defs/RetrievalCanceledEvent"
            },
            {
              "$ref": "#/$defs/RetrievalExpiredEvent"
            },
            {
              "$ref": "#/$defs/RetrievalIssueEvent"
            },
            {
              "$ref": "#/$defs/RetrievalFailedEvent"
            }
          ]
        }
      },
      "additionalProperties": false,
      "properties": {
        "events": {
          "items": {
            "$ref": "#/$defs/RiverhogLifecycleEvent"
          },
          "type": "array"
        },
        "has_more": {
          "type": "boolean"
        },
        "next_cursor": {
          "$ref": "#/$defs/LifecycleEventCursor"
        }
      },
      "required": [
        "events",
        "next_cursor",
        "has_more"
      ],
      "type": "object"
    },
    "signature": "'(*, events: list[RiverhogLifecycleEvent], next_cursor: LifecycleEventCursor, has_more: bool) -> None'"
  },
  "distribution": "riverhog-protocol",
  "module": "riverhog_protocol",
  "name": "RiverhogEventPage",
  "unit": "export"
}
```

</details>
