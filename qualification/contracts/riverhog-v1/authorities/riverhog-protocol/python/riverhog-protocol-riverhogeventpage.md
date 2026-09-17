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

- [ArchiveCopyCanceledData](#s-cfa4c816fe)
- [ArchiveCopyCanceledEvent](#s-2d7985a904)
- [ArchiveCopyCompletedData](#s-4ea8ff49ee)
- [ArchiveCopyCompletedEvent](#s-0f544f1f69)
- [ArchiveCopyIssueData](#s-a5bfba4563)
- [ArchiveCopyIssueEvent](#s-c13ccdbeaf)
- [ArchiveCopyRequestedData](#s-336f4457d3)
- [ArchiveCopyRequestedEvent](#s-7f20e5b5e8)
- [ArchiveStoreName](#s-2e2a7afe9a)
- [CollectionDeletedData](#s-04e497d16f)
- [CollectionDeletedEvent](#s-e18a1ebf85)
- [CollectionFinalizedData](#s-caf8e4a999)
- [CollectionFinalizedEvent](#s-dcb5f23831)
- [CollectionId](#s-3b7d7e3c2c)
- [LifecycleEventCursor](#s-99b2d14f05)
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
- [RiverhogEventCause](#s-56c61376dd)
- [RiverhogLifecycleEvent](#s-41ab5ca15a)

##### <a id="s-cfa4c816fe"></a>definition `ArchiveCopyCanceledData`

- <a id="s-10af2f5c27"></a>`type`: `"object"`
- <a id="s-3592f413c9"></a>`additionalProperties`: `false`
- <a id="s-e91ff79dca"></a>`required`: `["actor","initiator","collection_id","collection_created_at","source_store","destination_store","state"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-3280379529"></a>`actor` | yes | [RiverhogActor](#s-3ec07c9031) |  |
| <a id="s-c2af0612fd"></a>`cause` | no | anyOf=[([RiverhogEventCause](#s-56c61376dd)); (type="null")]; default=null |  |
| <a id="s-74b7080db4"></a>`collection_created_at` | yes | type="string"; maxLength=64; minLength=1 |  |
| <a id="s-2b4c483c5f"></a>`collection_id` | yes | [CollectionId](#s-3b7d7e3c2c) |  |
| <a id="s-33dba6f1eb"></a>`context` | no | anyOf=[(type="object"; additionalProperties=(any JSON value); x-riverhog-encoded-bytes-max=4096; x-riverhog-extent={"policy":"contract_max","reason":"bounded-lifecycle-event-context"}); (type="null")]; default=null |  |
| <a id="s-8a05bfc451"></a>`destination_store` | yes | [ArchiveStoreName](#s-2e2a7afe9a) |  |
| <a id="s-ab6c72e50a"></a>`initiator` | yes | [RiverhogActor](#s-3ec07c9031) |  |
| <a id="s-c2ba3d485e"></a>`source_store` | yes | [ArchiveStoreName](#s-2e2a7afe9a) |  |
| <a id="s-358b2083c7"></a>`state` | yes | type="string"; const="canceled" |  |

##### <a id="s-2d7985a904"></a>definition `ArchiveCopyCanceledEvent`

- <a id="s-1d27102af9"></a>`type`: `"object"`
- <a id="s-fe670f69ac"></a>`additionalProperties`: `false`
- <a id="s-0bac221031"></a>`required`: `["id","source","type","time","data"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-a474d761ef"></a>`data` | yes | [ArchiveCopyCanceledData](#s-cfa4c816fe) |  |
| <a id="s-cb803f4c4e"></a>`datacontenttype` | no | type="string"; const="application/json"; default="application/json" |  |
| <a id="s-ada951b04e"></a>`id` | yes | type="string"; minLength=1 |  |
| <a id="s-94ea7ef0e6"></a>`source` | yes | type="string"; minLength=1 |  |
| <a id="s-926bf3f7eb"></a>`specversion` | no | type="string"; const="1.0"; default="1.0" |  |
| <a id="s-2d17c4ef70"></a>`subject` | no | anyOf=[(type="string"; minLength=1); (type="null")]; default=null |  |
| <a id="s-748f1d05de"></a>`time` | yes | type="string" |  |
| <a id="s-cbc17223c2"></a>`type` | yes | type="string"; const="io.riverhog.riverhog.archive_copy.canceled" |  |

##### <a id="s-4ea8ff49ee"></a>definition `ArchiveCopyCompletedData`

- <a id="s-19fde6fa3d"></a>`type`: `"object"`
- <a id="s-a2af53bbc6"></a>`additionalProperties`: `false`
- <a id="s-e55f01a76b"></a>`required`: `["actor","initiator","collection_id","collection_created_at","source_store","destination_store","state"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-a872366ecc"></a>`actor` | yes | [RiverhogActor](#s-3ec07c9031) |  |
| <a id="s-ef402a6fa0"></a>`cause` | no | anyOf=[([RiverhogEventCause](#s-56c61376dd)); (type="null")]; default=null |  |
| <a id="s-9f5059796a"></a>`collection_created_at` | yes | type="string"; maxLength=64; minLength=1 |  |
| <a id="s-6df5b22451"></a>`collection_id` | yes | [CollectionId](#s-3b7d7e3c2c) |  |
| <a id="s-3aff650bb7"></a>`context` | no | anyOf=[(type="object"; additionalProperties=(any JSON value); x-riverhog-encoded-bytes-max=4096; x-riverhog-extent={"policy":"contract_max","reason":"bounded-lifecycle-event-context"}); (type="null")]; default=null |  |
| <a id="s-7d87335252"></a>`destination_store` | yes | [ArchiveStoreName](#s-2e2a7afe9a) |  |
| <a id="s-a5b8f6d486"></a>`initiator` | yes | [RiverhogActor](#s-3ec07c9031) |  |
| <a id="s-7ad943949b"></a>`source_store` | yes | [ArchiveStoreName](#s-2e2a7afe9a) |  |
| <a id="s-60642ae1ba"></a>`state` | yes | type="string"; const="completed" |  |

##### <a id="s-0f544f1f69"></a>definition `ArchiveCopyCompletedEvent`

- <a id="s-290d1ff9fb"></a>`type`: `"object"`
- <a id="s-311fa4459b"></a>`additionalProperties`: `false`
- <a id="s-63247076b9"></a>`required`: `["id","source","type","time","data"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-5ff93d0fdd"></a>`data` | yes | [ArchiveCopyCompletedData](#s-4ea8ff49ee) |  |
| <a id="s-37fb83fe9b"></a>`datacontenttype` | no | type="string"; const="application/json"; default="application/json" |  |
| <a id="s-178a884b88"></a>`id` | yes | type="string"; minLength=1 |  |
| <a id="s-5a8210c932"></a>`source` | yes | type="string"; minLength=1 |  |
| <a id="s-10cbede6bc"></a>`specversion` | no | type="string"; const="1.0"; default="1.0" |  |
| <a id="s-c31f959106"></a>`subject` | no | anyOf=[(type="string"; minLength=1); (type="null")]; default=null |  |
| <a id="s-582f595eb9"></a>`time` | yes | type="string" |  |
| <a id="s-4c91be5661"></a>`type` | yes | type="string"; const="io.riverhog.riverhog.archive_copy.completed" |  |

##### <a id="s-a5bfba4563"></a>definition `ArchiveCopyIssueData`

- <a id="s-cd41af16c7"></a>`type`: `"object"`
- <a id="s-296d7da7d1"></a>`additionalProperties`: `false`
- <a id="s-d57d928588"></a>`required`: `["actor","initiator","collection_id","collection_created_at","source_store","destination_store","state","error"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-75d33b6b4f"></a>`actor` | yes | [RiverhogActor](#s-3ec07c9031) |  |
| <a id="s-1dc61ccb83"></a>`cause` | no | anyOf=[([RiverhogEventCause](#s-56c61376dd)); (type="null")]; default=null |  |
| <a id="s-36c67f2162"></a>`collection_created_at` | yes | type="string"; maxLength=64; minLength=1 |  |
| <a id="s-f25dfe0509"></a>`collection_id` | yes | [CollectionId](#s-3b7d7e3c2c) |  |
| <a id="s-fc0492df67"></a>`context` | no | anyOf=[(type="object"; additionalProperties=(any JSON value); x-riverhog-encoded-bytes-max=4096; x-riverhog-extent={"policy":"contract_max","reason":"bounded-lifecycle-event-context"}); (type="null")]; default=null |  |
| <a id="s-349c37611e"></a>`destination_store` | yes | [ArchiveStoreName](#s-2e2a7afe9a) |  |
| <a id="s-71f2908355"></a>`error` | yes | type="string"; maxLength=16384; minLength=1 |  |
| <a id="s-74ad67bbb0"></a>`initiator` | yes | [RiverhogActor](#s-3ec07c9031) |  |
| <a id="s-36624435e1"></a>`source_store` | yes | [ArchiveStoreName](#s-2e2a7afe9a) |  |
| <a id="s-0003e99102"></a>`state` | yes | type="string"; const="failed" |  |

##### <a id="s-c13ccdbeaf"></a>definition `ArchiveCopyIssueEvent`

- <a id="s-1d2bef65b9"></a>`type`: `"object"`
- <a id="s-88fcc9dfb9"></a>`additionalProperties`: `false`
- <a id="s-e069c6aa3a"></a>`required`: `["id","source","type","time","data"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-2b6cae5387"></a>`data` | yes | [ArchiveCopyIssueData](#s-a5bfba4563) |  |
| <a id="s-b7a63933aa"></a>`datacontenttype` | no | type="string"; const="application/json"; default="application/json" |  |
| <a id="s-bb4a44340f"></a>`id` | yes | type="string"; minLength=1 |  |
| <a id="s-4478a9fc56"></a>`source` | yes | type="string"; minLength=1 |  |
| <a id="s-c27b43251e"></a>`specversion` | no | type="string"; const="1.0"; default="1.0" |  |
| <a id="s-8565661348"></a>`subject` | no | anyOf=[(type="string"; minLength=1); (type="null")]; default=null |  |
| <a id="s-e1369bddc7"></a>`time` | yes | type="string" |  |
| <a id="s-3c39fa7f36"></a>`type` | yes | type="string"; const="io.riverhog.riverhog.archive_copy.issue" |  |

##### <a id="s-336f4457d3"></a>definition `ArchiveCopyRequestedData`

- <a id="s-e2aaa9f2bb"></a>`type`: `"object"`
- <a id="s-f2fe97cdc8"></a>`additionalProperties`: `false`
- <a id="s-51f90a01f9"></a>`required`: `["actor","initiator","collection_id","collection_created_at","source_store","destination_store","state"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-5772cb13fd"></a>`actor` | yes | [RiverhogActor](#s-3ec07c9031) |  |
| <a id="s-91a65e5bab"></a>`cause` | no | anyOf=[([RiverhogEventCause](#s-56c61376dd)); (type="null")]; default=null |  |
| <a id="s-afdf08164c"></a>`collection_created_at` | yes | type="string"; maxLength=64; minLength=1 |  |
| <a id="s-d330244f3f"></a>`collection_id` | yes | [CollectionId](#s-3b7d7e3c2c) |  |
| <a id="s-8b24f1341b"></a>`context` | no | anyOf=[(type="object"; additionalProperties=(any JSON value); x-riverhog-encoded-bytes-max=4096; x-riverhog-extent={"policy":"contract_max","reason":"bounded-lifecycle-event-context"}); (type="null")]; default=null |  |
| <a id="s-049e8e50fc"></a>`destination_store` | yes | [ArchiveStoreName](#s-2e2a7afe9a) |  |
| <a id="s-336dc29ada"></a>`initiator` | yes | [RiverhogActor](#s-3ec07c9031) |  |
| <a id="s-328963d607"></a>`source_store` | yes | [ArchiveStoreName](#s-2e2a7afe9a) |  |
| <a id="s-b44a917376"></a>`state` | yes | type="string"; const="requested" |  |

##### <a id="s-7f20e5b5e8"></a>definition `ArchiveCopyRequestedEvent`

- <a id="s-ccf6a0102a"></a>`type`: `"object"`
- <a id="s-0a4efa39bd"></a>`additionalProperties`: `false`
- <a id="s-716d4cc32a"></a>`required`: `["id","source","type","time","data"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-4ca0f7b36e"></a>`data` | yes | [ArchiveCopyRequestedData](#s-336f4457d3) |  |
| <a id="s-b5d63fc490"></a>`datacontenttype` | no | type="string"; const="application/json"; default="application/json" |  |
| <a id="s-ab6d13184e"></a>`id` | yes | type="string"; minLength=1 |  |
| <a id="s-bd21e93da4"></a>`source` | yes | type="string"; minLength=1 |  |
| <a id="s-138ec9b34d"></a>`specversion` | no | type="string"; const="1.0"; default="1.0" |  |
| <a id="s-f6dd659f16"></a>`subject` | no | anyOf=[(type="string"; minLength=1); (type="null")]; default=null |  |
| <a id="s-bdcec90dcd"></a>`time` | yes | type="string" |  |
| <a id="s-0767fb8dcc"></a>`type` | yes | type="string"; const="io.riverhog.riverhog.archive_copy.requested" |  |

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
| <a id="s-49218e5e3e"></a>`cause` | no | anyOf=[([RiverhogEventCause](#s-56c61376dd)); (type="null")]; default=null |  |
| <a id="s-8dae7de57b"></a>`collection_created_at` | yes | type="string"; maxLength=64; minLength=1 |  |
| <a id="s-ac2d1c2565"></a>`collection_id` | yes | [CollectionId](#s-3b7d7e3c2c) |  |
| <a id="s-30c4f9b5e9"></a>`context` | no | anyOf=[(type="object"; additionalProperties=(any JSON value); x-riverhog-encoded-bytes-max=4096; x-riverhog-extent={"policy":"contract_max","reason":"bounded-lifecycle-event-context"}); (type="null")]; default=null |  |
| <a id="s-5bc7b30fe2"></a>`files` | yes | type="integer"; minimum=0 |  |
| <a id="s-65bb659349"></a>`initiator` | yes | [RiverhogActor](#s-3ec07c9031) |  |
| <a id="s-c1bfb9d0ef"></a>`remote_storage_bytes` | yes | type="integer"; minimum=0 |  |

##### <a id="s-e18a1ebf85"></a>definition `CollectionDeletedEvent`

- <a id="s-0c3545da3d"></a>`type`: `"object"`
- <a id="s-077f0fd07b"></a>`additionalProperties`: `false`
- <a id="s-b315fc9f5e"></a>`required`: `["id","source","type","time","data"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-ae267df8a1"></a>`data` | yes | [CollectionDeletedData](#s-04e497d16f) |  |
| <a id="s-4203cc0a1c"></a>`datacontenttype` | no | type="string"; const="application/json"; default="application/json" |  |
| <a id="s-1246c40f7b"></a>`id` | yes | type="string"; minLength=1 |  |
| <a id="s-28c4eb0e96"></a>`source` | yes | type="string"; minLength=1 |  |
| <a id="s-2e6704ebc4"></a>`specversion` | no | type="string"; const="1.0"; default="1.0" |  |
| <a id="s-29f8fe627a"></a>`subject` | no | anyOf=[(type="string"; minLength=1); (type="null")]; default=null |  |
| <a id="s-153e92bfb8"></a>`time` | yes | type="string" |  |
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
| <a id="s-1760e551a2"></a>`cause` | no | anyOf=[([RiverhogEventCause](#s-56c61376dd)); (type="null")]; default=null |  |
| <a id="s-36de94ee67"></a>`collection_created_at` | yes | type="string"; maxLength=64; minLength=1 |  |
| <a id="s-8c1457cfa0"></a>`collection_id` | yes | [CollectionId](#s-3b7d7e3c2c) |  |
| <a id="s-6c21ec8a3d"></a>`context` | no | anyOf=[(type="object"; additionalProperties=(any JSON value); x-riverhog-encoded-bytes-max=4096; x-riverhog-extent={"policy":"contract_max","reason":"bounded-lifecycle-event-context"}); (type="null")]; default=null |  |
| <a id="s-65413559df"></a>`files_total` | yes | type="integer"; minimum=0 |  |
| <a id="s-c3ea18e378"></a>`initiator` | yes | [RiverhogActor](#s-3ec07c9031) |  |

##### <a id="s-dcb5f23831"></a>definition `CollectionFinalizedEvent`

- <a id="s-3d619a313b"></a>`type`: `"object"`
- <a id="s-bd88c00517"></a>`additionalProperties`: `false`
- <a id="s-3748804a5e"></a>`required`: `["id","source","type","time","data"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-98c563de29"></a>`data` | yes | [CollectionFinalizedData](#s-caf8e4a999) |  |
| <a id="s-456c3ea72f"></a>`datacontenttype` | no | type="string"; const="application/json"; default="application/json" |  |
| <a id="s-51d7b31342"></a>`id` | yes | type="string"; minLength=1 |  |
| <a id="s-c335c5ae5e"></a>`source` | yes | type="string"; minLength=1 |  |
| <a id="s-25fea6a4fd"></a>`specversion` | no | type="string"; const="1.0"; default="1.0" |  |
| <a id="s-ef095a513a"></a>`subject` | no | anyOf=[(type="string"; minLength=1); (type="null")]; default=null |  |
| <a id="s-a93c31cd89"></a>`time` | yes | type="string" |  |
| <a id="s-c170710dc4"></a>`type` | yes | type="string"; const="io.riverhog.riverhog.collection.finalized" |  |

##### <a id="s-3b7d7e3c2c"></a>definition `CollectionId`

- <a id="s-c38d90d6e9"></a>`type`: `"integer"`
- <a id="s-dd8a09abe6"></a>`minimum`: `1`

##### <a id="s-99b2d14f05"></a>definition `LifecycleEventCursor`

- <a id="s-d22168b174"></a>`type`: `"string"`
- <a id="s-fd1803775d"></a>`maxLength`: `19`
- <a id="s-9e4214b970"></a>`minLength`: `1`
- <a id="s-967f676da4"></a>`pattern`: `"^(?:0\|[1-9][0-9]*)$"`

##### <a id="s-8f780dd62d"></a>definition `RetrievalCanceledData`

- <a id="s-9c3dc1f93c"></a>`type`: `"object"`
- <a id="s-92ea2a30f2"></a>`additionalProperties`: `false`
- <a id="s-aa2c63a88a"></a>`required`: `["actor","initiator","retrieval_id","collection_ids","state"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-9caf87ec0f"></a>`actor` | yes | [RiverhogActor](#s-3ec07c9031) |  |
| <a id="s-587fc9338b"></a>`cause` | no | anyOf=[([RiverhogEventCause](#s-56c61376dd)); (type="null")]; default=null |  |
| <a id="s-17ba014898"></a>`collection_created_at` | no | anyOf=[(type="string"; maxLength=64; minLength=1); (type="null")]; default=null |  |
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
- <a id="s-fccedfe566"></a>`required`: `["id","source","type","time","data"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-ff0dd0d976"></a>`data` | yes | [RetrievalCanceledData](#s-8f780dd62d) |  |
| <a id="s-93ab880947"></a>`datacontenttype` | no | type="string"; const="application/json"; default="application/json" |  |
| <a id="s-6e0c6be23f"></a>`id` | yes | type="string"; minLength=1 |  |
| <a id="s-4395aacfb6"></a>`source` | yes | type="string"; minLength=1 |  |
| <a id="s-976b2d5f99"></a>`specversion` | no | type="string"; const="1.0"; default="1.0" |  |
| <a id="s-9173f5b0df"></a>`subject` | no | anyOf=[(type="string"; minLength=1); (type="null")]; default=null |  |
| <a id="s-2c47db07d1"></a>`time` | yes | type="string" |  |
| <a id="s-2835564e78"></a>`type` | yes | type="string"; const="io.riverhog.riverhog.retrieval.canceled" |  |

##### <a id="s-e680525602"></a>definition `RetrievalCompletedData`

- <a id="s-6e207becee"></a>`type`: `"object"`
- <a id="s-48a748ff15"></a>`additionalProperties`: `false`
- <a id="s-327db209e8"></a>`required`: `["actor","initiator","retrieval_id","collection_ids","state"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-d633b25199"></a>`actor` | yes | [RiverhogActor](#s-3ec07c9031) |  |
| <a id="s-1703f42ba6"></a>`cause` | no | anyOf=[([RiverhogEventCause](#s-56c61376dd)); (type="null")]; default=null |  |
| <a id="s-483430fb94"></a>`collection_created_at` | no | anyOf=[(type="string"; maxLength=64; minLength=1); (type="null")]; default=null |  |
| <a id="s-4be8917304"></a>`collection_id` | no | anyOf=[([CollectionId](#s-3b7d7e3c2c)); (type="null")]; default=null |  |
| <a id="s-bb1b254e9e"></a>`collection_ids` | yes | type="array"; items=([CollectionId](#s-3b7d7e3c2c)); minItems=1 |  |
| <a id="s-dfcb1fcb34"></a>`context` | no | anyOf=[(type="object"; additionalProperties=(any JSON value); x-riverhog-encoded-bytes-max=4096; x-riverhog-extent={"policy":"contract_max","reason":"bounded-lifecycle-event-context"}); (type="null")]; default=null |  |
| <a id="s-0b7290ed8e"></a>`initiator` | yes | [RiverhogActor](#s-3ec07c9031) |  |
| <a id="s-034055a483"></a>`retrieval_id` | yes | type="string"; maxLength=300; minLength=1 |  |
| <a id="s-8e4aa554ab"></a>`state` | yes | type="string"; const="completed" |  |

##### <a id="s-e763e9511f"></a>definition `RetrievalCompletedEvent`

- <a id="s-ae7ea3d8f7"></a>`type`: `"object"`
- <a id="s-ad230f078e"></a>`additionalProperties`: `false`
- <a id="s-8e6cfd5a88"></a>`required`: `["id","source","type","time","data"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-c5881ab1a6"></a>`data` | yes | [RetrievalCompletedData](#s-e680525602) |  |
| <a id="s-24fd63d968"></a>`datacontenttype` | no | type="string"; const="application/json"; default="application/json" |  |
| <a id="s-1e88b7ff6e"></a>`id` | yes | type="string"; minLength=1 |  |
| <a id="s-9f53fa1852"></a>`source` | yes | type="string"; minLength=1 |  |
| <a id="s-f781b54b02"></a>`specversion` | no | type="string"; const="1.0"; default="1.0" |  |
| <a id="s-101aec1767"></a>`subject` | no | anyOf=[(type="string"; minLength=1); (type="null")]; default=null |  |
| <a id="s-f300bdc263"></a>`time` | yes | type="string" |  |
| <a id="s-880eb05bbe"></a>`type` | yes | type="string"; const="io.riverhog.riverhog.retrieval.completed" |  |

##### <a id="s-901491f41d"></a>definition `RetrievalExpiredData`

- <a id="s-2fbb8f7831"></a>`type`: `"object"`
- <a id="s-b7662ed3ea"></a>`additionalProperties`: `false`
- <a id="s-f2e526214f"></a>`required`: `["actor","initiator","retrieval_id","collection_ids","state"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-ed336aa0ab"></a>`actor` | yes | [RiverhogActor](#s-3ec07c9031) |  |
| <a id="s-77dbc0be59"></a>`cause` | no | anyOf=[([RiverhogEventCause](#s-56c61376dd)); (type="null")]; default=null |  |
| <a id="s-208b5a2bcc"></a>`collection_created_at` | no | anyOf=[(type="string"; maxLength=64; minLength=1); (type="null")]; default=null |  |
| <a id="s-4fe2097994"></a>`collection_id` | no | anyOf=[([CollectionId](#s-3b7d7e3c2c)); (type="null")]; default=null |  |
| <a id="s-015acc72f6"></a>`collection_ids` | yes | type="array"; items=([CollectionId](#s-3b7d7e3c2c)); minItems=1 |  |
| <a id="s-16112840b3"></a>`context` | no | anyOf=[(type="object"; additionalProperties=(any JSON value); x-riverhog-encoded-bytes-max=4096; x-riverhog-extent={"policy":"contract_max","reason":"bounded-lifecycle-event-context"}); (type="null")]; default=null |  |
| <a id="s-a6011e4483"></a>`initiator` | yes | [RiverhogActor](#s-3ec07c9031) |  |
| <a id="s-425de01a61"></a>`retrieval_id` | yes | type="string"; maxLength=300; minLength=1 |  |
| <a id="s-e6ff329283"></a>`state` | yes | type="string"; const="expired" |  |

##### <a id="s-cd3863d78c"></a>definition `RetrievalExpiredEvent`

- <a id="s-e5aafe422b"></a>`type`: `"object"`
- <a id="s-e87bbcfefa"></a>`additionalProperties`: `false`
- <a id="s-a5ee615f80"></a>`required`: `["id","source","type","time","data"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-1fe1754bc9"></a>`data` | yes | [RetrievalExpiredData](#s-901491f41d) |  |
| <a id="s-c0a852d97f"></a>`datacontenttype` | no | type="string"; const="application/json"; default="application/json" |  |
| <a id="s-469120a2d7"></a>`id` | yes | type="string"; minLength=1 |  |
| <a id="s-ae8b415b82"></a>`source` | yes | type="string"; minLength=1 |  |
| <a id="s-bfb0bf6f88"></a>`specversion` | no | type="string"; const="1.0"; default="1.0" |  |
| <a id="s-356d4325e2"></a>`subject` | no | anyOf=[(type="string"; minLength=1); (type="null")]; default=null |  |
| <a id="s-859791f24c"></a>`time` | yes | type="string" |  |
| <a id="s-c9e0511c11"></a>`type` | yes | type="string"; const="io.riverhog.riverhog.retrieval.expired" |  |

##### <a id="s-c2a74431c4"></a>definition `RetrievalFailedData`

- <a id="s-d7d1f236b0"></a>`type`: `"object"`
- <a id="s-d13f4451cc"></a>`additionalProperties`: `false`
- <a id="s-8624bf85ca"></a>`required`: `["actor","initiator","retrieval_id","collection_ids","state","error"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-9e0fcb7c1f"></a>`actor` | yes | [RiverhogActor](#s-3ec07c9031) |  |
| <a id="s-dae4492859"></a>`cause` | no | anyOf=[([RiverhogEventCause](#s-56c61376dd)); (type="null")]; default=null |  |
| <a id="s-68c0974906"></a>`collection_created_at` | no | anyOf=[(type="string"; maxLength=64; minLength=1); (type="null")]; default=null |  |
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
- <a id="s-82dacfdf97"></a>`required`: `["id","source","type","time","data"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-f6cf3cf2c9"></a>`data` | yes | [RetrievalFailedData](#s-c2a74431c4) |  |
| <a id="s-2f47f1d5aa"></a>`datacontenttype` | no | type="string"; const="application/json"; default="application/json" |  |
| <a id="s-bb02cf5b5c"></a>`id` | yes | type="string"; minLength=1 |  |
| <a id="s-a19b2058b9"></a>`source` | yes | type="string"; minLength=1 |  |
| <a id="s-b4479f5de0"></a>`specversion` | no | type="string"; const="1.0"; default="1.0" |  |
| <a id="s-c91911f642"></a>`subject` | no | anyOf=[(type="string"; minLength=1); (type="null")]; default=null |  |
| <a id="s-09898f0b1b"></a>`time` | yes | type="string" |  |
| <a id="s-7069924984"></a>`type` | yes | type="string"; const="io.riverhog.riverhog.retrieval.failed" |  |

##### <a id="s-5d019111c7"></a>definition `RetrievalIssueData`

- <a id="s-5c147f3572"></a>`type`: `"object"`
- <a id="s-14f2570df6"></a>`additionalProperties`: `false`
- <a id="s-ad7baa4e72"></a>`required`: `["actor","initiator","retrieval_id","collection_ids","state","error"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-9c895201e2"></a>`actor` | yes | [RiverhogActor](#s-3ec07c9031) |  |
| <a id="s-bb9dc193c6"></a>`cause` | no | anyOf=[([RiverhogEventCause](#s-56c61376dd)); (type="null")]; default=null |  |
| <a id="s-fadaa0dc64"></a>`collection_created_at` | no | anyOf=[(type="string"; maxLength=64; minLength=1); (type="null")]; default=null |  |
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
- <a id="s-86f0873a33"></a>`required`: `["id","source","type","time","data"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-74139bf847"></a>`data` | yes | [RetrievalIssueData](#s-5d019111c7) |  |
| <a id="s-9d4cd1ea57"></a>`datacontenttype` | no | type="string"; const="application/json"; default="application/json" |  |
| <a id="s-4009580dfc"></a>`id` | yes | type="string"; minLength=1 |  |
| <a id="s-c37fb27fa6"></a>`source` | yes | type="string"; minLength=1 |  |
| <a id="s-6533ffd54c"></a>`specversion` | no | type="string"; const="1.0"; default="1.0" |  |
| <a id="s-29dccbde08"></a>`subject` | no | anyOf=[(type="string"; minLength=1); (type="null")]; default=null |  |
| <a id="s-75a6f5c746"></a>`time` | yes | type="string" |  |
| <a id="s-8467f20f19"></a>`type` | yes | type="string"; const="io.riverhog.riverhog.retrieval.issue" |  |

##### <a id="s-94276b17d8"></a>definition `RetrievalReadyData`

- <a id="s-e1eabb007b"></a>`type`: `"object"`
- <a id="s-1b2cf44f95"></a>`additionalProperties`: `false`
- <a id="s-f145937c0c"></a>`required`: `["actor","initiator","retrieval_id","collection_ids","state","expires_at"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-a42a913481"></a>`actor` | yes | [RiverhogActor](#s-3ec07c9031) |  |
| <a id="s-68759b58f8"></a>`cause` | no | anyOf=[([RiverhogEventCause](#s-56c61376dd)); (type="null")]; default=null |  |
| <a id="s-31ea966e9e"></a>`collection_created_at` | no | anyOf=[(type="string"; maxLength=64; minLength=1); (type="null")]; default=null |  |
| <a id="s-d268e8c028"></a>`collection_id` | no | anyOf=[([CollectionId](#s-3b7d7e3c2c)); (type="null")]; default=null |  |
| <a id="s-ddd3bb8bc2"></a>`collection_ids` | yes | type="array"; items=([CollectionId](#s-3b7d7e3c2c)); minItems=1 |  |
| <a id="s-ed8038fbe3"></a>`context` | no | anyOf=[(type="object"; additionalProperties=(any JSON value); x-riverhog-encoded-bytes-max=4096; x-riverhog-extent={"policy":"contract_max","reason":"bounded-lifecycle-event-context"}); (type="null")]; default=null |  |
| <a id="s-3aa854894c"></a>`expires_at` | yes | type="string"; maxLength=64; minLength=1 |  |
| <a id="s-63d865e6bb"></a>`initiator` | yes | [RiverhogActor](#s-3ec07c9031) |  |
| <a id="s-d42e5eca9a"></a>`retrieval_id` | yes | type="string"; maxLength=300; minLength=1 |  |
| <a id="s-1a3f96afcd"></a>`state` | yes | type="string"; const="ready" |  |

##### <a id="s-f23d561333"></a>definition `RetrievalReadyEvent`

- <a id="s-f8df7b497b"></a>`type`: `"object"`
- <a id="s-c2992f732d"></a>`additionalProperties`: `false`
- <a id="s-d147d3d0a5"></a>`required`: `["id","source","type","time","data"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-097ba8da1e"></a>`data` | yes | [RetrievalReadyData](#s-94276b17d8) |  |
| <a id="s-f713cdf5f6"></a>`datacontenttype` | no | type="string"; const="application/json"; default="application/json" |  |
| <a id="s-69b39dcc7b"></a>`id` | yes | type="string"; minLength=1 |  |
| <a id="s-5b06ca20d8"></a>`source` | yes | type="string"; minLength=1 |  |
| <a id="s-75c83e093e"></a>`specversion` | no | type="string"; const="1.0"; default="1.0" |  |
| <a id="s-e373935a4e"></a>`subject` | no | anyOf=[(type="string"; minLength=1); (type="null")]; default=null |  |
| <a id="s-23798381bb"></a>`time` | yes | type="string" |  |
| <a id="s-c3c2577807"></a>`type` | yes | type="string"; const="io.riverhog.riverhog.retrieval.ready" |  |

##### <a id="s-d1bc1f3107"></a>definition `RetrievalRenewedData`

- <a id="s-34981f3a5d"></a>`type`: `"object"`
- <a id="s-cc5ba33e14"></a>`additionalProperties`: `false`
- <a id="s-f9c9a4d574"></a>`required`: `["actor","initiator","retrieval_id","collection_ids","state","expires_at"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-cf63af7b77"></a>`actor` | yes | [RiverhogActor](#s-3ec07c9031) |  |
| <a id="s-3890179752"></a>`cause` | no | anyOf=[([RiverhogEventCause](#s-56c61376dd)); (type="null")]; default=null |  |
| <a id="s-c0cc9568d5"></a>`collection_created_at` | no | anyOf=[(type="string"; maxLength=64; minLength=1); (type="null")]; default=null |  |
| <a id="s-17488d8589"></a>`collection_id` | no | anyOf=[([CollectionId](#s-3b7d7e3c2c)); (type="null")]; default=null |  |
| <a id="s-1115efd158"></a>`collection_ids` | yes | type="array"; items=([CollectionId](#s-3b7d7e3c2c)); minItems=1 |  |
| <a id="s-90f064dc62"></a>`context` | no | anyOf=[(type="object"; additionalProperties=(any JSON value); x-riverhog-encoded-bytes-max=4096; x-riverhog-extent={"policy":"contract_max","reason":"bounded-lifecycle-event-context"}); (type="null")]; default=null |  |
| <a id="s-3b00b3651c"></a>`expires_at` | yes | type="string"; maxLength=64; minLength=1 |  |
| <a id="s-ee8bf80162"></a>`initiator` | yes | [RiverhogActor](#s-3ec07c9031) |  |
| <a id="s-b6de652492"></a>`retrieval_id` | yes | type="string"; maxLength=300; minLength=1 |  |
| <a id="s-4cb4ac0a11"></a>`state` | yes | type="string"; const="ready" |  |

##### <a id="s-45fb1a016c"></a>definition `RetrievalRenewedEvent`

- <a id="s-f6935ffc2f"></a>`type`: `"object"`
- <a id="s-49deb12a22"></a>`additionalProperties`: `false`
- <a id="s-ed986274fe"></a>`required`: `["id","source","type","time","data"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-89089359d8"></a>`data` | yes | [RetrievalRenewedData](#s-d1bc1f3107) |  |
| <a id="s-fd68293878"></a>`datacontenttype` | no | type="string"; const="application/json"; default="application/json" |  |
| <a id="s-dd138eafd1"></a>`id` | yes | type="string"; minLength=1 |  |
| <a id="s-1f0472058a"></a>`source` | yes | type="string"; minLength=1 |  |
| <a id="s-904e68dbed"></a>`specversion` | no | type="string"; const="1.0"; default="1.0" |  |
| <a id="s-3c44160f20"></a>`subject` | no | anyOf=[(type="string"; minLength=1); (type="null")]; default=null |  |
| <a id="s-8d9ea4f6b7"></a>`time` | yes | type="string" |  |
| <a id="s-6a4f1e6158"></a>`type` | yes | type="string"; const="io.riverhog.riverhog.retrieval.renewed" |  |

##### <a id="s-64049bf6f9"></a>definition `RetrievalRequestedData`

- <a id="s-2a3fae73f5"></a>`type`: `"object"`
- <a id="s-c8ef2fb970"></a>`additionalProperties`: `false`
- <a id="s-5c327336af"></a>`required`: `["actor","initiator","retrieval_id","collection_ids","state","files","objects","restore_required"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-3a82345d45"></a>`actor` | yes | [RiverhogActor](#s-3ec07c9031) |  |
| <a id="s-72adccc6c8"></a>`cause` | no | anyOf=[([RiverhogEventCause](#s-56c61376dd)); (type="null")]; default=null |  |
| <a id="s-e32c279383"></a>`collection_created_at` | no | anyOf=[(type="string"; maxLength=64; minLength=1); (type="null")]; default=null |  |
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
- <a id="s-c306891d1a"></a>`required`: `["id","source","type","time","data"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-9c9d06d1bb"></a>`data` | yes | [RetrievalRequestedData](#s-64049bf6f9) |  |
| <a id="s-fb6e7fbbc9"></a>`datacontenttype` | no | type="string"; const="application/json"; default="application/json" |  |
| <a id="s-381822e8b4"></a>`id` | yes | type="string"; minLength=1 |  |
| <a id="s-ff969da433"></a>`source` | yes | type="string"; minLength=1 |  |
| <a id="s-dad540dd63"></a>`specversion` | no | type="string"; const="1.0"; default="1.0" |  |
| <a id="s-1e9333c620"></a>`subject` | no | anyOf=[(type="string"; minLength=1); (type="null")]; default=null |  |
| <a id="s-b7bf2cedf2"></a>`time` | yes | type="string" |  |
| <a id="s-52578520a5"></a>`type` | yes | type="string"; const="io.riverhog.riverhog.retrieval.requested" |  |

##### <a id="s-3ec07c9031"></a>definition `RiverhogActor`

- <a id="s-5670f78f7d"></a>`type`: `"object"`
- <a id="s-268c8df8e0"></a>`additionalProperties`: `false`
- <a id="s-c53cac5a10"></a>`required`: `["app"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-c35d213589"></a>`app` | yes | type="string"; maxLength=160; minLength=1 |  |
| <a id="s-87ad259a07"></a>`key_id` | no | anyOf=[(type="string"; maxLength=300; minLength=1); (type="null")]; default=null |  |

##### <a id="s-56c61376dd"></a>definition `RiverhogEventCause`

- <a id="s-7637042f67"></a>`type`: `"object"`
- <a id="s-e5f8d25978"></a>`additionalProperties`: `false`
- <a id="s-3acd1446c7"></a>`required`: `["id","source","type"]`

###### Fields

| Field | Required | Shape | Description |
|---|---:|---|---|
| <a id="s-f9d5f40e41"></a>`id` | yes | type="string"; maxLength=300; minLength=1 |  |
| <a id="s-c7579843a7"></a>`source` | yes | type="string"; maxLength=1000; minLength=1 |  |
| <a id="s-659df81f50"></a>`subject` | no | anyOf=[(type="string"; maxLength=1000; minLength=1); (type="null")]; default=null |  |
| <a id="s-e5cf71aa47"></a>`type` | yes | type="string"; maxLength=300; minLength=1 |  |

##### <a id="s-41ab5ca15a"></a>definition `RiverhogLifecycleEvent`

- <a id="s-2661c0e417"></a>`discriminator`: `{"mapping":{"io.riverhog.riverhog.archive_copy.canceled":"#/$defs/ArchiveCopyCanceledEvent","io.riverhog.riverhog.archive_copy.completed":"#/$defs/ArchiveCopyCompletedEvent","io.riverhog.riverhog.archive_copy.issue":"#/$defs/ArchiveCopyIssueEvent","io.riverhog.riverhog.archive_copy.requested":"#/$defs/ArchiveCopyRequestedEvent","io.riverhog.riverhog.collection.deleted":"#/$defs/CollectionDeletedEvent","io.riverhog.riverhog.collection.finalized":"#/$defs/CollectionFinalizedEvent","io.riverhog.riverhog.retrieval.canceled":"#/$defs/RetrievalCanceledEvent","io.riverhog.riverhog.retrieval.completed":"#/$defs/RetrievalCompletedEvent","io.riverhog.riverhog.retrieval.expired":"#/$defs/RetrievalExpiredEvent","io.riverhog.riverhog.retrieval.failed":"#/$defs/RetrievalFailedEvent","io.riverhog.riverhog.retrieval.issue":"#/$defs/RetrievalIssueEvent","io.riverhog.riverhog.retrieval.ready":"#/$defs/RetrievalReadyEvent","io.riverhog.riverhog.retrieval.renewed":"#/$defs/RetrievalRenewedEvent","io.riverhog.riverhog.retrieval.requested":"#/$defs/RetrievalRequestedEvent"},"propertyName":"type"}`

###### Exactly one must match (`oneOf`)

| Alternative | Schema |
|---|---|
| <a id="s-957bd4c2c3"></a>1 | [CollectionFinalizedEvent](#s-dcb5f23831) |
| <a id="s-ec5169534a"></a>2 | [CollectionDeletedEvent](#s-e18a1ebf85) |
| <a id="s-41b9e81340"></a>3 | [ArchiveCopyRequestedEvent](#s-7f20e5b5e8) |
| <a id="s-8980bd2c00"></a>4 | [ArchiveCopyCompletedEvent](#s-0f544f1f69) |
| <a id="s-a2ebed2009"></a>5 | [ArchiveCopyIssueEvent](#s-c13ccdbeaf) |
| <a id="s-543be3c82c"></a>6 | [ArchiveCopyCanceledEvent](#s-2d7985a904) |
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

<!-- exact-contract-value: 709d47d5a37072527779c24717803b28a6dcf6f0e71a6a5a444475fa2191affa -->

```json
{
  "contract": {
    "kind": "class",
    "schema": {
      "$defs": {
        "ArchiveCopyCanceledData": {
          "additionalProperties": false,
          "properties": {
            "actor": {
              "$ref": "#/$defs/RiverhogActor"
            },
            "cause": {
              "anyOf": [
                {
                  "$ref": "#/$defs/RiverhogEventCause"
                },
                {
                  "type": "null"
                }
              ],
              "default": null
            },
            "collection_created_at": {
              "maxLength": 64,
              "minLength": 1,
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
        "ArchiveCopyCanceledEvent": {
          "additionalProperties": false,
          "properties": {
            "data": {
              "$ref": "#/$defs/ArchiveCopyCanceledData"
            },
            "datacontenttype": {
              "const": "application/json",
              "default": "application/json",
              "type": "string"
            },
            "id": {
              "minLength": 1,
              "type": "string"
            },
            "source": {
              "minLength": 1,
              "type": "string"
            },
            "specversion": {
              "const": "1.0",
              "default": "1.0",
              "type": "string"
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
            "time": {
              "type": "string"
            },
            "type": {
              "const": "io.riverhog.riverhog.archive_copy.canceled",
              "type": "string"
            }
          },
          "required": [
            "id",
            "source",
            "type",
            "time",
            "data"
          ],
          "type": "object"
        },
        "ArchiveCopyCompletedData": {
          "additionalProperties": false,
          "properties": {
            "actor": {
              "$ref": "#/$defs/RiverhogActor"
            },
            "cause": {
              "anyOf": [
                {
                  "$ref": "#/$defs/RiverhogEventCause"
                },
                {
                  "type": "null"
                }
              ],
              "default": null
            },
            "collection_created_at": {
              "maxLength": 64,
              "minLength": 1,
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
        "ArchiveCopyCompletedEvent": {
          "additionalProperties": false,
          "properties": {
            "data": {
              "$ref": "#/$defs/ArchiveCopyCompletedData"
            },
            "datacontenttype": {
              "const": "application/json",
              "default": "application/json",
              "type": "string"
            },
            "id": {
              "minLength": 1,
              "type": "string"
            },
            "source": {
              "minLength": 1,
              "type": "string"
            },
            "specversion": {
              "const": "1.0",
              "default": "1.0",
              "type": "string"
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
            "time": {
              "type": "string"
            },
            "type": {
              "const": "io.riverhog.riverhog.archive_copy.completed",
              "type": "string"
            }
          },
          "required": [
            "id",
            "source",
            "type",
            "time",
            "data"
          ],
          "type": "object"
        },
        "ArchiveCopyIssueData": {
          "additionalProperties": false,
          "properties": {
            "actor": {
              "$ref": "#/$defs/RiverhogActor"
            },
            "cause": {
              "anyOf": [
                {
                  "$ref": "#/$defs/RiverhogEventCause"
                },
                {
                  "type": "null"
                }
              ],
              "default": null
            },
            "collection_created_at": {
              "maxLength": 64,
              "minLength": 1,
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
        "ArchiveCopyIssueEvent": {
          "additionalProperties": false,
          "properties": {
            "data": {
              "$ref": "#/$defs/ArchiveCopyIssueData"
            },
            "datacontenttype": {
              "const": "application/json",
              "default": "application/json",
              "type": "string"
            },
            "id": {
              "minLength": 1,
              "type": "string"
            },
            "source": {
              "minLength": 1,
              "type": "string"
            },
            "specversion": {
              "const": "1.0",
              "default": "1.0",
              "type": "string"
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
            "time": {
              "type": "string"
            },
            "type": {
              "const": "io.riverhog.riverhog.archive_copy.issue",
              "type": "string"
            }
          },
          "required": [
            "id",
            "source",
            "type",
            "time",
            "data"
          ],
          "type": "object"
        },
        "ArchiveCopyRequestedData": {
          "additionalProperties": false,
          "properties": {
            "actor": {
              "$ref": "#/$defs/RiverhogActor"
            },
            "cause": {
              "anyOf": [
                {
                  "$ref": "#/$defs/RiverhogEventCause"
                },
                {
                  "type": "null"
                }
              ],
              "default": null
            },
            "collection_created_at": {
              "maxLength": 64,
              "minLength": 1,
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
        "ArchiveCopyRequestedEvent": {
          "additionalProperties": false,
          "properties": {
            "data": {
              "$ref": "#/$defs/ArchiveCopyRequestedData"
            },
            "datacontenttype": {
              "const": "application/json",
              "default": "application/json",
              "type": "string"
            },
            "id": {
              "minLength": 1,
              "type": "string"
            },
            "source": {
              "minLength": 1,
              "type": "string"
            },
            "specversion": {
              "const": "1.0",
              "default": "1.0",
              "type": "string"
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
            "time": {
              "type": "string"
            },
            "type": {
              "const": "io.riverhog.riverhog.archive_copy.requested",
              "type": "string"
            }
          },
          "required": [
            "id",
            "source",
            "type",
            "time",
            "data"
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
            "cause": {
              "anyOf": [
                {
                  "$ref": "#/$defs/RiverhogEventCause"
                },
                {
                  "type": "null"
                }
              ],
              "default": null
            },
            "collection_created_at": {
              "maxLength": 64,
              "minLength": 1,
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
            "data": {
              "$ref": "#/$defs/CollectionDeletedData"
            },
            "datacontenttype": {
              "const": "application/json",
              "default": "application/json",
              "type": "string"
            },
            "id": {
              "minLength": 1,
              "type": "string"
            },
            "source": {
              "minLength": 1,
              "type": "string"
            },
            "specversion": {
              "const": "1.0",
              "default": "1.0",
              "type": "string"
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
            "time": {
              "type": "string"
            },
            "type": {
              "const": "io.riverhog.riverhog.collection.deleted",
              "type": "string"
            }
          },
          "required": [
            "id",
            "source",
            "type",
            "time",
            "data"
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
            "cause": {
              "anyOf": [
                {
                  "$ref": "#/$defs/RiverhogEventCause"
                },
                {
                  "type": "null"
                }
              ],
              "default": null
            },
            "collection_created_at": {
              "maxLength": 64,
              "minLength": 1,
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
            "data": {
              "$ref": "#/$defs/CollectionFinalizedData"
            },
            "datacontenttype": {
              "const": "application/json",
              "default": "application/json",
              "type": "string"
            },
            "id": {
              "minLength": 1,
              "type": "string"
            },
            "source": {
              "minLength": 1,
              "type": "string"
            },
            "specversion": {
              "const": "1.0",
              "default": "1.0",
              "type": "string"
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
            "time": {
              "type": "string"
            },
            "type": {
              "const": "io.riverhog.riverhog.collection.finalized",
              "type": "string"
            }
          },
          "required": [
            "id",
            "source",
            "type",
            "time",
            "data"
          ],
          "type": "object"
        },
        "CollectionId": {
          "minimum": 1,
          "type": "integer"
        },
        "LifecycleEventCursor": {
          "maxLength": 19,
          "minLength": 1,
          "pattern": "^(?:0|[1-9][0-9]*)$",
          "type": "string"
        },
        "RetrievalCanceledData": {
          "additionalProperties": false,
          "properties": {
            "actor": {
              "$ref": "#/$defs/RiverhogActor"
            },
            "cause": {
              "anyOf": [
                {
                  "$ref": "#/$defs/RiverhogEventCause"
                },
                {
                  "type": "null"
                }
              ],
              "default": null
            },
            "collection_created_at": {
              "anyOf": [
                {
                  "maxLength": 64,
                  "minLength": 1,
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
            "data": {
              "$ref": "#/$defs/RetrievalCanceledData"
            },
            "datacontenttype": {
              "const": "application/json",
              "default": "application/json",
              "type": "string"
            },
            "id": {
              "minLength": 1,
              "type": "string"
            },
            "source": {
              "minLength": 1,
              "type": "string"
            },
            "specversion": {
              "const": "1.0",
              "default": "1.0",
              "type": "string"
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
            "time": {
              "type": "string"
            },
            "type": {
              "const": "io.riverhog.riverhog.retrieval.canceled",
              "type": "string"
            }
          },
          "required": [
            "id",
            "source",
            "type",
            "time",
            "data"
          ],
          "type": "object"
        },
        "RetrievalCompletedData": {
          "additionalProperties": false,
          "properties": {
            "actor": {
              "$ref": "#/$defs/RiverhogActor"
            },
            "cause": {
              "anyOf": [
                {
                  "$ref": "#/$defs/RiverhogEventCause"
                },
                {
                  "type": "null"
                }
              ],
              "default": null
            },
            "collection_created_at": {
              "anyOf": [
                {
                  "maxLength": 64,
                  "minLength": 1,
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
            "data": {
              "$ref": "#/$defs/RetrievalCompletedData"
            },
            "datacontenttype": {
              "const": "application/json",
              "default": "application/json",
              "type": "string"
            },
            "id": {
              "minLength": 1,
              "type": "string"
            },
            "source": {
              "minLength": 1,
              "type": "string"
            },
            "specversion": {
              "const": "1.0",
              "default": "1.0",
              "type": "string"
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
            "time": {
              "type": "string"
            },
            "type": {
              "const": "io.riverhog.riverhog.retrieval.completed",
              "type": "string"
            }
          },
          "required": [
            "id",
            "source",
            "type",
            "time",
            "data"
          ],
          "type": "object"
        },
        "RetrievalExpiredData": {
          "additionalProperties": false,
          "properties": {
            "actor": {
              "$ref": "#/$defs/RiverhogActor"
            },
            "cause": {
              "anyOf": [
                {
                  "$ref": "#/$defs/RiverhogEventCause"
                },
                {
                  "type": "null"
                }
              ],
              "default": null
            },
            "collection_created_at": {
              "anyOf": [
                {
                  "maxLength": 64,
                  "minLength": 1,
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
            "data": {
              "$ref": "#/$defs/RetrievalExpiredData"
            },
            "datacontenttype": {
              "const": "application/json",
              "default": "application/json",
              "type": "string"
            },
            "id": {
              "minLength": 1,
              "type": "string"
            },
            "source": {
              "minLength": 1,
              "type": "string"
            },
            "specversion": {
              "const": "1.0",
              "default": "1.0",
              "type": "string"
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
            "time": {
              "type": "string"
            },
            "type": {
              "const": "io.riverhog.riverhog.retrieval.expired",
              "type": "string"
            }
          },
          "required": [
            "id",
            "source",
            "type",
            "time",
            "data"
          ],
          "type": "object"
        },
        "RetrievalFailedData": {
          "additionalProperties": false,
          "properties": {
            "actor": {
              "$ref": "#/$defs/RiverhogActor"
            },
            "cause": {
              "anyOf": [
                {
                  "$ref": "#/$defs/RiverhogEventCause"
                },
                {
                  "type": "null"
                }
              ],
              "default": null
            },
            "collection_created_at": {
              "anyOf": [
                {
                  "maxLength": 64,
                  "minLength": 1,
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
            "data": {
              "$ref": "#/$defs/RetrievalFailedData"
            },
            "datacontenttype": {
              "const": "application/json",
              "default": "application/json",
              "type": "string"
            },
            "id": {
              "minLength": 1,
              "type": "string"
            },
            "source": {
              "minLength": 1,
              "type": "string"
            },
            "specversion": {
              "const": "1.0",
              "default": "1.0",
              "type": "string"
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
            "time": {
              "type": "string"
            },
            "type": {
              "const": "io.riverhog.riverhog.retrieval.failed",
              "type": "string"
            }
          },
          "required": [
            "id",
            "source",
            "type",
            "time",
            "data"
          ],
          "type": "object"
        },
        "RetrievalIssueData": {
          "additionalProperties": false,
          "properties": {
            "actor": {
              "$ref": "#/$defs/RiverhogActor"
            },
            "cause": {
              "anyOf": [
                {
                  "$ref": "#/$defs/RiverhogEventCause"
                },
                {
                  "type": "null"
                }
              ],
              "default": null
            },
            "collection_created_at": {
              "anyOf": [
                {
                  "maxLength": 64,
                  "minLength": 1,
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
            "data": {
              "$ref": "#/$defs/RetrievalIssueData"
            },
            "datacontenttype": {
              "const": "application/json",
              "default": "application/json",
              "type": "string"
            },
            "id": {
              "minLength": 1,
              "type": "string"
            },
            "source": {
              "minLength": 1,
              "type": "string"
            },
            "specversion": {
              "const": "1.0",
              "default": "1.0",
              "type": "string"
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
            "time": {
              "type": "string"
            },
            "type": {
              "const": "io.riverhog.riverhog.retrieval.issue",
              "type": "string"
            }
          },
          "required": [
            "id",
            "source",
            "type",
            "time",
            "data"
          ],
          "type": "object"
        },
        "RetrievalReadyData": {
          "additionalProperties": false,
          "properties": {
            "actor": {
              "$ref": "#/$defs/RiverhogActor"
            },
            "cause": {
              "anyOf": [
                {
                  "$ref": "#/$defs/RiverhogEventCause"
                },
                {
                  "type": "null"
                }
              ],
              "default": null
            },
            "collection_created_at": {
              "anyOf": [
                {
                  "maxLength": 64,
                  "minLength": 1,
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
              "maxLength": 64,
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
            "data": {
              "$ref": "#/$defs/RetrievalReadyData"
            },
            "datacontenttype": {
              "const": "application/json",
              "default": "application/json",
              "type": "string"
            },
            "id": {
              "minLength": 1,
              "type": "string"
            },
            "source": {
              "minLength": 1,
              "type": "string"
            },
            "specversion": {
              "const": "1.0",
              "default": "1.0",
              "type": "string"
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
            "time": {
              "type": "string"
            },
            "type": {
              "const": "io.riverhog.riverhog.retrieval.ready",
              "type": "string"
            }
          },
          "required": [
            "id",
            "source",
            "type",
            "time",
            "data"
          ],
          "type": "object"
        },
        "RetrievalRenewedData": {
          "additionalProperties": false,
          "properties": {
            "actor": {
              "$ref": "#/$defs/RiverhogActor"
            },
            "cause": {
              "anyOf": [
                {
                  "$ref": "#/$defs/RiverhogEventCause"
                },
                {
                  "type": "null"
                }
              ],
              "default": null
            },
            "collection_created_at": {
              "anyOf": [
                {
                  "maxLength": 64,
                  "minLength": 1,
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
              "maxLength": 64,
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
            "data": {
              "$ref": "#/$defs/RetrievalRenewedData"
            },
            "datacontenttype": {
              "const": "application/json",
              "default": "application/json",
              "type": "string"
            },
            "id": {
              "minLength": 1,
              "type": "string"
            },
            "source": {
              "minLength": 1,
              "type": "string"
            },
            "specversion": {
              "const": "1.0",
              "default": "1.0",
              "type": "string"
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
            "time": {
              "type": "string"
            },
            "type": {
              "const": "io.riverhog.riverhog.retrieval.renewed",
              "type": "string"
            }
          },
          "required": [
            "id",
            "source",
            "type",
            "time",
            "data"
          ],
          "type": "object"
        },
        "RetrievalRequestedData": {
          "additionalProperties": false,
          "properties": {
            "actor": {
              "$ref": "#/$defs/RiverhogActor"
            },
            "cause": {
              "anyOf": [
                {
                  "$ref": "#/$defs/RiverhogEventCause"
                },
                {
                  "type": "null"
                }
              ],
              "default": null
            },
            "collection_created_at": {
              "anyOf": [
                {
                  "maxLength": 64,
                  "minLength": 1,
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
            "data": {
              "$ref": "#/$defs/RetrievalRequestedData"
            },
            "datacontenttype": {
              "const": "application/json",
              "default": "application/json",
              "type": "string"
            },
            "id": {
              "minLength": 1,
              "type": "string"
            },
            "source": {
              "minLength": 1,
              "type": "string"
            },
            "specversion": {
              "const": "1.0",
              "default": "1.0",
              "type": "string"
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
            "time": {
              "type": "string"
            },
            "type": {
              "const": "io.riverhog.riverhog.retrieval.requested",
              "type": "string"
            }
          },
          "required": [
            "id",
            "source",
            "type",
            "time",
            "data"
          ],
          "type": "object"
        },
        "RiverhogActor": {
          "additionalProperties": false,
          "properties": {
            "app": {
              "maxLength": 160,
              "minLength": 1,
              "type": "string"
            },
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
            }
          },
          "required": [
            "app"
          ],
          "type": "object"
        },
        "RiverhogEventCause": {
          "additionalProperties": false,
          "properties": {
            "id": {
              "maxLength": 300,
              "minLength": 1,
              "type": "string"
            },
            "source": {
              "maxLength": 1000,
              "minLength": 1,
              "type": "string"
            },
            "subject": {
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
            "type": {
              "maxLength": 300,
              "minLength": 1,
              "type": "string"
            }
          },
          "required": [
            "id",
            "source",
            "type"
          ],
          "type": "object"
        },
        "RiverhogLifecycleEvent": {
          "discriminator": {
            "mapping": {
              "io.riverhog.riverhog.archive_copy.canceled": "#/$defs/ArchiveCopyCanceledEvent",
              "io.riverhog.riverhog.archive_copy.completed": "#/$defs/ArchiveCopyCompletedEvent",
              "io.riverhog.riverhog.archive_copy.issue": "#/$defs/ArchiveCopyIssueEvent",
              "io.riverhog.riverhog.archive_copy.requested": "#/$defs/ArchiveCopyRequestedEvent",
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
              "$ref": "#/$defs/ArchiveCopyRequestedEvent"
            },
            {
              "$ref": "#/$defs/ArchiveCopyCompletedEvent"
            },
            {
              "$ref": "#/$defs/ArchiveCopyIssueEvent"
            },
            {
              "$ref": "#/$defs/ArchiveCopyCanceledEvent"
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
