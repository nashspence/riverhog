# generated:riverhog-storage-adapter protocol

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: process-protocol:riverhog-storage-adapter-support:generated-riverhog-storage-adapter-protocol:59727f5df8 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-storage-adapter-support](../index.md) |
| Interface | [Process Protocol](index.md) |

## External contract

| Subject | Shape |
|---|---|
| <a id="s-4c3e2d1769"></a>`authorities` | additional keys=`http_operations`, `semantic_acceptance`, `structural_models` |
| <a id="s-73519fff77"></a>`bundle_sha256` | "ed6a7530471c36528ec0c944a389ae5b9f06bb3f643b45ff61c0f8bd47cc4ec6" |
| <a id="s-85201da1d0"></a>`compatibility` | additional keys=`provider_ontology`, `unknown_fields` |
| <a id="s-de29ee0e6e"></a>`format` | "riverhog-storage-adapter-schema-bundle/v1" |
| <a id="s-8343e7f06b"></a>`protocol` | "riverhog-storage-adapter/v1" |
| <a id="s-da65a692e5"></a>`semantic_acceptance` | additional keys=`conformance`, `kind` |

## Maintained corroboration

### Related interface records

- [GET /v1/adapter](../process-protocol-operations/get-v1-adapter.md)
- [POST /v1/objects/delete-prefix](../process-protocol-operations/post-v1-objects-delete-prefix.md)
- [POST /v1/objects/delete](../process-protocol-operations/post-v1-objects-delete.md)
- [POST /v1/objects/head](../process-protocol-operations/post-v1-objects-head.md)
- [POST /v1/objects/put](../process-protocol-operations/post-v1-objects-put.md)
- [POST /v1/objects/read](../process-protocol-operations/post-v1-objects-read.md)
- [POST /v1/reads/cleanup](../process-protocol-operations/post-v1-reads-cleanup.md)
- [POST /v1/reads/prepare](../process-protocol-operations/post-v1-reads-prepare.md)
- [POST /v1/reads/status](../process-protocol-operations/post-v1-reads-status.md)
- [POST /v1/writes/abort](../process-protocol-operations/post-v1-writes-abort.md)
- [POST /v1/writes/begin](../process-protocol-operations/post-v1-writes-begin.md)
- [POST /v1/writes/complete](../process-protocol-operations/post-v1-writes-complete.md)
- [POST /v1/writes/completed](../process-protocol-operations/post-v1-writes-completed.md)
- [POST /v1/writes/segment](../process-protocol-operations/post-v1-writes-segment.md)
- [POST /v1/writes/segments](../process-protocol-operations/post-v1-writes-segments.md)
- [generated:riverhog-storage-adapter: AdapterDescriptor](../process-protocol-schemas/generated-riverhog-storage-adapter-adapterdescriptor.md)
- [generated:riverhog-storage-adapter: CompletedWriteLookupRequest](../process-protocol-schemas/generated-riverhog-storage-adapter-completedwritelookuprequest.md)
- [generated:riverhog-storage-adapter: CompletedObjectReceipt](../process-protocol-schemas/generated-riverhog-storage-adapter-completedobjectreceipt.md)
- [generated:riverhog-storage-adapter: DeleteObjectRequest](../process-protocol-schemas/generated-riverhog-storage-adapter-deleteobjectrequest.md)
- [generated:riverhog-storage-adapter: DeletePrefixRequest](../process-protocol-schemas/generated-riverhog-storage-adapter-deleteprefixrequest.md)
- [generated:riverhog-storage-adapter: ImmutableObjectReceipt](../process-protocol-schemas/generated-riverhog-storage-adapter-immutableobjectreceipt.md)
- [generated:riverhog-storage-adapter: MaintenanceResult](../process-protocol-schemas/generated-riverhog-storage-adapter-maintenanceresult.md)
- [generated:riverhog-storage-adapter: ObjectMetadataReceipt](../process-protocol-schemas/generated-riverhog-storage-adapter-objectmetadatareceipt.md)
- [generated:riverhog-storage-adapter: ObjectHeadRequest](../process-protocol-schemas/generated-riverhog-storage-adapter-objectheadrequest.md)
- [generated:riverhog-storage-adapter: ObjectReadReceipt](../process-protocol-schemas/generated-riverhog-storage-adapter-objectreadreceipt.md)
- [generated:riverhog-storage-adapter: ObjectReadRequest](../process-protocol-schemas/generated-riverhog-storage-adapter-objectreadrequest.md)
- [generated:riverhog-storage-adapter: ReadPreparationRequest](../process-protocol-schemas/generated-riverhog-storage-adapter-readpreparationrequest.md)
- [generated:riverhog-storage-adapter: ReadStatus](../process-protocol-schemas/generated-riverhog-storage-adapter-readstatus.md)
- [generated:riverhog-storage-adapter: SmallObjectWriteRequest](../process-protocol-schemas/generated-riverhog-storage-adapter-smallobjectwriterequest.md)
- [generated:riverhog-storage-adapter: StorageAdapterError](../process-protocol-schemas/generated-riverhog-storage-adapter-storageadaptererror.md)
- [generated:riverhog-storage-adapter: StorageAdapterConformanceResult](../process-protocol-schemas/generated-riverhog-storage-adapter-storageadapterconformanceresult.md)
- [generated:riverhog-storage-adapter: WriteCompleteRequest](../process-protocol-schemas/generated-riverhog-storage-adapter-writecompleterequest.md)
- [generated:riverhog-storage-adapter: WriteSegmentRequest](../process-protocol-schemas/generated-riverhog-storage-adapter-writesegmentrequest.md)
- [generated:riverhog-storage-adapter: WriteSegmentListRequest](../process-protocol-schemas/generated-riverhog-storage-adapter-writesegmentlistrequest.md)
- [generated:riverhog-storage-adapter: WriteSegmentReceipt](../process-protocol-schemas/generated-riverhog-storage-adapter-writesegmentreceipt.md)
- [generated:riverhog-storage-adapter: WriteSegmentPage](../process-protocol-schemas/generated-riverhog-storage-adapter-writesegmentpage.md)
- [generated:riverhog-storage-adapter: WriteSession](../process-protocol-schemas/generated-riverhog-storage-adapter-writesession.md)
- [generated:riverhog-storage-adapter: WriteStartRequest](../process-protocol-schemas/generated-riverhog-storage-adapter-writestartrequest.md)

## Governing policies

- <a id="pa-de0df4bd5c"></a>[compatibility/components/v1](../../../policies/index.md#p-95e9a12259)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [protocol:generated:riverhog-storage-adapter](../../../evidence/sources.md#src-ef281f2471) — `packages/riverhog-storage-adapter-support/src/riverhog_storage_adapter_support/schemas.py::storage_adapter_schema_bundle`

### Machine authority

- `/external_contract/protocol_schemas/generated:riverhog-storage-adapter/authorities`
- `/external_contract/protocol_schemas/generated:riverhog-storage-adapter/bundle_sha256`
- `/external_contract/protocol_schemas/generated:riverhog-storage-adapter/compatibility`
- `/external_contract/protocol_schemas/generated:riverhog-storage-adapter/format`
- `/external_contract/protocol_schemas/generated:riverhog-storage-adapter/protocol`
- `/external_contract/protocol_schemas/generated:riverhog-storage-adapter/semantic_acceptance`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

### `/external_contract/protocol_schemas/generated:riverhog-storage-adapter/authorities`

<!-- exact-contract-value: 723bdc629c692bd71cc54f046e71fc791297a8c6a984b85d1f0962ee4c72194e -->

```json
{
  "http_operations": "http_binding.operations",
  "semantic_acceptance": "semantic_acceptance",
  "structural_models": "schemas"
}
```

### `/external_contract/protocol_schemas/generated:riverhog-storage-adapter/bundle_sha256`

<!-- exact-contract-value: bba1c0f37d1dd1ece4258a32aaacd0bd885304ea010108064dcbe6452200c749 -->

```json
"ed6a7530471c36528ec0c944a389ae5b9f06bb3f643b45ff61c0f8bd47cc4ec6"
```

### `/external_contract/protocol_schemas/generated:riverhog-storage-adapter/compatibility`

<!-- exact-contract-value: 5c7132cf2948fe916c565bddbbe06263bbac476702adb97867d0af3e9d24a284 -->

```json
{
  "provider_ontology": "private",
  "unknown_fields": "reject"
}
```

### `/external_contract/protocol_schemas/generated:riverhog-storage-adapter/format`

<!-- exact-contract-value: 0ee9723360880ab353d34848d40bebcae178bfc6996bc4ff87d6a79fe6c54109 -->

```json
"riverhog-storage-adapter-schema-bundle/v1"
```

### `/external_contract/protocol_schemas/generated:riverhog-storage-adapter/protocol`

<!-- exact-contract-value: 979c8622598850c976d2a3a4e6f0b9675ba24e5d65f84606d91ff53c8dee4833 -->

```json
"riverhog-storage-adapter/v1"
```

### `/external_contract/protocol_schemas/generated:riverhog-storage-adapter/semantic_acceptance`

<!-- exact-contract-value: ffedd3ad65cd966759c96e4bf07976fc6090a5f979142607868df5efed0c3cd8 -->

```json
{
  "conformance": "riverhog-storage-adapter-conformance-result/v1",
  "kind": "session-and-object-relations"
}
```
