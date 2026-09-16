# riverhog_protocol.validate_collection_upload_artifact_custody_receipt

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-protocol:riverhog-protocol-validate-collection-upl-06b2879341:e4834aa230 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-c2acb1b217"></a>
- <a id="s-e9661832f3"></a>`distribution`: `riverhog-protocol`
- <a id="s-9ed593e0a9"></a>`module`: `riverhog_protocol`
- <a id="s-019111d634"></a>`name`: `validate_collection_upload_artifact_custody_receipt`
- <a id="s-94914d6718"></a>`unit`: `export`

### Declared structure

- <a id="s-217ed536fe"></a>`kind`: `"function"`
- <a id="s-202c7086bb"></a>`signature`: `"\"(collection_id: 'int', artifact: 'ImmutableFileIdentityDocument', receipt: 'CollectionUploadArtifactCustodyReceiptDocument') -> 'CollectionUploadArtifactCustodyReceiptDocument'\""`

## Governing policies

- <a id="pa-cb7221dbbb"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-protocol:riverhog_protocol](../../../evidence/sources.md#src-19e35f15d9) — `packages/riverhog-protocol/src/riverhog_protocol/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_protocol.validate_collection_upload_artifact_custody_receipt`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: b6d9c284dd48dc54c279915b86e25e21d745585f728681398c208b506449a176 -->

```json
{
  "contract": {
    "kind": "function",
    "signature": "\"(collection_id: 'int', artifact: 'ImmutableFileIdentityDocument', receipt: 'CollectionUploadArtifactCustodyReceiptDocument') -> 'CollectionUploadArtifactCustodyReceiptDocument'\""
  },
  "distribution": "riverhog-protocol",
  "module": "riverhog_protocol",
  "name": "validate_collection_upload_artifact_custody_receipt",
  "unit": "export"
}
```

</details>
