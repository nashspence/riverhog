# riverhog_protocol.validate_collection_upload_batch_against_registration_constraints

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-protocol:riverhog-protocol-validate-collection-upl-44358cbb29:3bbc85d35c -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-14e814f435"></a>
- <a id="s-9d147647bd"></a>`distribution`: `riverhog-protocol`
- <a id="s-9e2d9f4015"></a>`module`: `riverhog_protocol`
- <a id="s-8de028b5c8"></a>`name`: `validate_collection_upload_batch_against_registration_constraints`
- <a id="s-757165a854"></a>`unit`: `export`

### Declared structure

- <a id="s-ab12a8d0a6"></a>`kind`: `"function"`
- <a id="s-9e42ba41a2"></a>`signature`: `"\"(batch: 'CollectionUploadFileBatchDocument', constraints: 'CollectionUploadRegistrationConstraintsDocument') -> 'CollectionUploadFileBatchDocument'\""`

## Governing policies

- <a id="pa-23e8cd082c"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:riverhog-protocol:riverhog_protocol](../../../evidence/sources.md#src-19e35f15d9) — [packages/riverhog-protocol/src/riverhog\_protocol/\_\_init\_\_.py](../../../../../../packages/riverhog-protocol/src/riverhog_protocol/__init__.py)

### Machine authority

- `/external_contract/python/riverhog_protocol.validate_collection_upload_batch_against_registration_constraints`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: bce850981011267b62cc9848b603579680d6f96787226cfe760bfcc5f7f0a4f9 -->

```json
{
  "contract": {
    "kind": "function",
    "signature": "\"(batch: 'CollectionUploadFileBatchDocument', constraints: 'CollectionUploadRegistrationConstraintsDocument') -> 'CollectionUploadFileBatchDocument'\""
  },
  "distribution": "riverhog-protocol",
  "module": "riverhog_protocol",
  "name": "validate_collection_upload_batch_against_registration_constraints",
  "unit": "export"
}
```

</details>
