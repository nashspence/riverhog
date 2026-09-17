# riverhog_protocol.CollectionUploadArtifactCustodyReceiptDocument.seal

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-protocol:riverhog-protocol-collectionuploadartifac-367fd0c938:7441f9aea9 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [riverhog-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-f97abbb68a"></a>
- <a id="s-09dcabcaf2"></a>`distribution`: `riverhog-protocol`
- <a id="s-3ea5a903e1"></a>`module`: `riverhog_protocol`
- <a id="s-1733064083"></a>`name`: `seal`
- <a id="s-a377cd26bf"></a>`owner`: `riverhog_protocol.CollectionUploadArtifactCustodyReceiptDocument`
- <a id="s-c1da251fc0"></a>`unit`: `member`

### Declared structure

- <a id="s-40953a74e1"></a>`kind`: `"classmethod"`
- <a id="s-e6d77a77da"></a>`signature`: `"\"(cls, *, collection_id: 'int', path: 'str', bytes: 'int', sha256: 'str', archive_objects: 'Sequence[CollectionUploadCustodyObjectDocument]') -> 'CollectionUploadArtifactCustodyReceiptDocument'\""`

## Maintained corroboration

### Related interface records

- [CollectionUploadArtifactCustodyReceiptDocument](riverhog-protocol-collectionuploadartifactcustodyreceiptdocument.md)

## Governing policies

- <a id="pa-5df9222429"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:riverhog-protocol:riverhog_protocol](../../../evidence/sources/authorities.md#src-19e35f15d9) — [packages/riverhog-protocol/src/riverhog\_protocol/\_\_init\_\_.py](../../../../../../packages/riverhog-protocol/src/riverhog_protocol/__init__.py)

### Machine authority

- `/external_contract/python/riverhog_protocol.CollectionUploadArtifactCustodyReceiptDocument.seal`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: ba188db0032647d41ab92b603be2da5a79d9a472ad13a616d9a8c7f26184ed1a -->

```json
{
  "contract": {
    "kind": "classmethod",
    "signature": "\"(cls, *, collection_id: 'int', path: 'str', bytes: 'int', sha256: 'str', archive_objects: 'Sequence[CollectionUploadCustodyObjectDocument]') -> 'CollectionUploadArtifactCustodyReceiptDocument'\""
  },
  "distribution": "riverhog-protocol",
  "module": "riverhog_protocol",
  "name": "seal",
  "owner": "riverhog_protocol.CollectionUploadArtifactCustodyReceiptDocument",
  "unit": "member"
}
```

</details>
