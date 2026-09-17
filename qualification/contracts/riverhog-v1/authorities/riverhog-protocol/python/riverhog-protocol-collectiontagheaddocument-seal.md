# riverhog_protocol.CollectionTagHeadDocument.seal

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-protocol:riverhog-protocol-collectiontagheaddocument-seal:64166460cb -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-0c53023383"></a>
- <a id="s-4a829630de"></a>`distribution`: `riverhog-protocol`
- <a id="s-adf2d807b1"></a>`module`: `riverhog_protocol`
- <a id="s-964b099f3d"></a>`name`: `seal`
- <a id="s-5a3713e66a"></a>`owner`: `riverhog_protocol.CollectionTagHeadDocument`
- <a id="s-b6791bb3a5"></a>`unit`: `member`

### Declared structure

- <a id="s-a30af4e594"></a>`kind`: `"classmethod"`
- <a id="s-c29d578710"></a>`signature`: `"\"(cls, *, archive_root_sha256: 'str', revision: 'int', root_sha256: 'str \| None') -> 'CollectionTagHeadDocument'\""`

## Maintained corroboration

### Related interface records

- [CollectionTagHeadDocument](riverhog-protocol-collectiontagheaddocument.md)

## Governing policies

- <a id="pa-5cc0f63520"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:riverhog-protocol:riverhog_protocol](../../../evidence/sources.md#src-19e35f15d9) — [packages/riverhog-protocol/src/riverhog\_protocol/\_\_init\_\_.py](../../../../../../packages/riverhog-protocol/src/riverhog_protocol/__init__.py)

### Machine authority

- `/external_contract/python/riverhog_protocol.CollectionTagHeadDocument.seal`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 8df8805bc04d265b446df5722cd8a29e8e7ced3c4a87402bc6e3484d2c6a9838 -->

```json
{
  "contract": {
    "kind": "classmethod",
    "signature": "\"(cls, *, archive_root_sha256: 'str', revision: 'int', root_sha256: 'str | None') -> 'CollectionTagHeadDocument'\""
  },
  "distribution": "riverhog-protocol",
  "module": "riverhog_protocol",
  "name": "seal",
  "owner": "riverhog_protocol.CollectionTagHeadDocument",
  "unit": "member"
}
```

</details>
