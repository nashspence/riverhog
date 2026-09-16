# riverhog_protocol.CollectionTagHeadDocument.from_json_bytes

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-protocol:riverhog-protocol-collectiontagheaddocume-888d452b31:c6085701f1 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-4566912702"></a>
- <a id="s-6930719cad"></a>`distribution`: `riverhog-protocol`
- <a id="s-442866dd4b"></a>`module`: `riverhog_protocol`
- <a id="s-f66c171335"></a>`name`: `from_json_bytes`
- <a id="s-562bd90545"></a>`owner`: `riverhog_protocol.CollectionTagHeadDocument`
- <a id="s-17aed3b6d3"></a>`unit`: `member`

### Declared structure

- <a id="s-6e5c1c06c8"></a>`kind`: `"classmethod"`
- <a id="s-e994040b1e"></a>`signature`: `"\"(cls, content: 'bytes \| str') -> 'CollectionTagHeadDocument'\""`

## Maintained corroboration

### Related interface records

- [CollectionTagHeadDocument](riverhog-protocol-collectiontagheaddocument.md)

## Governing policies

- <a id="pa-d62a89be54"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-protocol:riverhog_protocol](../../../evidence/sources.md#src-19e35f15d9) — `packages/riverhog-protocol/src/riverhog_protocol/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_protocol.CollectionTagHeadDocument.from_json_bytes`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 7b0d8e8c1a2e9f580b6687865f480197484990c28bc1d9489323e87ee2019f1f -->

```json
{
  "contract": {
    "kind": "classmethod",
    "signature": "\"(cls, content: 'bytes | str') -> 'CollectionTagHeadDocument'\""
  },
  "distribution": "riverhog-protocol",
  "module": "riverhog_protocol",
  "name": "from_json_bytes",
  "owner": "riverhog_protocol.CollectionTagHeadDocument",
  "unit": "member"
}
```

</details>
