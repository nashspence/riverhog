# riverhog_protocol.collection_tag_head_identity

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-protocol:riverhog-protocol-collection-tag-head-identity:ba052072b9 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-e75f7b3ff1"></a>
- <a id="s-69ea20b9f5"></a>`distribution`: `riverhog-protocol`
- <a id="s-ca81c7dd52"></a>`module`: `riverhog_protocol`
- <a id="s-2af8d7d953"></a>`name`: `collection_tag_head_identity`
- <a id="s-a4206d12df"></a>`unit`: `export`

### Declared structure

- <a id="s-3ad3cbf631"></a>`kind`: `"function"`
- <a id="s-39c8ce60f6"></a>`signature`: `"\"(*, archive_root_sha256: 'str', revision: 'int', root_sha256: 'str \| None', tag_set_identity: 'str') -> 'str'\""`

## Governing policies

- <a id="pa-dcd0baf38d"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-protocol:riverhog_protocol](../../../evidence/sources.md#src-19e35f15d9) — `packages/riverhog-protocol/src/riverhog_protocol/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_protocol.collection_tag_head_identity`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 592e372049e1e2d4327266ca472b10b9d0f4468c8e5aa6d8316fdb5e8293fbaf -->

```json
{
  "contract": {
    "kind": "function",
    "signature": "\"(*, archive_root_sha256: 'str', revision: 'int', root_sha256: 'str | None', tag_set_identity: 'str') -> 'str'\""
  },
  "distribution": "riverhog-protocol",
  "module": "riverhog_protocol",
  "name": "collection_tag_head_identity",
  "unit": "export"
}
```

</details>
