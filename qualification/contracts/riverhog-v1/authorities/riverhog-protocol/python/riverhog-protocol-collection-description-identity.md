# riverhog_protocol.collection_description_identity

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-protocol:riverhog-protocol-collection-description-identity:cb4d3a2314 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-7760fe4067"></a>
- <a id="s-085d3b6b9e"></a>`distribution`: `riverhog-protocol`
- <a id="s-2935bd73cc"></a>`module`: `riverhog_protocol`
- <a id="s-b770cc32ca"></a>`name`: `collection_description_identity`
- <a id="s-7ce289e30f"></a>`unit`: `export`

### Declared structure

- <a id="s-94054e4e7f"></a>`kind`: `"function"`
- <a id="s-95a98ce595"></a>`signature`: `"\"(*, archive_root_sha256: 'str', revision: 'int', description: 'str \| None') -> 'str'\""`

## Governing policies

- <a id="pa-2dc63e5be1"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-protocol:riverhog_protocol](../../../evidence/sources.md#src-19e35f15d9) — `packages/riverhog-protocol/src/riverhog_protocol/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_protocol.collection_description_identity`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: b30f2c78beeb71d95d3e56b77fb934ff98a635742f13143e3306250751ed6699 -->

```json
{
  "contract": {
    "kind": "function",
    "signature": "\"(*, archive_root_sha256: 'str', revision: 'int', description: 'str | None') -> 'str'\""
  },
  "distribution": "riverhog-protocol",
  "module": "riverhog_protocol",
  "name": "collection_description_identity",
  "unit": "export"
}
```
