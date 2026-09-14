# riverhog_protocol.portable_collection_inventory_identity

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-protocol:riverhog-protocol-portable-collection-inv-b45c1c6434:df29a693b9 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-39b1b576e6"></a>
- <a id="s-cb85d7f52a"></a>`distribution`: `riverhog-protocol`
- <a id="s-f695a51b07"></a>`module`: `riverhog_protocol`
- <a id="s-d644f6eaaf"></a>`name`: `portable_collection_inventory_identity`
- <a id="s-13c45602b4"></a>`unit`: `export`

### Declared structure

- <a id="s-04233a979f"></a>`kind`: `"function"`
- <a id="s-8c2567eb4a"></a>`signature`: `"\"(header: 'PortableCollectionHeader', files: 'Iterable[PortableCollectionFile]') -> 'str'\""`

## Governing policies

- <a id="pa-c991182454"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-protocol:riverhog_protocol](../../../evidence/sources.md#src-19e35f15d9) — `packages/riverhog-protocol/src/riverhog_protocol/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_protocol.portable_collection_inventory_identity`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 2d684dfdc68650ca3422305627fad5b0567c35dd2927088e3cf191e01eb7469b -->

```json
{
  "contract": {
    "kind": "function",
    "signature": "\"(header: 'PortableCollectionHeader', files: 'Iterable[PortableCollectionFile]') -> 'str'\""
  },
  "distribution": "riverhog-protocol",
  "module": "riverhog_protocol",
  "name": "portable_collection_inventory_identity",
  "unit": "export"
}
```
