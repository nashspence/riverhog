# riverhog_protocol.collection_tag_sha256

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-protocol:riverhog-protocol-collection-tag-sha256:591ce80b2a -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-14fd43f47f"></a>
- <a id="s-bec6520f27"></a>`distribution`: `riverhog-protocol`
- <a id="s-4c98bedd86"></a>`module`: `riverhog_protocol`
- <a id="s-a4aee47be6"></a>`name`: `collection_tag_sha256`
- <a id="s-f3d11ac3a0"></a>`unit`: `export`

### Declared structure

- <a id="s-33363a774d"></a>`kind`: `"function"`
- <a id="s-bed99968c4"></a>`signature`: `"\"(tag: 'str') -> 'str'\""`

## Governing policies

- <a id="pa-76ec623c1f"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-protocol:riverhog_protocol](../../../evidence/sources.md#src-19e35f15d9) — `packages/riverhog-protocol/src/riverhog_protocol/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_protocol.collection_tag_sha256`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 61075ba2f5ded051f77cdea7eaa9a3be562db7836403dec89960b21bd4bbf564 -->

```json
{
  "contract": {
    "kind": "function",
    "signature": "\"(tag: 'str') -> 'str'\""
  },
  "distribution": "riverhog-protocol",
  "module": "riverhog_protocol",
  "name": "collection_tag_sha256",
  "unit": "export"
}
```
