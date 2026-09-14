# riverhog_protocol.collection_tag_set_identity

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:riverhog-protocol:riverhog-protocol-collection-tag-set-identity:61b62e9ddf -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [riverhog-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-ea276e9869"></a>
- <a id="s-7f0b249cc4"></a>`distribution`: `riverhog-protocol`
- <a id="s-3d3984ec91"></a>`module`: `riverhog_protocol`
- <a id="s-873cec05fc"></a>`name`: `collection_tag_set_identity`
- <a id="s-0f0fbac612"></a>`unit`: `export`

### Declared structure

- <a id="s-b22d91d64e"></a>`kind`: `"function"`
- <a id="s-51d641ea21"></a>`signature`: `"\"(root_sha256: 'str \| None') -> 'str'\""`

## Governing policies

- <a id="pa-d215c5c85d"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:riverhog-protocol:riverhog_protocol](../../../evidence/sources.md#src-19e35f15d9) — `packages/riverhog-protocol/src/riverhog_protocol/__init__.py`

### Machine authority

- `/external_contract/python/riverhog_protocol.collection_tag_set_identity`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: a14553f576f9ab3650dc93d6d747da1da9971f130e64c3b0e1ac3233d702ea97 -->

```json
{
  "contract": {
    "kind": "function",
    "signature": "\"(root_sha256: 'str | None') -> 'str'\""
  },
  "distribution": "riverhog-protocol",
  "module": "riverhog_protocol",
  "name": "collection_tag_set_identity",
  "unit": "export"
}
```
