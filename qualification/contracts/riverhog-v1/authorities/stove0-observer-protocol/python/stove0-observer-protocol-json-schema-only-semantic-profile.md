# stove0_observer_protocol.JSON_SCHEMA_ONLY_SEMANTIC_PROFILE

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-observer-protocol:stove0-observer-protocol-json-schema-only-542bb9350a:e0dde9cb2d -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-observer-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-204792a5cb"></a>
- <a id="s-c44f53fb75"></a>`distribution`: `stove0-observer-protocol`
- <a id="s-b40e0c02aa"></a>`module`: `stove0_observer_protocol`
- <a id="s-2187c915d6"></a>`name`: `JSON_SCHEMA_ONLY_SEMANTIC_PROFILE`
- <a id="s-b3fe0edae1"></a>`unit`: `export`

### Declared structure

- <a id="s-311f3c9382"></a>`kind`: `"object"`
- <a id="s-ae139a1c63"></a>`type`: `"stove0_protocol.models.SemanticValidationProfile"`

## Governing policies

- <a id="pa-556eb6f6cd"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-observer-protocol:stove0_observer_protocol](../../../evidence/sources/authorities.md#src-62450e0156) — [some-implementations/stove0/packages/observer-protocol/src/stove0\_observer\_protocol/\_\_init\_\_.py](../../../../../../some-implementations/stove0/packages/observer-protocol/src/stove0_observer_protocol/__init__.py)

### Machine authority

- `/external_contract/python/stove0_observer_protocol.JSON_SCHEMA_ONLY_SEMANTIC_PROFILE`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 4c5f71ce0ba08ad600f5d7fbb833d255881cb379f1bb3c810275a257d0cb0591 -->

```json
{
  "contract": {
    "kind": "object",
    "type": "stove0_protocol.models.SemanticValidationProfile"
  },
  "distribution": "stove0-observer-protocol",
  "module": "stove0_observer_protocol",
  "name": "JSON_SCHEMA_ONLY_SEMANTIC_PROFILE",
  "unit": "export"
}
```

</details>
