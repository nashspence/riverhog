# stove0_core.stove0_state_schema

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-server:stove0-core-stove0-state-schema:2aac8f9086 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-server](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-110f63371e"></a>
- <a id="s-885f9858d2"></a>`distribution`: `stove0-server`
- <a id="s-f6b26b19f4"></a>`module`: `stove0_core`
- <a id="s-cc448bc9b3"></a>`name`: `stove0_state_schema`
- <a id="s-f87677a890"></a>`unit`: `export`

### Declared structure

- <a id="s-a40392ee72"></a>`kind`: `"function"`
- <a id="s-40fde0f0c6"></a>`signature`: `"\"(database_url: 'str') -> 'StateSchema'\""`

## Governing policies

- <a id="pa-cb283bd8e0"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-server:stove0_core](../../../evidence/sources/authorities.md#src-7558b08e7f) — [some-implementations/stove0/application/server/src/stove0\_core/\_\_init\_\_.py](../../../../../../some-implementations/stove0/application/server/src/stove0_core/__init__.py)

### Machine authority

- `/external_contract/python/stove0_core.stove0_state_schema`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 2a4eb8ae28047ea8a7a458eb3d0e0ec7dde46483fa7532ab08ee57d1411712f1 -->

```json
{
  "contract": {
    "kind": "function",
    "signature": "\"(database_url: 'str') -> 'StateSchema'\""
  },
  "distribution": "stove0-server",
  "module": "stove0_core",
  "name": "stove0_state_schema",
  "unit": "export"
}
```

</details>
