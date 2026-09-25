# stove0_core.load_stove0_config

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-server:stove0-core-load-stove0-config:124325b4fc -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-server](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-86d36422a8"></a>
- <a id="s-ce055de40e"></a>`distribution`: `stove0-server`
- <a id="s-8175cd570c"></a>`module`: `stove0_core`
- <a id="s-b1e5404963"></a>`name`: `load_stove0_config`
- <a id="s-0a4ead66c7"></a>`unit`: `export`

### Declared structure

- <a id="s-1978b6be5c"></a>`kind`: `"function"`
- <a id="s-27f7152481"></a>`signature`: `"\"(path: 'Path \| None' = None, *, require_api_token: 'bool' = True) -> 'Stove0RuntimeConfig'\""`

## Governing policies

- <a id="pa-e5317703c4"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-server:stove0_core](../../../evidence/sources/authorities.md#src-7558b08e7f) — [some-implementations/stove0/application/server/src/stove0\_core/\_\_init\_\_.py](../../../../../../some-implementations/stove0/application/server/src/stove0_core/__init__.py)

### Machine authority

- `/external_contract/python/stove0_core.load_stove0_config`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 8bd5a5cc296b0ee411e5e91c1840682ed5ceb792aa0cbb75658a0dfcef3cb720 -->

```json
{
  "contract": {
    "kind": "function",
    "signature": "\"(path: 'Path | None' = None, *, require_api_token: 'bool' = True) -> 'Stove0RuntimeConfig'\""
  },
  "distribution": "stove0-server",
  "module": "stove0_core",
  "name": "load_stove0_config",
  "unit": "export"
}
```

</details>
