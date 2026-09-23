# stove0_core.Stove0StateError

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-server:stove0-core-stove0stateerror:d11294ca64 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-server](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-3090c70020"></a>
- <a id="s-f35b09f26d"></a>`distribution`: `stove0-server`
- <a id="s-2feedf68fd"></a>`module`: `stove0_core`
- <a id="s-cc61bcbd57"></a>`name`: `Stove0StateError`
- <a id="s-d0b004aa98"></a>`unit`: `export`

### Declared structure

- <a id="s-f087a2e828"></a>`kind`: `"class"`
- <a id="s-0e0168f2cb"></a>`signature`: `"unavailable"`

## Governing policies

- <a id="pa-8a607ab0b0"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-server:stove0_core](../../../evidence/sources/authorities.md#src-7558b08e7f) — [some-implementations/stove0/application/server/src/stove0\_core/\_\_init\_\_.py](../../../../../../some-implementations/stove0/application/server/src/stove0_core/__init__.py)

### Machine authority

- `/external_contract/python/stove0_core.Stove0StateError`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 4ea5205f71fe19409006c5683462fd68b71eed7a09964d64c8c68e60f580e73f -->

```json
{
  "contract": {
    "kind": "class",
    "signature": "unavailable"
  },
  "distribution": "stove0-server",
  "module": "stove0_core",
  "name": "Stove0StateError",
  "unit": "export"
}
```

</details>
