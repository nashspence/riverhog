# stove0_core.WorkPhase

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-server:stove0-core-workphase:65d8ff221a -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-server](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-e489996d2b"></a>
- <a id="s-9ab6a38a12"></a>`distribution`: `stove0-server`
- <a id="s-7a1f352fe1"></a>`module`: `stove0_core`
- <a id="s-93cfc5340c"></a>`name`: `WorkPhase`
- <a id="s-4f7e72219b"></a>`unit`: `export`

### Declared structure

- <a id="s-2f28c2d8f5"></a>`kind`: `"object"`
- <a id="s-5346a83d8a"></a>`type`: `"typing._LiteralGenericAlias"`

## Governing policies

- <a id="pa-31a39f6b5e"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-server:stove0_core](../../../evidence/sources/authorities.md#src-7558b08e7f) — [some-implementations/stove0/application/server/src/stove0\_core/\_\_init\_\_.py](../../../../../../some-implementations/stove0/application/server/src/stove0_core/__init__.py)

### Machine authority

- `/external_contract/python/stove0_core.WorkPhase`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 198c320cdda1802919b9429c7f2e44e179eec2cd18af4f2309690118a815f562 -->

```json
{
  "contract": {
    "kind": "object",
    "type": "typing._LiteralGenericAlias"
  },
  "distribution": "stove0-server",
  "module": "stove0_core",
  "name": "WorkPhase",
  "unit": "export"
}
```

</details>
