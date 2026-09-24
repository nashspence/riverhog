# stove0_core.DepartureEffectService.advance

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-server:stove0-core-departureeffectservice-advance:aeb6b3da38 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-server](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-39ee4b83ed"></a>
- <a id="s-221b967aba"></a>`distribution`: `stove0-server`
- <a id="s-636e455798"></a>`module`: `stove0_core`
- <a id="s-d8b5284d33"></a>`name`: `advance`
- <a id="s-fb5023edd3"></a>`owner`: `stove0_core.DepartureEffectService`
- <a id="s-ae14bf5548"></a>`unit`: `member`

### Declared structure

- <a id="s-ba590331e7"></a>`kind`: `"method"`
- <a id="s-99b4605cb7"></a>`signature`: `"\"(self, *, limit: 'int' = 25) -> 'DepartureRun'\""`

## Maintained corroboration

### Related interface records

- [DepartureEffectService](stove0-core-departureeffectservice.md)

## Governing policies

- <a id="pa-72a96db66e"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-server:stove0_core](../../../evidence/sources/authorities.md#src-7558b08e7f) — [some-implementations/stove0/application/server/src/stove0\_core/\_\_init\_\_.py](../../../../../../some-implementations/stove0/application/server/src/stove0_core/__init__.py)

### Machine authority

- `/external_contract/python/stove0_core.DepartureEffectService.advance`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 989dacbdb9c83eadb73d8b7860a49c7567edb8f71b8d2586a3ffdc49021b543e -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, *, limit: 'int' = 25) -> 'DepartureRun'\""
  },
  "distribution": "stove0-server",
  "module": "stove0_core",
  "name": "advance",
  "owner": "stove0_core.DepartureEffectService",
  "unit": "member"
}
```

</details>
