# stove0_target_support.DepartureEffectHttpBinding

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-target-support:stove0-target-support-departureeffecthttpbinding:efef6abf92 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-target-support](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-e88fed8ed8"></a>
- <a id="s-e5fdbe4445"></a>`distribution`: `stove0-target-support`
- <a id="s-73c0177def"></a>`module`: `stove0_target_support`
- <a id="s-47d48646f3"></a>`name`: `DepartureEffectHttpBinding`
- <a id="s-a43e822a2a"></a>`unit`: `export`

### Declared structure

- <a id="s-63203e89eb"></a>`kind`: `"class"`
- <a id="s-4ca6fe5f55"></a>`signature`: `"\"(target: 'DepartureEffectTargetService', *, maximum_request_bytes: 'int' = 16777216) -> 'None'\""`

## Maintained corroboration

### Related interface records

- [handle](stove0-target-support-departureeffecthttpbinding-handle.md)

## Governing policies

- <a id="pa-588bee8914"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-target-support:stove0_target_support](../../../evidence/sources/authorities.md#src-3c01163237) — [some-implementations/stove0/packages/target-support/src/stove0\_target\_support/\_\_init\_\_.py](../../../../../../some-implementations/stove0/packages/target-support/src/stove0_target_support/__init__.py)

### Machine authority

- `/external_contract/python/stove0_target_support.DepartureEffectHttpBinding`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: e11e24c446030b00d3fa630a556bb01a9a5d50e4132be7b2140e3cb39909d188 -->

```json
{
  "contract": {
    "kind": "class",
    "signature": "\"(target: 'DepartureEffectTargetService', *, maximum_request_bytes: 'int' = 16777216) -> 'None'\""
  },
  "distribution": "stove0-target-support",
  "module": "stove0_target_support",
  "name": "DepartureEffectHttpBinding",
  "unit": "export"
}
```

</details>
