# stove0_observer_support.OBSERVER_CONFORMANCE_RESULT

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-observer-support:stove0-observer-support-observer-conformance-result:58e7b246b3 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-observer-support](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-83575aeaab"></a>
- <a id="s-5bec6052b4"></a>`distribution`: `stove0-observer-support`
- <a id="s-1141bd7304"></a>`module`: `stove0_observer_support`
- <a id="s-92af45ee13"></a>`name`: `OBSERVER_CONFORMANCE_RESULT`
- <a id="s-6dfcabf518"></a>`unit`: `export`

### Declared structure

- <a id="s-5d59c6f221"></a>`kind`: `"constant"`
- <a id="s-e2d26d06a7"></a>`value`: `"stove0-observer-conformance-result/v1"`

## Governing policies

- <a id="pa-bfee1dcf4e"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-observer-support:stove0_observer_support](../../../evidence/sources/authorities.md#src-13bf3acd32) — [some-implementations/stove0/packages/observer-support/src/stove0\_observer\_support/\_\_init\_\_.py](../../../../../../some-implementations/stove0/packages/observer-support/src/stove0_observer_support/__init__.py)

### Machine authority

- `/external_contract/python/stove0_observer_support.OBSERVER_CONFORMANCE_RESULT`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 56a177f918a1590b9478cd93cb7f32791b681d9ced4ad8be9e90fc1bd3b335ec -->

```json
{
  "contract": {
    "kind": "constant",
    "value": "stove0-observer-conformance-result/v1"
  },
  "distribution": "stove0-observer-support",
  "module": "stove0_observer_support",
  "name": "OBSERVER_CONFORMANCE_RESULT",
  "unit": "export"
}
```

</details>
