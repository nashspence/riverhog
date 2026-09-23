# stove0_observer_support.ContentObservationRuntime.refresh_capability

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-observer-support:stove0-observer-support-contentobservatio-8f66c31ee9:ec60a2ff2d -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-observer-support](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-d244735c7b"></a>
- <a id="s-87ed87171a"></a>`distribution`: `stove0-observer-support`
- <a id="s-73f2469034"></a>`module`: `stove0_observer_support`
- <a id="s-c05223b98c"></a>`name`: `refresh_capability`
- <a id="s-898afb00b2"></a>`owner`: `stove0_observer_support.ContentObservationRuntime`
- <a id="s-1765a67309"></a>`unit`: `member`

### Declared structure

- <a id="s-4fa3ff7d6c"></a>`kind`: `"method"`
- <a id="s-4b924c6d5d"></a>`signature`: `"\"(self, capability_token: 'str') -> 'None'\""`

## Maintained corroboration

### Related interface records

- [ContentObservationRuntime](stove0-observer-support-contentobservationruntime.md)

## Governing policies

- <a id="pa-4f0e3bd899"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-observer-support:stove0_observer_support](../../../evidence/sources/authorities.md#src-13bf3acd32) — [some-implementations/stove0/packages/observer-support/src/stove0\_observer\_support/\_\_init\_\_.py](../../../../../../some-implementations/stove0/packages/observer-support/src/stove0_observer_support/__init__.py)

### Machine authority

- `/external_contract/python/stove0_observer_support.ContentObservationRuntime.refresh_capability`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 0777c05bb41a57b320c2ec56993fee0b98db709fa45b3342b0608e3621f94b2d -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, capability_token: 'str') -> 'None'\""
  },
  "distribution": "stove0-observer-support",
  "module": "stove0_observer_support",
  "name": "refresh_capability",
  "owner": "stove0_observer_support.ContentObservationRuntime",
  "unit": "member"
}
```

</details>
