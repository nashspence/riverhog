# stove0_observer_support.ContentObservationRuntime.__exit__

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-observer-support:stove0-observer-support-contentobservatio-7d2596b9cb:22e031588d -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-observer-support](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-b677af4505"></a>
- <a id="s-b8e2b61e2c"></a>`distribution`: `stove0-observer-support`
- <a id="s-a57d3ad585"></a>`module`: `stove0_observer_support`
- <a id="s-4dc4bac5bb"></a>`name`: `__exit__`
- <a id="s-b7652b31d0"></a>`owner`: `stove0_observer_support.ContentObservationRuntime`
- <a id="s-c07fe3e77e"></a>`unit`: `member`

### Declared structure

- <a id="s-77e92a3a2b"></a>`kind`: `"method"`
- <a id="s-fe734b0c5e"></a>`signature`: `"\"(self, _exc_type: 'object', _exc: 'object', _tb: 'object') -> 'None'\""`

## Maintained corroboration

### Related interface records

- [ContentObservationRuntime](stove0-observer-support-contentobservationruntime.md)

## Governing policies

- <a id="pa-33d2c7d88d"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-observer-support:stove0_observer_support](../../../evidence/sources/authorities.md#src-13bf3acd32) — [some-implementations/stove0/packages/observer-support/src/stove0\_observer\_support/\_\_init\_\_.py](../../../../../../some-implementations/stove0/packages/observer-support/src/stove0_observer_support/__init__.py)

### Machine authority

- `/external_contract/python/stove0_observer_support.ContentObservationRuntime.__exit__`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 3131ef7772393b944577fc57b068bb4fef375d0471afa02fac70aa7e088f0bb6 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, _exc_type: 'object', _exc: 'object', _tb: 'object') -> 'None'\""
  },
  "distribution": "stove0-observer-support",
  "module": "stove0_observer_support",
  "name": "__exit__",
  "owner": "stove0_observer_support.ContentObservationRuntime",
  "unit": "member"
}
```

</details>
