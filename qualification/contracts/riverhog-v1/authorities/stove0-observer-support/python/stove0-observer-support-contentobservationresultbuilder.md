# stove0_observer_support.ContentObservationResultBuilder

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-observer-support:stove0-observer-support-contentobservatio-04293846d2:7cb0e464a5 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-observer-support](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-6f2b1a27fe"></a>
- <a id="s-8ce74dd6ed"></a>`distribution`: `stove0-observer-support`
- <a id="s-eb17ea40d6"></a>`module`: `stove0_observer_support`
- <a id="s-d67f400fbb"></a>`name`: `ContentObservationResultBuilder`
- <a id="s-75fec10708"></a>`unit`: `export`

### Declared structure

- <a id="s-a0f568a587"></a>`kind`: `"class"`
- <a id="s-cd2b72a33c"></a>`signature`: `"\"(descriptor: 'ObserverDescriptor', request: 'ContentObservationRequest') -> 'None'\""`

## Maintained corroboration

### Related interface records

- [observed](stove0-observer-support-contentobservationresultbuilder-observed.md)
- [inapplicable](stove0-observer-support-contentobservationresultbuilder-inapplicable.md)
- [failed](stove0-observer-support-contentobservationresultbuilder-failed.md)
- [canceled](stove0-observer-support-contentobservationresultbuilder-canceled.md)

## Governing policies

- <a id="pa-0155b5e271"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-observer-support:stove0_observer_support](../../../evidence/sources/authorities.md#src-13bf3acd32) — [some-implementations/stove0/packages/observer-support/src/stove0\_observer\_support/\_\_init\_\_.py](../../../../../../some-implementations/stove0/packages/observer-support/src/stove0_observer_support/__init__.py)

### Machine authority

- `/external_contract/python/stove0_observer_support.ContentObservationResultBuilder`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 44eecaf8f7ef513c362a406dc03c91492e3c07c85722c9449e634d7923ee2088 -->

```json
{
  "contract": {
    "kind": "class",
    "signature": "\"(descriptor: 'ObserverDescriptor', request: 'ContentObservationRequest') -> 'None'\""
  },
  "distribution": "stove0-observer-support",
  "module": "stove0_observer_support",
  "name": "ContentObservationResultBuilder",
  "unit": "export"
}
```

</details>
