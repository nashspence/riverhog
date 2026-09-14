# stove0_observer_support.ObservationResultBuilder

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-observer-support:stove0-observer-support-observationresultbuilder:ee8b7d5a28 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-observer-support](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-a8aecb3f25"></a>
- <a id="s-91a2097c80"></a>`distribution`: `stove0-observer-support`
- <a id="s-e7504d6563"></a>`module`: `stove0_observer_support`
- <a id="s-aa6fb51e1c"></a>`name`: `ObservationResultBuilder`
- <a id="s-7d1b256830"></a>`unit`: `export`

### Declared structure

- <a id="s-dd6408e592"></a>`kind`: `"class"`
- <a id="s-0173d1e04c"></a>`signature`: `"\"(descriptor: 'ObserverDescriptor', request: 'ObservationRequest') -> 'None'\""`

## Maintained corroboration

### Related interface records

- [failed](stove0-observer-support-observationresultbuilder-failed.md)
- [inapplicable](stove0-observer-support-observationresultbuilder-inapplicable.md)
- [canceled](stove0-observer-support-observationresultbuilder-canceled.md)
- [observed](stove0-observer-support-observationresultbuilder-observed.md)

## Governing policies

- <a id="pa-1e454475de"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-observer-support:stove0_observer_support](../../../evidence/sources.md#src-13bf3acd32) — `reference/stove0/packages/observer-support/src/stove0_observer_support/__init__.py`

### Machine authority

- `/external_contract/python/stove0_observer_support.ObservationResultBuilder`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 9031a0e64bb59bc60f044267c74b84419a0581c7d7e66a86350325173cd09fa1 -->

```json
{
  "contract": {
    "kind": "class",
    "signature": "\"(descriptor: 'ObserverDescriptor', request: 'ObservationRequest') -> 'None'\""
  },
  "distribution": "stove0-observer-support",
  "module": "stove0_observer_support",
  "name": "ObservationResultBuilder",
  "unit": "export"
}
```
