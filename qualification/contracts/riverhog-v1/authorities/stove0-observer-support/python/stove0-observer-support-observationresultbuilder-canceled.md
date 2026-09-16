# stove0_observer_support.ObservationResultBuilder.canceled

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-observer-support:stove0-observer-support-observationresult-73c8b697aa:7fb54678db -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-observer-support](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-a4c232bc30"></a>
- <a id="s-d2c486c3cb"></a>`distribution`: `stove0-observer-support`
- <a id="s-8b2cfa0616"></a>`module`: `stove0_observer_support`
- <a id="s-29fc1d1808"></a>`name`: `canceled`
- <a id="s-c67362d214"></a>`owner`: `stove0_observer_support.ObservationResultBuilder`
- <a id="s-8bd1a55c11"></a>`unit`: `member`

### Declared structure

- <a id="s-c5dace2c63"></a>`kind`: `"method"`
- <a id="s-b8abe26b15"></a>`signature`: `"\"(self, *, execution_evidence: 'Mapping[str, JsonValue] \| None' = None) -> 'ObservationResult'\""`

## Maintained corroboration

### Related interface records

- [ObservationResultBuilder](stove0-observer-support-observationresultbuilder.md)

## Governing policies

- <a id="pa-71b1da744e"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-observer-support:stove0_observer_support](../../../evidence/sources.md#src-13bf3acd32) — `reference/stove0/packages/observer-support/src/stove0_observer_support/__init__.py`

### Machine authority

- `/external_contract/python/stove0_observer_support.ObservationResultBuilder.canceled`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: a032dd87d907ee666d7e8406f471c781b69e4917a15b3d8382264a5b46e85bca -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, *, execution_evidence: 'Mapping[str, JsonValue] | None' = None) -> 'ObservationResult'\""
  },
  "distribution": "stove0-observer-support",
  "module": "stove0_observer_support",
  "name": "canceled",
  "owner": "stove0_observer_support.ObservationResultBuilder",
  "unit": "member"
}
```

</details>
