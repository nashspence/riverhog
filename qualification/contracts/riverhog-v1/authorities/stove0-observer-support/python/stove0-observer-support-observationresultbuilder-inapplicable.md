# stove0_observer_support.ObservationResultBuilder.inapplicable

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-observer-support:stove0-observer-support-observationresult-4643ba2951:71f37cd115 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-observer-support](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-bf12074292"></a>
- <a id="s-1968303f65"></a>`distribution`: `stove0-observer-support`
- <a id="s-5ae428b1bb"></a>`module`: `stove0_observer_support`
- <a id="s-8df9ddc4cf"></a>`name`: `inapplicable`
- <a id="s-91afa23e21"></a>`owner`: `stove0_observer_support.ObservationResultBuilder`
- <a id="s-0c459290ad"></a>`unit`: `member`

### Declared structure

- <a id="s-ba70ab9719"></a>`kind`: `"method"`
- <a id="s-7cb817d9ee"></a>`signature`: `"\"(self, *, code: 'str', message: 'str', execution_evidence: 'Mapping[str, JsonValue] \| None' = None) -> 'ObservationResult'\""`

## Maintained corroboration

### Related interface records

- [ObservationResultBuilder](stove0-observer-support-observationresultbuilder.md)

## Governing policies

- <a id="pa-0880decc41"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-observer-support:stove0_observer_support](../../../evidence/sources.md#src-13bf3acd32) — `reference/stove0/packages/observer-support/src/stove0_observer_support/__init__.py`

### Machine authority

- `/external_contract/python/stove0_observer_support.ObservationResultBuilder.inapplicable`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 148e6b46a2f30f454bf3fa2c8c2842d222162f7b8943ce483c17e4f5235d7f9e -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, *, code: 'str', message: 'str', execution_evidence: 'Mapping[str, JsonValue] | None' = None) -> 'ObservationResult'\""
  },
  "distribution": "stove0-observer-support",
  "module": "stove0_observer_support",
  "name": "inapplicable",
  "owner": "stove0_observer_support.ObservationResultBuilder",
  "unit": "member"
}
```

</details>
