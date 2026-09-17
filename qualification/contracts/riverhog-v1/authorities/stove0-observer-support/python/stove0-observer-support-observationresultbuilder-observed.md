# stove0_observer_support.ObservationResultBuilder.observed

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-observer-support:stove0-observer-support-observationresult-8e34c02ffb:478e580307 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-observer-support](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-d1b7f4aacf"></a>
- <a id="s-6cc8083b07"></a>`distribution`: `stove0-observer-support`
- <a id="s-400c67cc35"></a>`module`: `stove0_observer_support`
- <a id="s-54663e0ae5"></a>`name`: `observed`
- <a id="s-e54434e276"></a>`owner`: `stove0_observer_support.ObservationResultBuilder`
- <a id="s-37fd7ea1c1"></a>`unit`: `member`

### Declared structure

- <a id="s-65b324cf4a"></a>`kind`: `"method"`
- <a id="s-b0d861813d"></a>`signature`: `"\"(self, facts: 'Mapping[str, JsonValue]', *, execution_evidence: 'Mapping[str, JsonValue] \| None' = None) -> 'ObservationResult'\""`

## Maintained corroboration

### Related interface records

- [ObservationResultBuilder](stove0-observer-support-observationresultbuilder.md)

## Governing policies

- <a id="pa-4ba59ba572"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-observer-support:stove0_observer_support](../../../evidence/sources.md#src-13bf3acd32) — [reference/stove0/packages/observer-support/src/stove0\_observer\_support/\_\_init\_\_.py](../../../../../../reference/stove0/packages/observer-support/src/stove0_observer_support/__init__.py)

### Machine authority

- `/external_contract/python/stove0_observer_support.ObservationResultBuilder.observed`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: faa47de0f35d91bb8921958fca0379b853a6dd2ca5e742c19b2014c1cb9bcce8 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, facts: 'Mapping[str, JsonValue]', *, execution_evidence: 'Mapping[str, JsonValue] | None' = None) -> 'ObservationResult'\""
  },
  "distribution": "stove0-observer-support",
  "module": "stove0_observer_support",
  "name": "observed",
  "owner": "stove0_observer_support.ObservationResultBuilder",
  "unit": "member"
}
```

</details>
