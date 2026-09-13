# stove0_observer_support.ObservationRuntime.prepare

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-observer-support:stove0-observer-support-observationruntime-prepare:af83faab23 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-observer-support](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-c0d3978b17"></a>
| Field | Shape |
|---|---|
| <a id="s-26d562f04e"></a>`contract` | additional keys=`kind`, `signature` |
| <a id="s-3d580f4d54"></a>`distribution` | "stove0-observer-support" |
| <a id="s-de816a8f53"></a>`module` | "stove0_observer_support" |
| <a id="s-9f6cec31df"></a>`name` | "prepare" |
| <a id="s-b87eb1e8c9"></a>`owner` | "stove0_observer_support.ObservationRuntime" |
| <a id="s-2552794449"></a>`unit` | "member" |

## Maintained corroboration

### Related interface records

- [stove0_observer_support.ObservationRuntime](stove0-observer-support-observationruntime.md)

## Governing policies

- <a id="pa-21dd22eade"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-observer-support:stove0_observer_support](../../../evidence/sources.md#src-13bf3acd32) — `reference/stove0/packages/observer-support/src/stove0_observer_support/__init__.py`

### Machine authority

- `/external_contract/python/stove0_observer_support.ObservationRuntime.prepare`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: fdbfef2f10e3331727d073de66bebdb81c620213fc7d78cfe7cfe082f64fc48b -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, subjects: 'Sequence[ArtifactSubject] | None' = None, **kwargs: 'Any') -> 'ClaimedRetrieval'\""
  },
  "distribution": "stove0-observer-support",
  "module": "stove0_observer_support",
  "name": "prepare",
  "owner": "stove0_observer_support.ObservationRuntime",
  "unit": "member"
}
```
