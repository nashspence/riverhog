# stove0_observer_support.ObservationRuntime.refresh_capability

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-observer-support:stove0-observer-support-observationruntim-f3650706d6:5a6973cb1a -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-observer-support](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-43985b310a"></a>
| Field | Shape |
|---|---|
| <a id="s-2296a3952f"></a>`contract` | additional keys=`kind`, `signature` |
| <a id="s-2b1a2d075a"></a>`distribution` | "stove0-observer-support" |
| <a id="s-957f986567"></a>`module` | "stove0_observer_support" |
| <a id="s-6b9b1ba03f"></a>`name` | "refresh_capability" |
| <a id="s-8776a63c82"></a>`owner` | "stove0_observer_support.ObservationRuntime" |
| <a id="s-ed70da85b8"></a>`unit` | "member" |

## Maintained corroboration

### Related interface records

- [stove0_observer_support.ObservationRuntime](stove0-observer-support-observationruntime.md)

## Governing policies

- <a id="pa-e732ff5221"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-observer-support:stove0_observer_support](../../../evidence/sources.md#src-13bf3acd32) — `reference/stove0/packages/observer-support/src/stove0_observer_support/__init__.py`

### Machine authority

- `/external_contract/python/stove0_observer_support.ObservationRuntime.refresh_capability`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: c63d685443369ff8d62d75c8dbb383175185388fab48a06e2e89ffd44dee6f1d -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, capability_token: 'str') -> 'None'\""
  },
  "distribution": "stove0-observer-support",
  "module": "stove0_observer_support",
  "name": "refresh_capability",
  "owner": "stove0_observer_support.ObservationRuntime",
  "unit": "member"
}
```
