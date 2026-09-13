# stove0_observer_protocol.ObservationRequestPayload.canonical_subjects

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-observer-protocol:stove0-observer-protocol-observationreque-012901d578:05b014c575 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-observer-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-0fffc3de75"></a>
| Field | Shape |
|---|---|
| <a id="s-95a162768a"></a>`contract` | additional keys=`kind`, `signature` |
| <a id="s-026d6af38b"></a>`distribution` | "stove0-observer-protocol" |
| <a id="s-2ae561b038"></a>`module` | "stove0_observer_protocol" |
| <a id="s-47132d199f"></a>`name` | "canonical_subjects" |
| <a id="s-337dada5de"></a>`owner` | "stove0_observer_protocol.ObservationRequestPayload" |
| <a id="s-7ecd2483f7"></a>`unit` | "member" |

## Maintained corroboration

### Related interface records

- [stove0_observer_protocol.ObservationRequestPayload](stove0-observer-protocol-observationrequestpayload.md)

## Governing policies

- <a id="pa-ec8c600654"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-observer-protocol:stove0_observer_protocol](../../../evidence/sources.md#src-62450e0156) — `reference/stove0/packages/observer-protocol/src/stove0_observer_protocol/__init__.py`

### Machine authority

- `/external_contract/python/stove0_observer_protocol.ObservationRequestPayload.canonical_subjects`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: d572697043d9bc2dc72f0c39c71578beae3a5d66a2c948824b7a6a6511f37fb9 -->

```json
{
  "contract": {
    "kind": "classmethod",
    "signature": "\"(cls, value: 'tuple[ArtifactSubject, ...]') -> 'tuple[ArtifactSubject, ...]'\""
  },
  "distribution": "stove0-observer-protocol",
  "module": "stove0_observer_protocol",
  "name": "canonical_subjects",
  "owner": "stove0_observer_protocol.ObservationRequestPayload",
  "unit": "member"
}
```
