# stove0_observer_protocol.ObservationRequest.canonical_subjects

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-observer-protocol:stove0-observer-protocol-observationreque-660a0ddfee:07e86ae2a6 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-observer-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-6015157123"></a>
- <a id="s-92bcb90e8b"></a>`distribution`: `stove0-observer-protocol`
- <a id="s-d24b58ee78"></a>`module`: `stove0_observer_protocol`
- <a id="s-765381d17c"></a>`name`: `canonical_subjects`
- <a id="s-9021bdcf57"></a>`owner`: `stove0_observer_protocol.ObservationRequest`
- <a id="s-2505c7ac88"></a>`unit`: `member`

### Declared structure

- <a id="s-2d5e302b9c"></a>`kind`: `"classmethod"`
- <a id="s-59dd56fd8b"></a>`signature`: `"\"(cls, value: 'tuple[ArtifactSubject, ...]') -> 'tuple[ArtifactSubject, ...]'\""`

## Maintained corroboration

### Related interface records

- [ObservationRequest](stove0-observer-protocol-observationrequest.md)

## Governing policies

- <a id="pa-14fd7aaad1"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-observer-protocol:stove0_observer_protocol](../../../evidence/sources.md#src-62450e0156) — `reference/stove0/packages/observer-protocol/src/stove0_observer_protocol/__init__.py`

### Machine authority

- `/external_contract/python/stove0_observer_protocol.ObservationRequest.canonical_subjects`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 666b725947718b386701545700ebcbb475093d3973785cc9894695b197a5d35d -->

```json
{
  "contract": {
    "kind": "classmethod",
    "signature": "\"(cls, value: 'tuple[ArtifactSubject, ...]') -> 'tuple[ArtifactSubject, ...]'\""
  },
  "distribution": "stove0-observer-protocol",
  "module": "stove0_observer_protocol",
  "name": "canonical_subjects",
  "owner": "stove0_observer_protocol.ObservationRequest",
  "unit": "member"
}
```
