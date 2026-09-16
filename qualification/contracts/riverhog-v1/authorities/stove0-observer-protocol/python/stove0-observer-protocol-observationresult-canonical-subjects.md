# stove0_observer_protocol.ObservationResult.canonical_subjects

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-observer-protocol:stove0-observer-protocol-observationresul-10b1708118:bddc6eed71 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-observer-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-1355fcdd82"></a>
- <a id="s-306d7ddcc4"></a>`distribution`: `stove0-observer-protocol`
- <a id="s-b6e45dbe3e"></a>`module`: `stove0_observer_protocol`
- <a id="s-ced357639b"></a>`name`: `canonical_subjects`
- <a id="s-a4cbeee948"></a>`owner`: `stove0_observer_protocol.ObservationResult`
- <a id="s-dd5448ddb7"></a>`unit`: `member`

### Declared structure

- <a id="s-9699f8f366"></a>`kind`: `"classmethod"`
- <a id="s-630133bea3"></a>`signature`: `"\"(cls, value: 'tuple[ArtifactSubject, ...]') -> 'tuple[ArtifactSubject, ...]'\""`

## Maintained corroboration

### Related interface records

- [ObservationResult](stove0-observer-protocol-observationresult.md)

## Governing policies

- <a id="pa-4617ed42cb"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-observer-protocol:stove0_observer_protocol](../../../evidence/sources.md#src-62450e0156) — `reference/stove0/packages/observer-protocol/src/stove0_observer_protocol/__init__.py`

### Machine authority

- `/external_contract/python/stove0_observer_protocol.ObservationResult.canonical_subjects`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: c1974693bcfbf93da1fe16f1975c47a919bbf7c3fb722d32648eba070c1e1b96 -->

```json
{
  "contract": {
    "kind": "classmethod",
    "signature": "\"(cls, value: 'tuple[ArtifactSubject, ...]') -> 'tuple[ArtifactSubject, ...]'\""
  },
  "distribution": "stove0-observer-protocol",
  "module": "stove0_observer_protocol",
  "name": "canonical_subjects",
  "owner": "stove0_observer_protocol.ObservationResult",
  "unit": "member"
}
```
