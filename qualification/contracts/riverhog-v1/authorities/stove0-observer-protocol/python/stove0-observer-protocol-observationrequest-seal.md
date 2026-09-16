# stove0_observer_protocol.ObservationRequest.seal

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-observer-protocol:stove0-observer-protocol-observationrequest-seal:41c10e236a -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-observer-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-90538543bf"></a>
- <a id="s-7917f93697"></a>`distribution`: `stove0-observer-protocol`
- <a id="s-8e42325334"></a>`module`: `stove0_observer_protocol`
- <a id="s-bcd740e3ee"></a>`name`: `seal`
- <a id="s-ca97183e4e"></a>`owner`: `stove0_observer_protocol.ObservationRequest`
- <a id="s-3d293b7c60"></a>`unit`: `member`

### Declared structure

- <a id="s-8bd921e084"></a>`kind`: `"classmethod"`
- <a id="s-c5e1873c92"></a>`signature`: `"\"(cls, payload: 'ObservationRequestPayload') -> 'ObservationRequest'\""`

## Maintained corroboration

### Related interface records

- [ObservationRequest](stove0-observer-protocol-observationrequest.md)

## Governing policies

- <a id="pa-2b171db5d3"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-observer-protocol:stove0_observer_protocol](../../../evidence/sources.md#src-62450e0156) — `reference/stove0/packages/observer-protocol/src/stove0_observer_protocol/__init__.py`

### Machine authority

- `/external_contract/python/stove0_observer_protocol.ObservationRequest.seal`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 5672fd487525c2e5ae889859d7982617e4ab0084fe358e422571ff8433fff4f5 -->

```json
{
  "contract": {
    "kind": "classmethod",
    "signature": "\"(cls, payload: 'ObservationRequestPayload') -> 'ObservationRequest'\""
  },
  "distribution": "stove0-observer-protocol",
  "module": "stove0_observer_protocol",
  "name": "seal",
  "owner": "stove0_observer_protocol.ObservationRequest",
  "unit": "member"
}
```

</details>
