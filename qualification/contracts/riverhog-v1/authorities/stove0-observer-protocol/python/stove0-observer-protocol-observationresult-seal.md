# stove0_observer_protocol.ObservationResult.seal

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-observer-protocol:stove0-observer-protocol-observationresult-seal:7a9878d641 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-observer-protocol](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-52dcc1773e"></a>
- <a id="s-64d3ca6a35"></a>`distribution`: `stove0-observer-protocol`
- <a id="s-793c20e525"></a>`module`: `stove0_observer_protocol`
- <a id="s-ad598328bb"></a>`name`: `seal`
- <a id="s-bf9a113155"></a>`owner`: `stove0_observer_protocol.ObservationResult`
- <a id="s-0b2e243052"></a>`unit`: `member`

### Declared structure

- <a id="s-05a50fb648"></a>`kind`: `"classmethod"`
- <a id="s-5fb1588d2d"></a>`signature`: `"\"(cls, payload: 'ObservationResultPayload') -> 'ObservationResult'\""`

## Maintained corroboration

### Related interface records

- [ObservationResult](stove0-observer-protocol-observationresult.md)

## Governing policies

- <a id="pa-d59fdf5836"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-observer-protocol:stove0_observer_protocol](../../../evidence/sources.md#src-62450e0156) — `reference/stove0/packages/observer-protocol/src/stove0_observer_protocol/__init__.py`

### Machine authority

- `/external_contract/python/stove0_observer_protocol.ObservationResult.seal`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 822145b1cd5fa59a81b811796cb25ac65d76b7c67d283307d37f1e1338cc0207 -->

```json
{
  "contract": {
    "kind": "classmethod",
    "signature": "\"(cls, payload: 'ObservationResultPayload') -> 'ObservationResult'\""
  },
  "distribution": "stove0-observer-protocol",
  "module": "stove0_observer_protocol",
  "name": "seal",
  "owner": "stove0_observer_protocol.ObservationResult",
  "unit": "member"
}
```

</details>
