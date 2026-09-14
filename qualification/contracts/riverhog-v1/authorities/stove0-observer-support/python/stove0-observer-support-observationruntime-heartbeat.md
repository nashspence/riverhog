# stove0_observer_support.ObservationRuntime.heartbeat

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-observer-support:stove0-observer-support-observationruntime-heartbeat:f345359b52 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-observer-support](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-a2deba32f2"></a>
- <a id="s-d9fa31c427"></a>`distribution`: `stove0-observer-support`
- <a id="s-1fe1501d76"></a>`module`: `stove0_observer_support`
- <a id="s-0ea2a8cdc1"></a>`name`: `heartbeat`
- <a id="s-6d6f3b2119"></a>`owner`: `stove0_observer_support.ObservationRuntime`
- <a id="s-645d7e5f04"></a>`unit`: `member`

### Declared structure

- <a id="s-6ca9a17537"></a>`kind`: `"method"`
- <a id="s-0b2dbd833e"></a>`signature`: `"\"(self) -> 'None'\""`

## Maintained corroboration

### Related interface records

- [ObservationRuntime](stove0-observer-support-observationruntime.md)

## Governing policies

- <a id="pa-155651c219"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-observer-support:stove0_observer_support](../../../evidence/sources.md#src-13bf3acd32) — `reference/stove0/packages/observer-support/src/stove0_observer_support/__init__.py`

### Machine authority

- `/external_contract/python/stove0_observer_support.ObservationRuntime.heartbeat`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 557056f3826dce7c1b90258509e2078e319e6a8c34c43a74a5e06acbf361db0e -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self) -> 'None'\""
  },
  "distribution": "stove0-observer-support",
  "module": "stove0_observer_support",
  "name": "heartbeat",
  "owner": "stove0_observer_support.ObservationRuntime",
  "unit": "member"
}
```
