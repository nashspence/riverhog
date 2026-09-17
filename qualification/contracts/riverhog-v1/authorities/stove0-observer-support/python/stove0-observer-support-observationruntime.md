# stove0_observer_support.ObservationRuntime

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-observer-support:stove0-observer-support-observationruntime:1d8032c007 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-observer-support](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-25e692f500"></a>
- <a id="s-d16b38ba57"></a>`distribution`: `stove0-observer-support`
- <a id="s-41ba94a569"></a>`module`: `stove0_observer_support`
- <a id="s-805482cfea"></a>`name`: `ObservationRuntime`
- <a id="s-d93443815d"></a>`unit`: `export`

### Declared structure

- <a id="s-9fdd048e57"></a>`kind`: `"class"`
- <a id="s-557a2c6c21"></a>`signature`: `"\"(api: 'Any', *, request: 'ObservationRequest', claim_id: 'str', fence: 'int', cancellation_check: 'CancellationCheck \| None' = None, heartbeat: 'Heartbeat \| None' = None, workspace_assurance: 'str' = 'ephemeral', owned_api: 'bool' = False) -> 'None'\""`

## Maintained corroboration

### Related interface records

- [read_bytes](stove0-observer-support-observationruntime-read-bytes.md)
- [open_workspace](stove0-observer-support-observationruntime-open-workspace.md)
- [from_invocation](stove0-observer-support-observationruntime-from-invocation.md)
- [materialize](stove0-observer-support-observationruntime-materialize.md)
- [refresh_capability](stove0-observer-support-observationruntime-refresh-capability.md)
- [close](stove0-observer-support-observationruntime-close.md)
- [__enter__](stove0-observer-support-observationruntime-enter.md)
- [__exit__](stove0-observer-support-observationruntime-exit.md)
- [heartbeat](stove0-observer-support-observationruntime-heartbeat.md)
- [prepare](stove0-observer-support-observationruntime-prepare.md)
- [stream](stove0-observer-support-observationruntime-stream.md)
- [subjects](stove0-observer-support-observationruntime-subjects.md)

## Governing policies

- <a id="pa-8ee0a19600"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-observer-support:stove0_observer_support](../../../evidence/sources.md#src-13bf3acd32) — [reference/stove0/packages/observer-support/src/stove0\_observer\_support/\_\_init\_\_.py](../../../../../../reference/stove0/packages/observer-support/src/stove0_observer_support/__init__.py)

### Machine authority

- `/external_contract/python/stove0_observer_support.ObservationRuntime`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 4d0dac2584e17aafa6226028a2dffc01acde6208f8a6aca9b9bc9b8a0ac221a5 -->

```json
{
  "contract": {
    "kind": "class",
    "signature": "\"(api: 'Any', *, request: 'ObservationRequest', claim_id: 'str', fence: 'int', cancellation_check: 'CancellationCheck | None' = None, heartbeat: 'Heartbeat | None' = None, workspace_assurance: 'str' = 'ephemeral', owned_api: 'bool' = False) -> 'None'\""
  },
  "distribution": "stove0-observer-support",
  "module": "stove0_observer_support",
  "name": "ObservationRuntime",
  "unit": "export"
}
```

</details>
