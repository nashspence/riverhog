# stove0_observer_support.ContentObservationRuntime

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-observer-support:stove0-observer-support-contentobservationruntime:19c3e70b3b -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [stove0-observer-support](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-52b35670db"></a>
- <a id="s-062a6c8b5c"></a>`distribution`: `stove0-observer-support`
- <a id="s-7fe967fc8f"></a>`module`: `stove0_observer_support`
- <a id="s-f8ac78c4ec"></a>`name`: `ContentObservationRuntime`
- <a id="s-5da379a78c"></a>`unit`: `export`

### Declared structure

- <a id="s-20eafbb5fc"></a>`kind`: `"class"`
- <a id="s-53cadaaa93"></a>`signature`: `"\"(api: 'Any', *, request: 'ContentObservationRequest', claim_id: 'str', fence: 'int', cancellation_check: 'CancellationCheck \| None' = None, heartbeat: 'Heartbeat \| None' = None, declared_workspace_protection: 'DeclaredWorkspaceProtection', owned_api: 'bool' = False) -> 'None'\""`

## Maintained corroboration

### Related interface records

- [__enter__](stove0-observer-support-contentobservationruntime-enter.md)
- [heartbeat](stove0-observer-support-contentobservationruntime-heartbeat.md)
- [subjects](stove0-observer-support-contentobservationruntime-subjects.md)
- [prepare](stove0-observer-support-contentobservationruntime-prepare.md)
- [__exit__](stove0-observer-support-contentobservationruntime-exit.md)
- [refresh_capability](stove0-observer-support-contentobservationruntime-refresh-capability.md)
- [stream](stove0-observer-support-contentobservationruntime-stream.md)
- [close](stove0-observer-support-contentobservationruntime-close.md)
- [from_invocation](stove0-observer-support-contentobservationruntime-from-invocation.md)
- [materialize](stove0-observer-support-contentobservationruntime-materialize.md)
- [read_bytes](stove0-observer-support-contentobservationruntime-read-bytes.md)
- [open_workspace](stove0-observer-support-contentobservationruntime-open-workspace.md)

## Governing policies

- <a id="pa-c0b251d721"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:stove0-observer-support:stove0_observer_support](../../../evidence/sources/authorities.md#src-13bf3acd32) — [some-implementations/stove0/packages/observer-support/src/stove0\_observer\_support/\_\_init\_\_.py](../../../../../../some-implementations/stove0/packages/observer-support/src/stove0_observer_support/__init__.py)

### Machine authority

- `/external_contract/python/stove0_observer_support.ContentObservationRuntime`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 63e208def0903843347242ad3977b6c9df57b686eb9ad5a3953796d3132bae79 -->

```json
{
  "contract": {
    "kind": "class",
    "signature": "\"(api: 'Any', *, request: 'ContentObservationRequest', claim_id: 'str', fence: 'int', cancellation_check: 'CancellationCheck | None' = None, heartbeat: 'Heartbeat | None' = None, declared_workspace_protection: 'DeclaredWorkspaceProtection', owned_api: 'bool' = False) -> 'None'\""
  },
  "distribution": "stove0-observer-support",
  "module": "stove0_observer_support",
  "name": "ContentObservationRuntime",
  "unit": "export"
}
```

</details>
