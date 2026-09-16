# stove0_observer_support.ObservationRuntime.from_invocation

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-observer-support:stove0-observer-support-observationruntim-b9a41dcc0d:d877701549 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-observer-support](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-5ab767ae8b"></a>
- <a id="s-b3d62e31b2"></a>`distribution`: `stove0-observer-support`
- <a id="s-3dd2402d92"></a>`module`: `stove0_observer_support`
- <a id="s-ae5562d40e"></a>`name`: `from_invocation`
- <a id="s-3eca18de95"></a>`owner`: `stove0_observer_support.ObservationRuntime`
- <a id="s-56e4435d93"></a>`unit`: `member`

### Declared structure

- <a id="s-c9b320649b"></a>`kind`: `"classmethod"`
- <a id="s-9e3aef9127"></a>`signature`: `"\"(cls, invocation: 'ObservationInvocation', *, cancellation_check: 'CancellationCheck \| None' = None, heartbeat: 'Heartbeat \| None' = None) -> 'ObservationRuntime'\""`

## Maintained corroboration

### Related interface records

- [ObservationRuntime](stove0-observer-support-observationruntime.md)

## Governing policies

- <a id="pa-e533cbdc95"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-observer-support:stove0_observer_support](../../../evidence/sources.md#src-13bf3acd32) — `reference/stove0/packages/observer-support/src/stove0_observer_support/__init__.py`

### Machine authority

- `/external_contract/python/stove0_observer_support.ObservationRuntime.from_invocation`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 62b219ba05aa2016b40700a8a7087142310e49c4d6307a596e01315e36c2f001 -->

```json
{
  "contract": {
    "kind": "classmethod",
    "signature": "\"(cls, invocation: 'ObservationInvocation', *, cancellation_check: 'CancellationCheck | None' = None, heartbeat: 'Heartbeat | None' = None) -> 'ObservationRuntime'\""
  },
  "distribution": "stove0-observer-support",
  "module": "stove0_observer_support",
  "name": "from_invocation",
  "owner": "stove0_observer_support.ObservationRuntime",
  "unit": "member"
}
```

</details>
