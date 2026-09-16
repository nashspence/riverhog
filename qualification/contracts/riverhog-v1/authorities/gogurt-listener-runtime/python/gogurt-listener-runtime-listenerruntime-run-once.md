# gogurt_listener_runtime.ListenerRuntime.run_once

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:gogurt-listener-runtime:gogurt-listener-runtime-listenerruntime-run-once:6913722999 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [gogurt-listener-runtime](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-9c1a5517ff"></a>
- <a id="s-3f5adbb3a4"></a>`distribution`: `gogurt-listener-runtime`
- <a id="s-891f334c5a"></a>`module`: `gogurt_listener_runtime`
- <a id="s-2ee33ea901"></a>`name`: `run_once`
- <a id="s-863515078c"></a>`owner`: `gogurt_listener_runtime.ListenerRuntime`
- <a id="s-104423e8f0"></a>`unit`: `member`

### Declared structure

- <a id="s-173a7df4b5"></a>`kind`: `"method"`
- <a id="s-cdcb0f290d"></a>`signature`: `"\"(self) -> 'None'\""`

## Maintained corroboration

### Related interface records

- [ListenerRuntime](gogurt-listener-runtime-listenerruntime.md)

## Governing policies

- <a id="pa-e609a05252"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:gogurt-listener-runtime:gogurt_listener_runtime](../../../evidence/sources.md#src-259980dd25) — `reference/gogurt/packages/listener-runtime/src/gogurt_listener_runtime/__init__.py`

### Machine authority

- `/external_contract/python/gogurt_listener_runtime.ListenerRuntime.run_once`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: cdfa22e02c8289dfc6380689f8b142bbf9f85db5ee4b0ce4c221aae4003b8779 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self) -> 'None'\""
  },
  "distribution": "gogurt-listener-runtime",
  "module": "gogurt_listener_runtime",
  "name": "run_once",
  "owner": "gogurt_listener_runtime.ListenerRuntime",
  "unit": "member"
}
```

</details>
