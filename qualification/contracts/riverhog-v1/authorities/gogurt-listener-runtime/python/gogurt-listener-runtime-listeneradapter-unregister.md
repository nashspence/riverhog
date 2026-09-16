# gogurt_listener_runtime.ListenerAdapter.unregister

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:gogurt-listener-runtime:gogurt-listener-runtime-listeneradapter-unregister:8145d187da -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [gogurt-listener-runtime](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-59da971ff6"></a>
- <a id="s-916773c221"></a>`distribution`: `gogurt-listener-runtime`
- <a id="s-9a32d947b9"></a>`module`: `gogurt_listener_runtime`
- <a id="s-e99cc8cd3b"></a>`name`: `unregister`
- <a id="s-0c5f5eec03"></a>`owner`: `gogurt_listener_runtime.ListenerAdapter`
- <a id="s-e7af329488"></a>`unit`: `member`

### Declared structure

- <a id="s-5846b638d3"></a>`kind`: `"method"`
- <a id="s-afc8e0e055"></a>`signature`: `"\"(self, paths: 'ListenerRuntimePaths') -> 'None'\""`

## Maintained corroboration

### Related interface records

- [ListenerAdapter](gogurt-listener-runtime-listeneradapter.md)

## Governing policies

- <a id="pa-e58bacf052"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:gogurt-listener-runtime:gogurt_listener_runtime](../../../evidence/sources.md#src-259980dd25) — `reference/gogurt/packages/listener-runtime/src/gogurt_listener_runtime/__init__.py`

### Machine authority

- `/external_contract/python/gogurt_listener_runtime.ListenerAdapter.unregister`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: f6d72b8a1c92014edf0312086a359b53e909bda6825d31e211820da1430c1e66 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, paths: 'ListenerRuntimePaths') -> 'None'\""
  },
  "distribution": "gogurt-listener-runtime",
  "module": "gogurt_listener_runtime",
  "name": "unregister",
  "owner": "gogurt_listener_runtime.ListenerAdapter",
  "unit": "member"
}
```

</details>
