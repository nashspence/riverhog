# gogurt_listener_runtime.ListenerAdapter

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:gogurt-listener-runtime:gogurt-listener-runtime-listeneradapter:be66da5fed -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [gogurt-listener-runtime](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-a5251728ae"></a>
| Field | Shape |
|---|---|
| <a id="s-bb28009a25"></a>`contract` | additional keys=`kind`, `signature` |
| <a id="s-34de6ca169"></a>`distribution` | "gogurt-listener-runtime" |
| <a id="s-2130ff8f93"></a>`module` | "gogurt_listener_runtime" |
| <a id="s-e2c3307f70"></a>`name` | "ListenerAdapter" |
| <a id="s-94f702b7f1"></a>`unit` | "export" |

## Maintained corroboration

### Related interface records

- [gogurt_listener_runtime.ListenerAdapter.process_is_running](gogurt-listener-runtime-listeneradapter-process-is-running.md)
- [gogurt_listener_runtime.ListenerAdapter.register](gogurt-listener-runtime-listeneradapter-register.md)
- [gogurt_listener_runtime.ListenerAdapter.start](gogurt-listener-runtime-listeneradapter-start.md)
- [gogurt_listener_runtime.ListenerAdapter.status](gogurt-listener-runtime-listeneradapter-status.md)
- [gogurt_listener_runtime.ListenerAdapter.stop](gogurt-listener-runtime-listeneradapter-stop.md)
- [gogurt_listener_runtime.ListenerAdapter.unregister](gogurt-listener-runtime-listeneradapter-unregister.md)

## Governing policies

- <a id="pa-b42c5b9664"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:gogurt-listener-runtime:gogurt_listener_runtime](../../../evidence/sources.md#src-259980dd25) — `reference/gogurt/packages/listener-runtime/src/gogurt_listener_runtime/__init__.py`

### Machine authority

- `/external_contract/python/gogurt_listener_runtime.ListenerAdapter`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: a525361b56024d47ef9342dbeb219db579fc8ea5b37a7b7c18b9608cd4c4d564 -->

```json
{
  "contract": {
    "kind": "class",
    "signature": "'(*args, **kwargs)'"
  },
  "distribution": "gogurt-listener-runtime",
  "module": "gogurt_listener_runtime",
  "name": "ListenerAdapter",
  "unit": "export"
}
```
