# gogurt_listener_runtime.ListenerRuntime.request_stop

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:gogurt-listener-runtime:gogurt-listener-runtime-listenerruntime-request-stop:f78b9f9e37 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [gogurt-listener-runtime](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-ed95535769"></a>
- <a id="s-9ae1dbbacc"></a>`distribution`: `gogurt-listener-runtime`
- <a id="s-65acafc436"></a>`module`: `gogurt_listener_runtime`
- <a id="s-910c31c466"></a>`name`: `request_stop`
- <a id="s-60f1c06d53"></a>`owner`: `gogurt_listener_runtime.ListenerRuntime`
- <a id="s-dae7074e96"></a>`unit`: `member`

### Declared structure

- <a id="s-c7874836ff"></a>`kind`: `"method"`
- <a id="s-7efe4624e2"></a>`signature`: `"\"(self) -> 'None'\""`

## Maintained corroboration

### Related interface records

- [ListenerRuntime](gogurt-listener-runtime-listenerruntime.md)

## Governing policies

- <a id="pa-455d93ed3d"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:gogurt-listener-runtime:gogurt_listener_runtime](../../../evidence/sources.md#src-259980dd25) — `reference/gogurt/packages/listener-runtime/src/gogurt_listener_runtime/__init__.py`

### Machine authority

- `/external_contract/python/gogurt_listener_runtime.ListenerRuntime.request_stop`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 00a918b0a2c86846e7a9b97484a43d7d440dfae0d1ae2e8d664158f5751752f1 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self) -> 'None'\""
  },
  "distribution": "gogurt-listener-runtime",
  "module": "gogurt_listener_runtime",
  "name": "request_stop",
  "owner": "gogurt_listener_runtime.ListenerRuntime",
  "unit": "member"
}
```
