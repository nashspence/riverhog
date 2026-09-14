# gogurt_listener_runtime.ListenerAdapter.start

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:gogurt-listener-runtime:gogurt-listener-runtime-listeneradapter-start:ca093f6b70 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [gogurt-listener-runtime](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-2fbda393b8"></a>
- <a id="s-02b9bba2d4"></a>`distribution`: `gogurt-listener-runtime`
- <a id="s-62bb9fd977"></a>`module`: `gogurt_listener_runtime`
- <a id="s-d0b51f2613"></a>`name`: `start`
- <a id="s-1df2f12890"></a>`owner`: `gogurt_listener_runtime.ListenerAdapter`
- <a id="s-cf11c7a687"></a>`unit`: `member`

### Declared structure

- <a id="s-c36a2148cb"></a>`kind`: `"method"`
- <a id="s-45863062da"></a>`signature`: `"\"(self, paths: 'ListenerRuntimePaths') -> 'None'\""`

## Maintained corroboration

### Related interface records

- [gogurt_listener_runtime.ListenerAdapter](gogurt-listener-runtime-listeneradapter.md)

## Governing policies

- <a id="pa-f222a54b8a"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:gogurt-listener-runtime:gogurt_listener_runtime](../../../evidence/sources.md#src-259980dd25) — `reference/gogurt/packages/listener-runtime/src/gogurt_listener_runtime/__init__.py`

### Machine authority

- `/external_contract/python/gogurt_listener_runtime.ListenerAdapter.start`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 08d6305dcbac44832104f904a84fb9dbdf67dd75213e7ef99150653518afac06 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, paths: 'ListenerRuntimePaths') -> 'None'\""
  },
  "distribution": "gogurt-listener-runtime",
  "module": "gogurt_listener_runtime",
  "name": "start",
  "owner": "gogurt_listener_runtime.ListenerAdapter",
  "unit": "member"
}
```
