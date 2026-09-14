# gogurt_listener_runtime.ListenerConfig.write

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:gogurt-listener-runtime:gogurt-listener-runtime-listenerconfig-write:9ae1d80599 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [gogurt-listener-runtime](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-df62584159"></a>
- <a id="s-37656c4354"></a>`distribution`: `gogurt-listener-runtime`
- <a id="s-a67e36841a"></a>`module`: `gogurt_listener_runtime`
- <a id="s-f7fb777e04"></a>`name`: `write`
- <a id="s-6cc232dad6"></a>`owner`: `gogurt_listener_runtime.ListenerConfig`
- <a id="s-d2f7648dda"></a>`unit`: `member`

### Declared structure

- <a id="s-9a2b92f081"></a>`kind`: `"method"`
- <a id="s-b87fc4fbb3"></a>`signature`: `"\"(self, path: 'Path') -> 'None'\""`

## Maintained corroboration

### Related interface records

- [gogurt_listener_runtime.ListenerConfig](gogurt-listener-runtime-listenerconfig.md)

## Governing policies

- <a id="pa-bc8d79dc47"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:gogurt-listener-runtime:gogurt_listener_runtime](../../../evidence/sources.md#src-259980dd25) — `reference/gogurt/packages/listener-runtime/src/gogurt_listener_runtime/__init__.py`

### Machine authority

- `/external_contract/python/gogurt_listener_runtime.ListenerConfig.write`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: c9d63e94f61c7f935eb3a541fd158b030b674af374bbbdd8ab018710b2708aa8 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, path: 'Path') -> 'None'\""
  },
  "distribution": "gogurt-listener-runtime",
  "module": "gogurt_listener_runtime",
  "name": "write",
  "owner": "gogurt_listener_runtime.ListenerConfig",
  "unit": "member"
}
```
