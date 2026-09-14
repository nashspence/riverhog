# gogurt_listener_runtime.LISTENER_OPERATIONS

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:gogurt-listener-runtime:gogurt-listener-runtime-listener-operations:89db2fc507 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [gogurt-listener-runtime](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-3654075802"></a>
- <a id="s-01edcd986c"></a>`distribution`: `gogurt-listener-runtime`
- <a id="s-92e8360a61"></a>`module`: `gogurt_listener_runtime`
- <a id="s-98169919a1"></a>`name`: `LISTENER_OPERATIONS`
- <a id="s-4010108c2b"></a>`unit`: `export`

### Declared structure

- <a id="s-4db5ce591d"></a>`kind`: `"constant"`
- <a id="s-c247d8ac5d"></a>`value`: `["install","status","start","stop","restart","uninstall"]`

## Governing policies

- <a id="pa-93dbd52a7e"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:gogurt-listener-runtime:gogurt_listener_runtime](../../../evidence/sources.md#src-259980dd25) — `reference/gogurt/packages/listener-runtime/src/gogurt_listener_runtime/__init__.py`

### Machine authority

- `/external_contract/python/gogurt_listener_runtime.LISTENER_OPERATIONS`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: a99a5f70f42e56bf93363e5d21d67d18063cb17b7cd07694b1a13a099a69073c -->

```json
{
  "contract": {
    "kind": "constant",
    "value": [
      "install",
      "status",
      "start",
      "stop",
      "restart",
      "uninstall"
    ]
  },
  "distribution": "gogurt-listener-runtime",
  "module": "gogurt_listener_runtime",
  "name": "LISTENER_OPERATIONS",
  "unit": "export"
}
```
