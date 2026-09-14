# gogurt_listener_runtime.LISTENER_CONFIG_SCHEMA

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:gogurt-listener-runtime:gogurt-listener-runtime-listener-config-schema:f4daa0f67c -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [gogurt-listener-runtime](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-c77dccf9f2"></a>
- <a id="s-1851513c69"></a>`distribution`: `gogurt-listener-runtime`
- <a id="s-6ad02a3b67"></a>`module`: `gogurt_listener_runtime`
- <a id="s-59ebd133d0"></a>`name`: `LISTENER_CONFIG_SCHEMA`
- <a id="s-7934c40c02"></a>`unit`: `export`

### Declared structure

- <a id="s-5d044ed6d5"></a>`kind`: `"constant"`
- <a id="s-0c22c2004e"></a>`value`: `"gogurt-listener-config/v1"`

## Governing policies

- <a id="pa-5b63ef2909"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:gogurt-listener-runtime:gogurt_listener_runtime](../../../evidence/sources.md#src-259980dd25) — `reference/gogurt/packages/listener-runtime/src/gogurt_listener_runtime/__init__.py`

### Machine authority

- `/external_contract/python/gogurt_listener_runtime.LISTENER_CONFIG_SCHEMA`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: d9036b44cb826dde66619b31d0e313c365111e7a8a5f022b427400f8b1a263f0 -->

```json
{
  "contract": {
    "kind": "constant",
    "value": "gogurt-listener-config/v1"
  },
  "distribution": "gogurt-listener-runtime",
  "module": "gogurt_listener_runtime",
  "name": "LISTENER_CONFIG_SCHEMA",
  "unit": "export"
}
```
