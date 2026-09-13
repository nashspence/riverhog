# gogurt_listener_runtime.LISTENER_STATUS_SCHEMA

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:gogurt-listener-runtime:gogurt-listener-runtime-listener-status-schema:762d8f60e3 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [gogurt-listener-runtime](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-19084b2d57"></a>
| Field | Shape |
|---|---|
| <a id="s-c00e1bad10"></a>`contract` | additional keys=`kind`, `value` |
| <a id="s-d5ae59c1c3"></a>`distribution` | "gogurt-listener-runtime" |
| <a id="s-90c71950bb"></a>`module` | "gogurt_listener_runtime" |
| <a id="s-922a7810f0"></a>`name` | "LISTENER_STATUS_SCHEMA" |
| <a id="s-ff7d933c39"></a>`unit` | "export" |

## Governing policies

- <a id="pa-1b0c940c00"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:gogurt-listener-runtime:gogurt_listener_runtime](../../../evidence/sources.md#src-259980dd25) — `reference/gogurt/packages/listener-runtime/src/gogurt_listener_runtime/__init__.py`

### Machine authority

- `/external_contract/python/gogurt_listener_runtime.LISTENER_STATUS_SCHEMA`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 08ce47f32c38f9aeb902ee49a337150c7093b5a6d79f387afb809a2cf1abf55f -->

```json
{
  "contract": {
    "kind": "constant",
    "value": "gogurt-listener-status/v1"
  },
  "distribution": "gogurt-listener-runtime",
  "module": "gogurt_listener_runtime",
  "name": "LISTENER_STATUS_SCHEMA",
  "unit": "export"
}
```
