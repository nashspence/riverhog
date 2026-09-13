# gogurt_core.MIN_GOGURT_INTERVAL_SECONDS

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:gogurt-core:gogurt-core-min-gogurt-interval-seconds:209a3de6af -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [gogurt-core](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-8156a8be49"></a>
| Field | Shape |
|---|---|
| <a id="s-cd13bcdb08"></a>`contract` | additional keys=`kind`, `value` |
| <a id="s-52c194f0df"></a>`distribution` | "gogurt-core" |
| <a id="s-622c180cc9"></a>`module` | "gogurt_core" |
| <a id="s-ccbea54025"></a>`name` | "MIN_GOGURT_INTERVAL_SECONDS" |
| <a id="s-ff88e48ae0"></a>`unit` | "export" |

## Governing policies

- <a id="pa-55e91fde7c"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:gogurt-core:gogurt_core](../../../evidence/sources.md#src-e253e4a684) — `reference/gogurt/packages/core/src/gogurt_core/__init__.py`

### Machine authority

- `/external_contract/python/gogurt_core.MIN_GOGURT_INTERVAL_SECONDS`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: d217ae709c071a701fbd23784d13e2ecc98dcb0a0216ee0adfd260d12e785d58 -->

```json
{
  "contract": {
    "kind": "constant",
    "value": 0.1
  },
  "distribution": "gogurt-core",
  "module": "gogurt_core",
  "name": "MIN_GOGURT_INTERVAL_SECONDS",
  "unit": "export"
}
```
