# gogurt_listener_runtime.ListenerStore.summary

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:gogurt-listener-runtime:gogurt-listener-runtime-listenerstore-summary:2f710c1ebb -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [gogurt-listener-runtime](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-1747991167"></a>
| Field | Shape |
|---|---|
| <a id="s-97c9e66c90"></a>`contract` | additional keys=`kind`, `signature` |
| <a id="s-11eaa40ccc"></a>`distribution` | "gogurt-listener-runtime" |
| <a id="s-063baf4794"></a>`module` | "gogurt_listener_runtime" |
| <a id="s-47e5d449fc"></a>`name` | "summary" |
| <a id="s-86c7e4ba72"></a>`owner` | "gogurt_listener_runtime.ListenerStore" |
| <a id="s-e8e491d528"></a>`unit` | "member" |

## Maintained corroboration

### Related interface records

- [gogurt_listener_runtime.ListenerStore](gogurt-listener-runtime-listenerstore.md)

## Governing policies

- <a id="pa-c0886eaa61"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:gogurt-listener-runtime:gogurt_listener_runtime](../../../evidence/sources.md#src-259980dd25) — `reference/gogurt/packages/listener-runtime/src/gogurt_listener_runtime/__init__.py`

### Machine authority

- `/external_contract/python/gogurt_listener_runtime.ListenerStore.summary`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 53bf7579856c55a036aa51666e0cc98b46f8457bd99669d9b3b2a568418481e8 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, *, timeout_seconds: 'float' = 30) -> 'dict[str, object]'\""
  },
  "distribution": "gogurt-listener-runtime",
  "module": "gogurt_listener_runtime",
  "name": "summary",
  "owner": "gogurt_listener_runtime.ListenerStore",
  "unit": "member"
}
```
