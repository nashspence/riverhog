# gogurt_listener_runtime.ListenerAdapter.stop

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:gogurt-listener-runtime:gogurt-listener-runtime-listeneradapter-stop:2dd6b5b46b -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [gogurt-listener-runtime](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-c149dd66de"></a>
| Field | Shape |
|---|---|
| <a id="s-c46433297b"></a>`contract` | additional keys=`kind`, `signature` |
| <a id="s-715523c276"></a>`distribution` | "gogurt-listener-runtime" |
| <a id="s-766ed8029e"></a>`module` | "gogurt_listener_runtime" |
| <a id="s-9ccb03eff9"></a>`name` | "stop" |
| <a id="s-f80a47c3b0"></a>`owner` | "gogurt_listener_runtime.ListenerAdapter" |
| <a id="s-a6498b26c9"></a>`unit` | "member" |

## Maintained corroboration

### Related interface records

- [gogurt_listener_runtime.ListenerAdapter](gogurt-listener-runtime-listeneradapter.md)

## Governing policies

- <a id="pa-d5b457db93"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:gogurt-listener-runtime:gogurt_listener_runtime](../../../evidence/sources.md#src-259980dd25) — `reference/gogurt/packages/listener-runtime/src/gogurt_listener_runtime/__init__.py`

### Machine authority

- `/external_contract/python/gogurt_listener_runtime.ListenerAdapter.stop`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 3686bc2d5cf868648726f928db3361c2781b0361468241c109de6dac57f30141 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, paths: 'ListenerRuntimePaths') -> 'None'\""
  },
  "distribution": "gogurt-listener-runtime",
  "module": "gogurt_listener_runtime",
  "name": "stop",
  "owner": "gogurt_listener_runtime.ListenerAdapter",
  "unit": "member"
}
```
