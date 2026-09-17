# gogurt_listener_runtime.ListenerStore.summary

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:gogurt-listener-runtime:gogurt-listener-runtime-listenerstore-summary:2f710c1ebb -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [gogurt-listener-runtime](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-1747991167"></a>
- <a id="s-11eaa40ccc"></a>`distribution`: `gogurt-listener-runtime`
- <a id="s-063baf4794"></a>`module`: `gogurt_listener_runtime`
- <a id="s-47e5d449fc"></a>`name`: `summary`
- <a id="s-86c7e4ba72"></a>`owner`: `gogurt_listener_runtime.ListenerStore`
- <a id="s-e8e491d528"></a>`unit`: `member`

### Declared structure

- <a id="s-379f8f056a"></a>`kind`: `"method"`
- <a id="s-759546c2d6"></a>`signature`: `"\"(self, *, timeout_seconds: 'float' = 30) -> 'dict[str, object]'\""`

## Maintained corroboration

### Related interface records

- [ListenerStore](gogurt-listener-runtime-listenerstore.md)

## Governing policies

- <a id="pa-c0886eaa61"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:gogurt-listener-runtime:gogurt_listener_runtime](../../../evidence/sources/authorities.md#src-259980dd25) — [reference/gogurt/packages/listener-runtime/src/gogurt\_listener\_runtime/\_\_init\_\_.py](../../../../../../reference/gogurt/packages/listener-runtime/src/gogurt_listener_runtime/__init__.py)

### Machine authority

- `/external_contract/python/gogurt_listener_runtime.ListenerStore.summary`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

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

</details>
