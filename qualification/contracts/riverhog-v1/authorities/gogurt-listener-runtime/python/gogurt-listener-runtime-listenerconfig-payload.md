# gogurt_listener_runtime.ListenerConfig.payload

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:gogurt-listener-runtime:gogurt-listener-runtime-listenerconfig-payload:4746817be1 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [gogurt-listener-runtime](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-749c60dbbe"></a>
- <a id="s-748cfc0f86"></a>`distribution`: `gogurt-listener-runtime`
- <a id="s-27aa1b763a"></a>`module`: `gogurt_listener_runtime`
- <a id="s-68f2797798"></a>`name`: `payload`
- <a id="s-99342711a3"></a>`owner`: `gogurt_listener_runtime.ListenerConfig`
- <a id="s-b002612a5c"></a>`unit`: `member`

### Declared structure

- <a id="s-1cadc847de"></a>`kind`: `"method"`
- <a id="s-4b5676db30"></a>`signature`: `"\"(self) -> 'dict[str, object]'\""`

## Maintained corroboration

### Related interface records

- [ListenerConfig](gogurt-listener-runtime-listenerconfig.md)

## Governing policies

- <a id="pa-524c2ef8ff"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:gogurt-listener-runtime:gogurt_listener_runtime](../../../evidence/sources.md#src-259980dd25) — `reference/gogurt/packages/listener-runtime/src/gogurt_listener_runtime/__init__.py`

### Machine authority

- `/external_contract/python/gogurt_listener_runtime.ListenerConfig.payload`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: e8052656b621ec72b07d220f3c8d9616345cdbed0ae248782e67a0c51d7151c2 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self) -> 'dict[str, object]'\""
  },
  "distribution": "gogurt-listener-runtime",
  "module": "gogurt_listener_runtime",
  "name": "payload",
  "owner": "gogurt_listener_runtime.ListenerConfig",
  "unit": "member"
}
```

</details>
