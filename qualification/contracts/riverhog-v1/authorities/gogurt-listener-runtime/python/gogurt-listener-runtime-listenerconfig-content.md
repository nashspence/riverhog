# gogurt_listener_runtime.ListenerConfig.content

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:gogurt-listener-runtime:gogurt-listener-runtime-listenerconfig-content:fbc38e9269 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [gogurt-listener-runtime](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-ff7b4374e3"></a>
- <a id="s-77eaf7915f"></a>`distribution`: `gogurt-listener-runtime`
- <a id="s-546b936424"></a>`module`: `gogurt_listener_runtime`
- <a id="s-be837958de"></a>`name`: `content`
- <a id="s-f1e9738f4b"></a>`owner`: `gogurt_listener_runtime.ListenerConfig`
- <a id="s-b8be27bb32"></a>`unit`: `member`

### Declared structure

- <a id="s-cc05bd36cc"></a>`kind`: `"method"`
- <a id="s-64619a5be5"></a>`signature`: `"\"(self) -> 'bytes'\""`

## Maintained corroboration

### Related interface records

- [ListenerConfig](gogurt-listener-runtime-listenerconfig.md)

## Governing policies

- <a id="pa-ce3afd8f61"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:gogurt-listener-runtime:gogurt_listener_runtime](../../../evidence/sources.md#src-259980dd25) — `reference/gogurt/packages/listener-runtime/src/gogurt_listener_runtime/__init__.py`

### Machine authority

- `/external_contract/python/gogurt_listener_runtime.ListenerConfig.content`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: de6efe0e708af3c28241d7239744feb526271102f4c2bdd34d36f99a9a64f542 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self) -> 'bytes'\""
  },
  "distribution": "gogurt-listener-runtime",
  "module": "gogurt_listener_runtime",
  "name": "content",
  "owner": "gogurt_listener_runtime.ListenerConfig",
  "unit": "member"
}
```

</details>
