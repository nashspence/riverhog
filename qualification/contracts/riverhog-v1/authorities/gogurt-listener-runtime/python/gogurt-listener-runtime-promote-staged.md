# gogurt_listener_runtime.promote_staged

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:gogurt-listener-runtime:gogurt-listener-runtime-promote-staged:2763418743 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [gogurt-listener-runtime](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-f1da5fb921"></a>
- <a id="s-5d7b3983fc"></a>`distribution`: `gogurt-listener-runtime`
- <a id="s-4e8dcdab94"></a>`module`: `gogurt_listener_runtime`
- <a id="s-284ea430e2"></a>`name`: `promote_staged`
- <a id="s-d330e2090e"></a>`unit`: `export`

### Declared structure

- <a id="s-8d4f19df4a"></a>`kind`: `"function"`
- <a id="s-9e396bff96"></a>`signature`: `"\"(temporary: 'Path', destination: 'Path', *, mode: 'int') -> 'None'\""`

## Governing policies

- <a id="pa-1f500ba153"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:gogurt-listener-runtime:gogurt_listener_runtime](../../../evidence/sources.md#src-259980dd25) — `reference/gogurt/packages/listener-runtime/src/gogurt_listener_runtime/__init__.py`

### Machine authority

- `/external_contract/python/gogurt_listener_runtime.promote_staged`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: e736218bff053b6a73e7521a98af53497c9e269d756cca330d6c58f2a5838349 -->

```json
{
  "contract": {
    "kind": "function",
    "signature": "\"(temporary: 'Path', destination: 'Path', *, mode: 'int') -> 'None'\""
  },
  "distribution": "gogurt-listener-runtime",
  "module": "gogurt_listener_runtime",
  "name": "promote_staged",
  "unit": "export"
}
```

</details>
