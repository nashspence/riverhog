# gogurt_listener_runtime.ListenerLock.__enter__

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:gogurt-listener-runtime:gogurt-listener-runtime-listenerlock-enter:923beae773 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [gogurt-listener-runtime](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-3dbdae9ca2"></a>
- <a id="s-43294b8893"></a>`distribution`: `gogurt-listener-runtime`
- <a id="s-ebbadd6c9c"></a>`module`: `gogurt_listener_runtime`
- <a id="s-dc94bfbabc"></a>`name`: `__enter__`
- <a id="s-4b06e0a35c"></a>`owner`: `gogurt_listener_runtime.ListenerLock`
- <a id="s-3fe247782e"></a>`unit`: `member`

### Declared structure

- <a id="s-47952e4168"></a>`kind`: `"method"`
- <a id="s-131370ba41"></a>`signature`: `"\"(self) -> '_FileLock'\""`

## Maintained corroboration

### Related interface records

- [ListenerLock](gogurt-listener-runtime-listenerlock.md)

## Governing policies

- <a id="pa-e4afc0925d"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:gogurt-listener-runtime:gogurt_listener_runtime](../../../evidence/sources.md#src-259980dd25) — `reference/gogurt/packages/listener-runtime/src/gogurt_listener_runtime/__init__.py`

### Machine authority

- `/external_contract/python/gogurt_listener_runtime.ListenerLock.__enter__`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: b8ad6301b93c60fcb7de32f70820dc92a8a68d0fcd5df85c82dd01cf77a7c0d9 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self) -> '_FileLock'\""
  },
  "distribution": "gogurt-listener-runtime",
  "module": "gogurt_listener_runtime",
  "name": "__enter__",
  "owner": "gogurt_listener_runtime.ListenerLock",
  "unit": "member"
}
```

</details>
