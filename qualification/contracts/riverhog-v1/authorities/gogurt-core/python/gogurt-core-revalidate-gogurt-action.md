# gogurt_core.revalidate_gogurt_action

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:gogurt-core:gogurt-core-revalidate-gogurt-action:4ede6c53a7 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [gogurt-core](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-7af6ff8279"></a>
- <a id="s-d4c2f3f4de"></a>`distribution`: `gogurt-core`
- <a id="s-65970ec788"></a>`module`: `gogurt_core`
- <a id="s-2183758bd0"></a>`name`: `revalidate_gogurt_action`
- <a id="s-d307950e6b"></a>`unit`: `export`

### Declared structure

- <a id="s-594c1bbf04"></a>`kind`: `"function"`
- <a id="s-bf70bbc449"></a>`signature`: `"\"(plan: 'Mapping[str, object]', *, provider: 'MountedVolumeProvider') -> 'list[str]'\""`

## Governing policies

- <a id="pa-17c81cfc15"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:gogurt-core:gogurt_core](../../../evidence/sources.md#src-e253e4a684) — `reference/gogurt/packages/core/src/gogurt_core/__init__.py`

### Machine authority

- `/external_contract/python/gogurt_core.revalidate_gogurt_action`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 3c91deb6dd141d0c64ee78d061a19588a5eecd586af8c56fb3ea90418f805da9 -->

```json
{
  "contract": {
    "kind": "function",
    "signature": "\"(plan: 'Mapping[str, object]', *, provider: 'MountedVolumeProvider') -> 'list[str]'\""
  },
  "distribution": "gogurt-core",
  "module": "gogurt_core",
  "name": "revalidate_gogurt_action",
  "unit": "export"
}
```
