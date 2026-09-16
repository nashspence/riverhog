# gogurt_core.MountDiscovery

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:gogurt-core:gogurt-core-mountdiscovery:011bd5bcc2 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [gogurt-core](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-4301789d08"></a>
- <a id="s-cdaa308b6f"></a>`distribution`: `gogurt-core`
- <a id="s-6b05bac8ab"></a>`module`: `gogurt_core`
- <a id="s-910ebc970e"></a>`name`: `MountDiscovery`
- <a id="s-4bd0bd0a3f"></a>`unit`: `export`

### Declared structure

- <a id="s-680f5e7d1a"></a>`kind`: `"type-alias"`
- <a id="s-376b59fc72"></a>`value`: `"collections.abc.Callable[[], collections.abc.Sequence[pathlib.Path]]"`

## Governing policies

- <a id="pa-bb4b82ffc3"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:gogurt-core:gogurt_core](../../../evidence/sources.md#src-e253e4a684) — `reference/gogurt/packages/core/src/gogurt_core/__init__.py`

### Machine authority

- `/external_contract/python/gogurt_core.MountDiscovery`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: bc617f0c9a276e4af49d75866b081d88c04aacc9b8d6ec9d8bc149cb071b26f1 -->

```json
{
  "contract": {
    "kind": "type-alias",
    "value": "collections.abc.Callable[[], collections.abc.Sequence[pathlib.Path]]"
  },
  "distribution": "gogurt-core",
  "module": "gogurt_core",
  "name": "MountDiscovery",
  "unit": "export"
}
```

</details>
