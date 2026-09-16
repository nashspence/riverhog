# gogurt_core.write_gogurt_marker

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:gogurt-core:gogurt-core-write-gogurt-marker:255b57f3f9 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [gogurt-core](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-0dfa97cf7e"></a>
- <a id="s-b653df3114"></a>`distribution`: `gogurt-core`
- <a id="s-3d6389f0ab"></a>`module`: `gogurt_core`
- <a id="s-75d75dceb8"></a>`name`: `write_gogurt_marker`
- <a id="s-7227331e53"></a>`unit`: `export`

### Declared structure

- <a id="s-38e7e6735b"></a>`kind`: `"function"`
- <a id="s-fd9d8c1c8d"></a>`signature`: `"\"(config_file: 'PathInput', route_name: 'str', mount_point: 'PathInput', *, provider: 'MountedVolumeProvider', force: 'bool' = False) -> 'MountedMarkerObservation'\""`

## Governing policies

- <a id="pa-91030e8530"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:gogurt-core:gogurt_core](../../../evidence/sources.md#src-e253e4a684) — `reference/gogurt/packages/core/src/gogurt_core/__init__.py`

### Machine authority

- `/external_contract/python/gogurt_core.write_gogurt_marker`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: e302f45c288aba1c11b7bc23704fec823f4d8c945ae967710b9d215a4443b0d7 -->

```json
{
  "contract": {
    "kind": "function",
    "signature": "\"(config_file: 'PathInput', route_name: 'str', mount_point: 'PathInput', *, provider: 'MountedVolumeProvider', force: 'bool' = False) -> 'MountedMarkerObservation'\""
  },
  "distribution": "gogurt-core",
  "module": "gogurt_core",
  "name": "write_gogurt_marker",
  "unit": "export"
}
```

</details>
