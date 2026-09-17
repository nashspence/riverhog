# gogurt_core.plan_gogurt_marker

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:gogurt-core:gogurt-core-plan-gogurt-marker:21169a9a34 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [gogurt-core](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-4b1312daef"></a>
- <a id="s-825df7efd4"></a>`distribution`: `gogurt-core`
- <a id="s-9dc6dc384c"></a>`module`: `gogurt_core`
- <a id="s-bb1ef40dac"></a>`name`: `plan_gogurt_marker`
- <a id="s-2fe657dabc"></a>`unit`: `export`

### Declared structure

- <a id="s-6e9ecac44c"></a>`kind`: `"function"`
- <a id="s-99254e7dfe"></a>`signature`: `"\"(config_file: 'PathInput', route_name: 'str', mount_point: 'PathInput', *, provider: 'MountedVolumeProvider', force: 'bool' = False) -> 'dict[str, object]'\""`

## Governing policies

- <a id="pa-2c1d79866d"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:gogurt-core:gogurt_core](../../../evidence/sources.md#src-e253e4a684) — [reference/gogurt/packages/core/src/gogurt\_core/\_\_init\_\_.py](../../../../../../reference/gogurt/packages/core/src/gogurt_core/__init__.py)

### Machine authority

- `/external_contract/python/gogurt_core.plan_gogurt_marker`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 9006b4d8d47f71f23d1a345bcc01b1921fe73e8ff34061499bd02b7a8a004a1a -->

```json
{
  "contract": {
    "kind": "function",
    "signature": "\"(config_file: 'PathInput', route_name: 'str', mount_point: 'PathInput', *, provider: 'MountedVolumeProvider', force: 'bool' = False) -> 'dict[str, object]'\""
  },
  "distribution": "gogurt-core",
  "module": "gogurt_core",
  "name": "plan_gogurt_marker",
  "unit": "export"
}
```

</details>
