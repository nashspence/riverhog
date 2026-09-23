# gogurt_core.plan_gogurt_action

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:gogurt-core:gogurt-core-plan-gogurt-action:f6099dc860 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [gogurt-core](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-f63babb7a0"></a>
- <a id="s-d75b490e5a"></a>`distribution`: `gogurt-core`
- <a id="s-dbb9468a77"></a>`module`: `gogurt_core`
- <a id="s-452a9dacf9"></a>`name`: `plan_gogurt_action`
- <a id="s-bfad8acaa5"></a>`unit`: `export`

### Declared structure

- <a id="s-e3aaddae20"></a>`kind`: `"function"`
- <a id="s-b5024bce1c"></a>`signature`: `"\"(config_file: 'PathInput', mount_point: 'PathInput', *, provider: 'MountedVolumeProvider', actions_dir: 'PathInput \| None' = None) -> 'dict[str, object]'\""`

## Governing policies

- <a id="pa-ab47bc51b5"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:gogurt-core:gogurt_core](../../../evidence/sources/authorities.md#src-e253e4a684) — [some-implementations/gogurt/packages/core/src/gogurt\_core/\_\_init\_\_.py](../../../../../../some-implementations/gogurt/packages/core/src/gogurt_core/__init__.py)

### Machine authority

- `/external_contract/python/gogurt_core.plan_gogurt_action`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 9f8c6f64ed181541f26bed5cc90a32f94e503ed85ac696406da733ad93a79e90 -->

```json
{
  "contract": {
    "kind": "function",
    "signature": "\"(config_file: 'PathInput', mount_point: 'PathInput', *, provider: 'MountedVolumeProvider', actions_dir: 'PathInput | None' = None) -> 'dict[str, object]'\""
  },
  "distribution": "gogurt-core",
  "module": "gogurt_core",
  "name": "plan_gogurt_action",
  "unit": "export"
}
```

</details>
