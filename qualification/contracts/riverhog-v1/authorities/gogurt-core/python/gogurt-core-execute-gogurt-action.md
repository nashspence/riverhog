# gogurt_core.execute_gogurt_action

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:gogurt-core:gogurt-core-execute-gogurt-action:634f219f85 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [gogurt-core](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-5d7c150f5b"></a>
- <a id="s-81f67d4ff5"></a>`distribution`: `gogurt-core`
- <a id="s-b9df29100f"></a>`module`: `gogurt_core`
- <a id="s-c732b4836d"></a>`name`: `execute_gogurt_action`
- <a id="s-a26bd4c886"></a>`unit`: `export`

### Declared structure

- <a id="s-b6f982aa66"></a>`kind`: `"function"`
- <a id="s-229a8d2688"></a>`signature`: `"\"(plan: 'Mapping[str, object]', *, provider: 'MountedVolumeProvider', capture_output: 'bool' = False) -> 'subprocess.CompletedProcess[str]'\""`

## Governing policies

- <a id="pa-a87e5d3877"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:gogurt-core:gogurt_core](../../../evidence/sources.md#src-e253e4a684) — [reference/gogurt/packages/core/src/gogurt\_core/\_\_init\_\_.py](../../../../../../reference/gogurt/packages/core/src/gogurt_core/__init__.py)

### Machine authority

- `/external_contract/python/gogurt_core.execute_gogurt_action`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: aa0bd2544e26d80cb39304047f256097ee049a0f354954f1aa45fcbc8c0dbccb -->

```json
{
  "contract": {
    "kind": "function",
    "signature": "\"(plan: 'Mapping[str, object]', *, provider: 'MountedVolumeProvider', capture_output: 'bool' = False) -> 'subprocess.CompletedProcess[str]'\""
  },
  "distribution": "gogurt-core",
  "module": "gogurt_core",
  "name": "execute_gogurt_action",
  "unit": "export"
}
```

</details>
