# gogurt_core.MountedVolumeAccess.discover

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:gogurt-core:gogurt-core-mountedvolumeaccess-discover:2813cac76f -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [gogurt-core](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-46a8e2eee7"></a>
- <a id="s-2ff1b8568e"></a>`distribution`: `gogurt-core`
- <a id="s-01308283b8"></a>`module`: `gogurt_core`
- <a id="s-22dc1c1a5c"></a>`name`: `discover`
- <a id="s-0a5ab4c587"></a>`owner`: `gogurt_core.MountedVolumeAccess`
- <a id="s-25fb10dda0"></a>`unit`: `member`

### Declared structure

- <a id="s-75a05729de"></a>`kind`: `"method"`
- <a id="s-dc3615acbe"></a>`signature`: `"\"(self) -> 'Sequence[Path]'\""`

## Maintained corroboration

### Related interface records

- [MountedVolumeAccess](gogurt-core-mountedvolumeaccess.md)

## Governing policies

- <a id="pa-85e9be10ac"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:gogurt-core:gogurt_core](../../../evidence/sources.md#src-e253e4a684) — [reference/gogurt/packages/core/src/gogurt\_core/\_\_init\_\_.py](../../../../../../reference/gogurt/packages/core/src/gogurt_core/__init__.py)

### Machine authority

- `/external_contract/python/gogurt_core.MountedVolumeAccess.discover`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 31d70d30bc51510605c061a7c95c76101e9856de4d9e9bfb63920547b6103543 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self) -> 'Sequence[Path]'\""
  },
  "distribution": "gogurt-core",
  "module": "gogurt_core",
  "name": "discover",
  "owner": "gogurt_core.MountedVolumeAccess",
  "unit": "member"
}
```

</details>
