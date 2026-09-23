# gogurt_core.MountedVolumeProvider.discover

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:gogurt-core:gogurt-core-mountedvolumeprovider-discover:1ca5f62925 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [gogurt-core](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-6f86d36cfa"></a>
- <a id="s-dfe55bd8a9"></a>`distribution`: `gogurt-core`
- <a id="s-297c9ca339"></a>`module`: `gogurt_core`
- <a id="s-ea34739640"></a>`name`: `discover`
- <a id="s-acf9025648"></a>`owner`: `gogurt_core.MountedVolumeProvider`
- <a id="s-2aec854911"></a>`unit`: `member`

### Declared structure

- <a id="s-b477c10d56"></a>`kind`: `"method"`
- <a id="s-3227a36a6e"></a>`signature`: `"\"(self) -> 'Sequence[Path]'\""`

## Maintained corroboration

### Related interface records

- [MountedVolumeProvider](gogurt-core-mountedvolumeprovider.md)

## Governing policies

- <a id="pa-c7006f93e1"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:gogurt-core:gogurt_core](../../../evidence/sources/authorities.md#src-e253e4a684) — [some-implementations/gogurt/packages/core/src/gogurt\_core/\_\_init\_\_.py](../../../../../../some-implementations/gogurt/packages/core/src/gogurt_core/__init__.py)

### Machine authority

- `/external_contract/python/gogurt_core.MountedVolumeProvider.discover`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 6cd3b6bf583db24b4a654cc6c388a19d9743a8cc804a9e47ff99dd0324a80859 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self) -> 'Sequence[Path]'\""
  },
  "distribution": "gogurt-core",
  "module": "gogurt_core",
  "name": "discover",
  "owner": "gogurt_core.MountedVolumeProvider",
  "unit": "member"
}
```

</details>
