# gogurt_core.MountedVolumeProvider.reference

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:gogurt-core:gogurt-core-mountedvolumeprovider-reference:a02faecf1a -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [gogurt-core](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-13a73ab366"></a>
- <a id="s-e9775d0cbd"></a>`distribution`: `gogurt-core`
- <a id="s-f7a6bdabda"></a>`module`: `gogurt_core`
- <a id="s-3bb5dddcbd"></a>`name`: `reference`
- <a id="s-03ce566616"></a>`owner`: `gogurt_core.MountedVolumeProvider`
- <a id="s-236075b76f"></a>`unit`: `member`

### Declared structure

- <a id="s-72d5f8ed7f"></a>`kind`: `"property"`
- <a id="s-007798be0b"></a>`signature`: `"\"(self) -> 'GogurtProviderReference'\""`

## Maintained corroboration

### Related interface records

- [MountedVolumeProvider](gogurt-core-mountedvolumeprovider.md)

## Governing policies

- <a id="pa-f1d412ab5a"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:gogurt-core:gogurt_core](../../../evidence/sources/authorities.md#src-e253e4a684) — [reference/gogurt/packages/core/src/gogurt\_core/\_\_init\_\_.py](../../../../../../reference/gogurt/packages/core/src/gogurt_core/__init__.py)

### Machine authority

- `/external_contract/python/gogurt_core.MountedVolumeProvider.reference`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 23da7e66d0c4a47c1c51b269510215bc8418ed97731ac49096121e61685ed659 -->

```json
{
  "contract": {
    "kind": "property",
    "signature": "\"(self) -> 'GogurtProviderReference'\""
  },
  "distribution": "gogurt-core",
  "module": "gogurt_core",
  "name": "reference",
  "owner": "gogurt_core.MountedVolumeProvider",
  "unit": "member"
}
```

</details>
