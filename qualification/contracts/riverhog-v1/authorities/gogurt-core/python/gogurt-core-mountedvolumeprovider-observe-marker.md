# gogurt_core.MountedVolumeProvider.observe_marker

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:gogurt-core:gogurt-core-mountedvolumeprovider-observe-marker:0f38222625 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [gogurt-core](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-a0b337cc76"></a>
- <a id="s-32d519bf98"></a>`distribution`: `gogurt-core`
- <a id="s-d6277fde4d"></a>`module`: `gogurt_core`
- <a id="s-4b470b2dc2"></a>`name`: `observe_marker`
- <a id="s-664d28fff1"></a>`owner`: `gogurt_core.MountedVolumeProvider`
- <a id="s-e661f69182"></a>`unit`: `member`

### Declared structure

- <a id="s-5241773107"></a>`kind`: `"method"`
- <a id="s-13f5399a78"></a>`signature`: `"\"(self, mount_point: 'Path') -> 'MountedMarkerObservation \| None'\""`

## Maintained corroboration

### Related interface records

- [MountedVolumeProvider](gogurt-core-mountedvolumeprovider.md)

## Governing policies

- <a id="pa-7caf563e03"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:gogurt-core:gogurt_core](../../../evidence/sources/authorities.md#src-e253e4a684) — [reference/gogurt/packages/core/src/gogurt\_core/\_\_init\_\_.py](../../../../../../reference/gogurt/packages/core/src/gogurt_core/__init__.py)

### Machine authority

- `/external_contract/python/gogurt_core.MountedVolumeProvider.observe_marker`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: f31b0afce75b6d2dfb4a20ae1dba35d860d7da50301f4f447894da8aff824734 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, mount_point: 'Path') -> 'MountedMarkerObservation | None'\""
  },
  "distribution": "gogurt-core",
  "module": "gogurt_core",
  "name": "observe_marker",
  "owner": "gogurt_core.MountedVolumeProvider",
  "unit": "member"
}
```

</details>
