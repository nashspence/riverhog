# gogurt_core.MountedVolumeProvider.publish_marker

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:gogurt-core:gogurt-core-mountedvolumeprovider-publish-marker:7b2e12b722 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [gogurt-core](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-52ab781559"></a>
- <a id="s-27f32a4bbd"></a>`distribution`: `gogurt-core`
- <a id="s-710ad9f7e6"></a>`module`: `gogurt_core`
- <a id="s-2bfbaa5841"></a>`name`: `publish_marker`
- <a id="s-25bed63622"></a>`owner`: `gogurt_core.MountedVolumeProvider`
- <a id="s-74d1a3d590"></a>`unit`: `member`

### Declared structure

- <a id="s-8f068d4357"></a>`kind`: `"method"`
- <a id="s-8692059215"></a>`signature`: `"\"(self, mount_point: 'Path', marker: 'GogurtRouteMarker', *, expected: 'MountedMarkerObservation \| None') -> 'MountedMarkerObservation'\""`

## Maintained corroboration

### Related interface records

- [MountedVolumeProvider](gogurt-core-mountedvolumeprovider.md)

## Governing policies

- <a id="pa-23d26a8a77"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:gogurt-core:gogurt_core](../../../evidence/sources.md#src-e253e4a684) — `reference/gogurt/packages/core/src/gogurt_core/__init__.py`

### Machine authority

- `/external_contract/python/gogurt_core.MountedVolumeProvider.publish_marker`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 816f2986cc56fbca6de8000d9efe1ccb1ae4ecdf8ac2aa7a3401660a94a010f3 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, mount_point: 'Path', marker: 'GogurtRouteMarker', *, expected: 'MountedMarkerObservation | None') -> 'MountedMarkerObservation'\""
  },
  "distribution": "gogurt-core",
  "module": "gogurt_core",
  "name": "publish_marker",
  "owner": "gogurt_core.MountedVolumeProvider",
  "unit": "member"
}
```
