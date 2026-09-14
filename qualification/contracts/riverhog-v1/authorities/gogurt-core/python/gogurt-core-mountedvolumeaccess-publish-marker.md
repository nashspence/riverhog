# gogurt_core.MountedVolumeAccess.publish_marker

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:gogurt-core:gogurt-core-mountedvolumeaccess-publish-marker:17262a5eeb -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [gogurt-core](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-11e0a5233a"></a>
- <a id="s-4601446d26"></a>`distribution`: `gogurt-core`
- <a id="s-23ccafaed3"></a>`module`: `gogurt_core`
- <a id="s-ce29208e5a"></a>`name`: `publish_marker`
- <a id="s-024ed15d59"></a>`owner`: `gogurt_core.MountedVolumeAccess`
- <a id="s-494737f855"></a>`unit`: `member`

### Declared structure

- <a id="s-cee94c8a91"></a>`kind`: `"method"`
- <a id="s-c48a384f71"></a>`signature`: `"\"(self, mount_point: 'Path', marker: 'GogurtRouteMarker', *, expected: 'MountedMarkerObservation \| None') -> 'MountedMarkerObservation'\""`

## Maintained corroboration

### Related interface records

- [MountedVolumeAccess](gogurt-core-mountedvolumeaccess.md)

## Governing policies

- <a id="pa-7a01d42e59"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:gogurt-core:gogurt_core](../../../evidence/sources.md#src-e253e4a684) — `reference/gogurt/packages/core/src/gogurt_core/__init__.py`

### Machine authority

- `/external_contract/python/gogurt_core.MountedVolumeAccess.publish_marker`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 2f334608e183664ddb420d1d42984cd2b4eb9b27fbcfdd2fa59b5729f2383f61 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, mount_point: 'Path', marker: 'GogurtRouteMarker', *, expected: 'MountedMarkerObservation | None') -> 'MountedMarkerObservation'\""
  },
  "distribution": "gogurt-core",
  "module": "gogurt_core",
  "name": "publish_marker",
  "owner": "gogurt_core.MountedVolumeAccess",
  "unit": "member"
}
```
