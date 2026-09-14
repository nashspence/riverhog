# gogurt_core.MountedVolumeAccess.observe_marker

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:gogurt-core:gogurt-core-mountedvolumeaccess-observe-marker:778c4df684 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [gogurt-core](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-5e472d5bca"></a>
- <a id="s-5ae0fb51fc"></a>`distribution`: `gogurt-core`
- <a id="s-c006529db0"></a>`module`: `gogurt_core`
- <a id="s-e7e3ac0b77"></a>`name`: `observe_marker`
- <a id="s-f9bc1ddd52"></a>`owner`: `gogurt_core.MountedVolumeAccess`
- <a id="s-a82dff3dd1"></a>`unit`: `member`

### Declared structure

- <a id="s-8ddf20a36b"></a>`kind`: `"method"`
- <a id="s-ceb0183ba5"></a>`signature`: `"\"(self, mount_point: 'Path') -> 'MountedMarkerObservation \| None'\""`

## Maintained corroboration

### Related interface records

- [MountedVolumeAccess](gogurt-core-mountedvolumeaccess.md)

## Governing policies

- <a id="pa-be73d1cc6d"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:gogurt-core:gogurt_core](../../../evidence/sources.md#src-e253e4a684) — `reference/gogurt/packages/core/src/gogurt_core/__init__.py`

### Machine authority

- `/external_contract/python/gogurt_core.MountedVolumeAccess.observe_marker`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 226994d3ac58063664799ee3622b0e856e94c2c109db77d436133058c7aac1c8 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, mount_point: 'Path') -> 'MountedMarkerObservation | None'\""
  },
  "distribution": "gogurt-core",
  "module": "gogurt_core",
  "name": "observe_marker",
  "owner": "gogurt_core.MountedVolumeAccess",
  "unit": "member"
}
```
