# gogurt_path_volume_support.PathMountedVolumeAccess.discover

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:gogurt-path-volume-support:gogurt-path-volume-support-pathmountedvol-4de303ab81:dc4613d35e -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [gogurt-path-volume-support](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-5d1c961ccf"></a>
| Field | Shape |
|---|---|
| <a id="s-fab550c782"></a>`contract` | additional keys=`kind`, `signature` |
| <a id="s-f3d136cf63"></a>`distribution` | "gogurt-path-volume-support" |
| <a id="s-0eea4d274e"></a>`module` | "gogurt_path_volume_support" |
| <a id="s-5c62e19d75"></a>`name` | "discover" |
| <a id="s-cb6c562817"></a>`owner` | "gogurt_path_volume_support.PathMountedVolumeAccess" |
| <a id="s-e2a0932a11"></a>`unit` | "member" |

## Maintained corroboration

### Related interface records

- [gogurt_path_volume_support.PathMountedVolumeAccess](gogurt-path-volume-support-pathmountedvolumeaccess.md)

## Governing policies

- <a id="pa-686fe27318"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:gogurt-path-volume-support:gogurt_path_volume_support](../../../evidence/sources.md#src-a67d948855) — `reference/gogurt/mounted-volume/path-support/src/gogurt_path_volume_support/__init__.py`

### Machine authority

- `/external_contract/python/gogurt_path_volume_support.PathMountedVolumeAccess.discover`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: fb7ef54b3ba262d2b403fc4a7e76c0edb1cd5a253437a4ad81593d1ece9d7178 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self) -> 'Sequence[Path]'\""
  },
  "distribution": "gogurt-path-volume-support",
  "module": "gogurt_path_volume_support",
  "name": "discover",
  "owner": "gogurt_path_volume_support.PathMountedVolumeAccess",
  "unit": "member"
}
```
