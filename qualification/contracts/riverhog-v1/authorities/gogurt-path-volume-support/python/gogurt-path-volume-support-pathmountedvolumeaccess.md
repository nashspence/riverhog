# gogurt_path_volume_support.PathMountedVolumeAccess

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:gogurt-path-volume-support:gogurt-path-volume-support-pathmountedvolumeaccess:4928ca9832 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [gogurt-path-volume-support](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-8d9f14a03b"></a>
- <a id="s-c289068450"></a>`distribution`: `gogurt-path-volume-support`
- <a id="s-22e56d4f40"></a>`module`: `gogurt_path_volume_support`
- <a id="s-0e478b45cf"></a>`name`: `PathMountedVolumeAccess`
- <a id="s-6e5c658a6c"></a>`unit`: `export`

### Declared structure

- <a id="s-b0537f3240"></a>`kind`: `"class"`
- <a id="s-a900fa1b7b"></a>`signature`: `"\"(discover_mounts: 'Callable[[], Sequence[Path]]') -> None\""`

#### Dataclass fields

| Field | Type | Default |
|---|---|---|
| <a id="s-972ed64c78"></a>`discover_mounts` | `'Callable[[], Sequence[Path]]'` | `required` |

## Maintained corroboration

### Related interface records

- [gogurt_path_volume_support.PathMountedVolumeAccess.discover](gogurt-path-volume-support-pathmountedvolumeaccess-discover.md)
- [gogurt_path_volume_support.PathMountedVolumeAccess.publish_marker](gogurt-path-volume-support-pathmountedvolumeaccess-publish-marker.md)
- [gogurt_path_volume_support.PathMountedVolumeAccess.observe_marker](gogurt-path-volume-support-pathmountedvolumeaccess-observe-marker.md)

## Governing policies

- <a id="pa-e715e6cd5d"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:gogurt-path-volume-support:gogurt_path_volume_support](../../../evidence/sources.md#src-a67d948855) — `reference/gogurt/mounted-volume/path-support/src/gogurt_path_volume_support/__init__.py`

### Machine authority

- `/external_contract/python/gogurt_path_volume_support.PathMountedVolumeAccess`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: ba2060599216fd642f8025b3579f3925f809413a84a823977ef571014d1fcdd6 -->

```json
{
  "contract": {
    "fields": [
      {
        "default": "required",
        "name": "discover_mounts",
        "type": "'Callable[[], Sequence[Path]]'"
      }
    ],
    "kind": "class",
    "signature": "\"(discover_mounts: 'Callable[[], Sequence[Path]]') -> None\""
  },
  "distribution": "gogurt-path-volume-support",
  "module": "gogurt_path_volume_support",
  "name": "PathMountedVolumeAccess",
  "unit": "export"
}
```
