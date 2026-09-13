# stove0_media_archive_target_support.resolve_media_archive_preflight_projection

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-media-archive-target-support:stove0-media-archive-target-support-resol-c20c93900a:9c50e63ce7 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-media-archive-target-support](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-f97d477cdd"></a>
| Field | Shape |
|---|---|
| <a id="s-ba2b270aca"></a>`contract` | additional keys=`kind`, `signature` |
| <a id="s-a44cb6d2e3"></a>`distribution` | "stove0-media-archive-target-support" |
| <a id="s-a6829368ff"></a>`module` | "stove0_media_archive_target_support" |
| <a id="s-838cb0a821"></a>`name` | "resolve_media_archive_preflight_projection" |
| <a id="s-46d6cacaad"></a>`unit` | "export" |

## Governing policies

- <a id="pa-dcead77e30"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-media-archive-target-support:stove0_media_archive_target_support](../../../evidence/sources.md#src-3caa343b1d) — `reference/stove0/targets/media-archive/support/src/stove0_media_archive_target_support/__init__.py`

### Machine authority

- `/external_contract/python/stove0_media_archive_target_support.resolve_media_archive_preflight_projection`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: d2bdce4e88f2ed024ff9e4e35a7cb5761e3b70ef0ba365440dcf4cf282323ea6 -->

```json
{
  "contract": {
    "kind": "function",
    "signature": "\"(request: 'TargetPreflightRequest', *, policy: 'MediaProjectionPolicy', archive_directory: 'str', archive_suffix: 'str') -> 'MediaArchiveProjection'\""
  },
  "distribution": "stove0-media-archive-target-support",
  "module": "stove0_media_archive_target_support",
  "name": "resolve_media_archive_preflight_projection",
  "unit": "export"
}
```
