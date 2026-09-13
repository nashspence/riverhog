# stove0_media_archive_target_support.render_projection_xmp

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-media-archive-target-support:stove0-media-archive-target-support-rende-ea7a4d6f44:64dd3eee42 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-media-archive-target-support](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-04cc32919c"></a>
| Field | Shape |
|---|---|
| <a id="s-062419608c"></a>`contract` | additional keys=`kind`, `signature` |
| <a id="s-b47e94e1e7"></a>`distribution` | "stove0-media-archive-target-support" |
| <a id="s-7ed7254a83"></a>`module` | "stove0_media_archive_target_support" |
| <a id="s-d95d958f94"></a>`name` | "render_projection_xmp" |
| <a id="s-de94852aa7"></a>`unit` | "export" |

## Governing policies

- <a id="pa-a6f97de044"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-media-archive-target-support:stove0_media_archive_target_support](../../../evidence/sources.md#src-3caa343b1d) — `reference/stove0/targets/media-archive/support/src/stove0_media_archive_target_support/__init__.py`

### Machine authority

- `/external_contract/python/stove0_media_archive_target_support.render_projection_xmp`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: c9cf40a0ea18305ef0a70672dafee3d4625fd1a93475a1b0b44f9bdbbf492ea9 -->

```json
{
  "contract": {
    "kind": "function",
    "signature": "\"(item: 'MediaProjectionItem', *, tags: 'Sequence[str]' = ()) -> 'bytes'\""
  },
  "distribution": "stove0-media-archive-target-support",
  "module": "stove0_media_archive_target_support",
  "name": "render_projection_xmp",
  "unit": "export"
}
```
