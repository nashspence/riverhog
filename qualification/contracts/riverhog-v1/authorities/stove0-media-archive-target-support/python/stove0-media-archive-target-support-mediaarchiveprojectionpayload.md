# stove0_media_archive_target_support.MediaArchiveProjectionPayload

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-media-archive-target-support:stove0-media-archive-target-support-media-e09af878c8:367bf9ddd7 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-media-archive-target-support](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-fc6f298259"></a>
| Field | Shape |
|---|---|
| <a id="s-4e90ba0f5d"></a>`contract` | additional keys=`kind`, `schema_sha256`, `signature` |
| <a id="s-6b8380db07"></a>`distribution` | "stove0-media-archive-target-support" |
| <a id="s-cd5d97b04f"></a>`module` | "stove0_media_archive_target_support" |
| <a id="s-d82e5d5fa9"></a>`name` | "MediaArchiveProjectionPayload" |
| <a id="s-fccb50dbab"></a>`unit` | "export" |

## Maintained corroboration

### Related interface records

- [stove0_media_archive_target_support.MediaArchiveProjectionPayload.canonical_members](stove0-media-archive-target-support-mediaarchiveprojectionpayload-canonical-members.md)

## Governing policies

- <a id="pa-ff35b15178"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-media-archive-target-support:stove0_media_archive_target_support](../../../evidence/sources.md#src-3caa343b1d) — `reference/stove0/targets/media-archive/support/src/stove0_media_archive_target_support/__init__.py`

### Machine authority

- `/external_contract/python/stove0_media_archive_target_support.MediaArchiveProjectionPayload`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 223579225ca66460a5780294f2780194ea5f663dc3919885e725898c030237cc -->

```json
{
  "contract": {
    "kind": "class",
    "schema_sha256": "637ed91c9b5702d779d7dfa48c011e623e38c6c7fb91ca7a4f8e01871aeafd09",
    "signature": "\"(*, format: Literal['stove0-media-archive-projection/v1'] = 'stove0-media-archive-projection/v1', observation_result_sha256s: tuple[str, ...], items: Annotated[tuple[stove0_media_archive_target_support.projection.MediaProjectionItem, ...], MinLen(min_length=1)], retained_xmp_sidecars: tuple[stove0_media_archive_target_support.projection.RetainedXmpSidecar, ...] = ()) -> None\""
  },
  "distribution": "stove0-media-archive-target-support",
  "module": "stove0_media_archive_target_support",
  "name": "MediaArchiveProjectionPayload",
  "unit": "export"
}
```
