# stove0_media_archive_target_support.MediaArchiveProjection

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-media-archive-target-support:stove0-media-archive-target-support-media-14aa64971e:30c961b4c0 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-media-archive-target-support](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-b08e612f85"></a>
| Field | Shape |
|---|---|
| <a id="s-4b442b7eaf"></a>`contract` | additional keys=`kind`, `schema_sha256`, `signature` |
| <a id="s-eef714bddf"></a>`distribution` | "stove0-media-archive-target-support" |
| <a id="s-b257ccf303"></a>`module` | "stove0_media_archive_target_support" |
| <a id="s-493ebd5dd6"></a>`name` | "MediaArchiveProjection" |
| <a id="s-7b45bd41b1"></a>`unit` | "export" |

## Maintained corroboration

### Related interface records

- [stove0_media_archive_target_support.MediaArchiveProjection.item_for](stove0-media-archive-target-support-mediaarchiveprojection-item-for.md)
- [stove0_media_archive_target_support.MediaArchiveProjection.verify_digest](stove0-media-archive-target-support-mediaarchiveprojection-verify-digest.md)
- [stove0_media_archive_target_support.MediaArchiveProjection.validate_plan_evidence](stove0-media-archive-target-support-mediaarchiveprojection-validate-plan-evidence.md)
- [stove0_media_archive_target_support.MediaArchiveProjection.seal](stove0-media-archive-target-support-mediaarchiveprojection-seal.md)

## Governing policies

- <a id="pa-284debfb35"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-media-archive-target-support:stove0_media_archive_target_support](../../../evidence/sources.md#src-3caa343b1d) — `reference/stove0/targets/media-archive/support/src/stove0_media_archive_target_support/__init__.py`

### Machine authority

- `/external_contract/python/stove0_media_archive_target_support.MediaArchiveProjection`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 87e98c62bb9acb6d2a5f85a7679dd89ff998edf80df2890c7090d0e7706e5f04 -->

```json
{
  "contract": {
    "kind": "class",
    "schema_sha256": "96d4a855d44b0272b52404a36f74d1fbee4ab33a060aa246a2fa2d070d7ccb8b",
    "signature": "\"(*, format: Literal['stove0-media-archive-projection/v1'] = 'stove0-media-archive-projection/v1', observation_result_sha256s: tuple[str, ...], items: Annotated[tuple[stove0_media_archive_target_support.projection.MediaProjectionItem, ...], MinLen(min_length=1)], retained_xmp_sidecars: tuple[stove0_media_archive_target_support.projection.RetainedXmpSidecar, ...] = (), projection_sha256: Annotated[str, _PydanticGeneralMetadata(pattern='^[0-9a-f]{64}$')]) -> None\""
  },
  "distribution": "stove0-media-archive-target-support",
  "module": "stove0_media_archive_target_support",
  "name": "MediaArchiveProjection",
  "unit": "export"
}
```
