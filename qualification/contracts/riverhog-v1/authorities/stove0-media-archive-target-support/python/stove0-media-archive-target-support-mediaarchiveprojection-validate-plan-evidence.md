# stove0_media_archive_target_support.MediaArchiveProjection.validate_plan_evidence

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-media-archive-target-support:stove0-media-archive-target-support-media-604b829027:787872120f -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-media-archive-target-support](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-f1f5c262a5"></a>
- <a id="s-db4842c697"></a>`distribution`: `stove0-media-archive-target-support`
- <a id="s-6afb572cae"></a>`module`: `stove0_media_archive_target_support`
- <a id="s-2618e60916"></a>`name`: `validate_plan_evidence`
- <a id="s-867946d7fd"></a>`owner`: `stove0_media_archive_target_support.MediaArchiveProjection`
- <a id="s-777fee86ca"></a>`unit`: `member`

### Declared structure

- <a id="s-07ccb69245"></a>`kind`: `"method"`
- <a id="s-a1dcd10759"></a>`signature`: `"\"(self, observation_result_sha256s: 'Sequence[str]') -> 'None'\""`

## Maintained corroboration

### Related interface records

- [MediaArchiveProjection](stove0-media-archive-target-support-mediaarchiveprojection.md)

## Governing policies

- <a id="pa-73301b8a4b"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-media-archive-target-support:stove0_media_archive_target_support](../../../evidence/sources.md#src-3caa343b1d) — `reference/stove0/targets/media-archive/support/src/stove0_media_archive_target_support/__init__.py`

### Machine authority

- `/external_contract/python/stove0_media_archive_target_support.MediaArchiveProjection.validate_plan_evidence`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: ee3fc240c2367dbb82e59c1b17fd0370a0aa789e515260ebd67b56d8a418607d -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, observation_result_sha256s: 'Sequence[str]') -> 'None'\""
  },
  "distribution": "stove0-media-archive-target-support",
  "module": "stove0_media_archive_target_support",
  "name": "validate_plan_evidence",
  "owner": "stove0_media_archive_target_support.MediaArchiveProjection",
  "unit": "member"
}
```

</details>
