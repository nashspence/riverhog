# stove0_media_archive_target_support.MediaArchiveProjection.verify_digest

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:stove0-media-archive-target-support:stove0-media-archive-target-support-media-5ac539a51e:56cbf924f0 -->

Exact externally visible contract owned by this semantic dossier.

| Audit field | Value |
|---|---|
| Authority | [stove0-media-archive-target-support](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-3639664fd3"></a>
- <a id="s-6b530d096a"></a>`distribution`: `stove0-media-archive-target-support`
- <a id="s-54e336429f"></a>`module`: `stove0_media_archive_target_support`
- <a id="s-ef46278aea"></a>`name`: `verify_digest`
- <a id="s-1a2b56a2b6"></a>`owner`: `stove0_media_archive_target_support.MediaArchiveProjection`
- <a id="s-3d51946cb1"></a>`unit`: `member`

### Declared structure

- <a id="s-f44112cf0a"></a>`kind`: `"method"`
- <a id="s-17da530afe"></a>`signature`: `"\"(self) -> 'Self'\""`

## Maintained corroboration

### Related interface records

- [MediaArchiveProjection](stove0-media-archive-target-support-mediaarchiveprojection.md)

## Governing policies

- <a id="pa-a925171357"></a>[compatibility/python-api/v1](../../../policies/index.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources.md#q-0ba2578a3e)
- [make build](../../../evidence/sources.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources.md#src-47381a6c4f) — `scripts/contract_freeze.py::contract_projection`
- [python:stove0-media-archive-target-support:stove0_media_archive_target_support](../../../evidence/sources.md#src-3caa343b1d) — `reference/stove0/targets/media-archive/support/src/stove0_media_archive_target_support/__init__.py`

### Machine authority

- `/external_contract/python/stove0_media_archive_target_support.MediaArchiveProjection.verify_digest`

### Exact owned JSON

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: ca0314558680ac2481cd1d326b9f793895180be1de1a1b029672f9006aab8ac9 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self) -> 'Self'\""
  },
  "distribution": "stove0-media-archive-target-support",
  "module": "stove0_media_archive_target_support",
  "name": "verify_digest",
  "owner": "stove0_media_archive_target_support.MediaArchiveProjection",
  "unit": "member"
}
```
