# a_stove0_media_archive_lib.MediaArchiveProjection.validate_plan_evidence

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:a-stove0-media-archive-lib:a-stove0-media-archive-lib-mediaarchivepr-a631202514:dd386497ce -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [a-stove0-media-archive-lib](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-1952b1769c"></a>
- <a id="s-1f7ac10b71"></a>`distribution`: `a-stove0-media-archive-lib`
- <a id="s-98943302ca"></a>`module`: `a_stove0_media_archive_lib`
- <a id="s-5250eb8e5d"></a>`name`: `validate_plan_evidence`
- <a id="s-a08c34df42"></a>`owner`: `a_stove0_media_archive_lib.MediaArchiveProjection`
- <a id="s-15e7db0a0f"></a>`unit`: `member`

### Declared structure

- <a id="s-c058aaa63c"></a>`kind`: `"method"`
- <a id="s-77c79f37e6"></a>`signature`: `"\"(self, observation_result_sha256s: 'Sequence[str]') -> 'None'\""`

## Maintained corroboration

### Related interface records

- [MediaArchiveProjection](a-stove0-media-archive-lib-mediaarchiveprojection.md)

## Governing policies

- <a id="pa-49b2ec3ff1"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:a-stove0-media-archive-lib:a_stove0_media_archive_lib](../../../evidence/sources/authorities.md#src-6929294187) — [some-implementations/stove0/targets/media-archive/support/src/a\_stove0\_media\_archive\_lib/\_\_init\_\_.py](../../../../../../some-implementations/stove0/targets/media-archive/support/src/a_stove0_media_archive_lib/__init__.py)

### Machine authority

- `/external_contract/python/a_stove0_media_archive_lib.MediaArchiveProjection.validate_plan_evidence`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: e93c2e5b3e261e863a9e63fed2c6e1456c34a1af64e87de258a25bfaf6538642 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self, observation_result_sha256s: 'Sequence[str]') -> 'None'\""
  },
  "distribution": "a-stove0-media-archive-lib",
  "module": "a_stove0_media_archive_lib",
  "name": "validate_plan_evidence",
  "owner": "a_stove0_media_archive_lib.MediaArchiveProjection",
  "unit": "member"
}
```

</details>
