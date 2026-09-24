# a_stove0_media_archive_lib.MediaArchiveProjection.canonical_members

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:a-stove0-media-archive-lib:a-stove0-media-archive-lib-mediaarchivepr-a313c17531:6a0f31a866 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [a-stove0-media-archive-lib](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-058147b248"></a>
- <a id="s-732a531d10"></a>`distribution`: `a-stove0-media-archive-lib`
- <a id="s-289b9c6bbf"></a>`module`: `a_stove0_media_archive_lib`
- <a id="s-59e16fdc51"></a>`name`: `canonical_members`
- <a id="s-1a53460093"></a>`owner`: `a_stove0_media_archive_lib.MediaArchiveProjection`
- <a id="s-13307c881c"></a>`unit`: `member`

### Declared structure

- <a id="s-41f414dfb1"></a>`kind`: `"method"`
- <a id="s-3567f0945c"></a>`signature`: `"\"(self) -> 'Self'\""`

## Maintained corroboration

### Related interface records

- [MediaArchiveProjection](a-stove0-media-archive-lib-mediaarchiveprojection.md)

## Governing policies

- <a id="pa-5f89d0699f"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:a-stove0-media-archive-lib:a_stove0_media_archive_lib](../../../evidence/sources/authorities.md#src-6929294187) — [some-implementations/stove0/targets/media-archive/support/src/a\_stove0\_media\_archive\_lib/\_\_init\_\_.py](../../../../../../some-implementations/stove0/targets/media-archive/support/src/a_stove0_media_archive_lib/__init__.py)

### Machine authority

- `/external_contract/python/a_stove0_media_archive_lib.MediaArchiveProjection.canonical_members`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: b5329f823922df6f06e12ebd89f5efd5e357046866092b3ce61ae6755669d5f5 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self) -> 'Self'\""
  },
  "distribution": "a-stove0-media-archive-lib",
  "module": "a_stove0_media_archive_lib",
  "name": "canonical_members",
  "owner": "a_stove0_media_archive_lib.MediaArchiveProjection",
  "unit": "member"
}
```

</details>
