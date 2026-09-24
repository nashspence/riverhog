# a_stove0_media_archive_lib.MediaArchiveProjection.verify_digest

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:a-stove0-media-archive-lib:a-stove0-media-archive-lib-mediaarchivepr-de4a12c1ae:29cefcaaaf -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [a-stove0-media-archive-lib](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-e9612ea9bf"></a>
- <a id="s-2e2ce73743"></a>`distribution`: `a-stove0-media-archive-lib`
- <a id="s-1f2bcf417d"></a>`module`: `a_stove0_media_archive_lib`
- <a id="s-074ace12a2"></a>`name`: `verify_digest`
- <a id="s-6257dcc56f"></a>`owner`: `a_stove0_media_archive_lib.MediaArchiveProjection`
- <a id="s-7bfcf3e05c"></a>`unit`: `member`

### Declared structure

- <a id="s-52cdbdca4e"></a>`kind`: `"method"`
- <a id="s-cdf0a02d5b"></a>`signature`: `"\"(self) -> 'Self'\""`

## Maintained corroboration

### Related interface records

- [MediaArchiveProjection](a-stove0-media-archive-lib-mediaarchiveprojection.md)

## Governing policies

- <a id="pa-cdf7538019"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:a-stove0-media-archive-lib:a_stove0_media_archive_lib](../../../evidence/sources/authorities.md#src-6929294187) — [some-implementations/stove0/targets/media-archive/support/src/a\_stove0\_media\_archive\_lib/\_\_init\_\_.py](../../../../../../some-implementations/stove0/targets/media-archive/support/src/a_stove0_media_archive_lib/__init__.py)

### Machine authority

- `/external_contract/python/a_stove0_media_archive_lib.MediaArchiveProjection.verify_digest`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: cde2f76dc689b28145c678a3d6a12a09ca03c115ce58f44e619cac119dc8925f -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self) -> 'Self'\""
  },
  "distribution": "a-stove0-media-archive-lib",
  "module": "a_stove0_media_archive_lib",
  "name": "verify_digest",
  "owner": "a_stove0_media_archive_lib.MediaArchiveProjection",
  "unit": "member"
}
```

</details>
