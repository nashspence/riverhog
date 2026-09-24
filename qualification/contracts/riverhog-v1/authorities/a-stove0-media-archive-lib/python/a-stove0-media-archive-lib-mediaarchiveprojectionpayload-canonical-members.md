# a_stove0_media_archive_lib.MediaArchiveProjectionPayload.canonical_members

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:a-stove0-media-archive-lib:a-stove0-media-archive-lib-mediaarchivepr-ae0552cd07:577dfab6dd -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [a-stove0-media-archive-lib](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-f57a60de6b"></a>
- <a id="s-88b6577e5f"></a>`distribution`: `a-stove0-media-archive-lib`
- <a id="s-a925946d34"></a>`module`: `a_stove0_media_archive_lib`
- <a id="s-9c44dae270"></a>`name`: `canonical_members`
- <a id="s-d298b3b36c"></a>`owner`: `a_stove0_media_archive_lib.MediaArchiveProjectionPayload`
- <a id="s-0a6b3a27e0"></a>`unit`: `member`

### Declared structure

- <a id="s-ae9ec7b1ad"></a>`kind`: `"method"`
- <a id="s-384d953797"></a>`signature`: `"\"(self) -> 'Self'\""`

## Maintained corroboration

### Related interface records

- [MediaArchiveProjectionPayload](a-stove0-media-archive-lib-mediaarchiveprojectionpayload.md)

## Governing policies

- <a id="pa-1c5bf3a725"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:a-stove0-media-archive-lib:a_stove0_media_archive_lib](../../../evidence/sources/authorities.md#src-6929294187) — [some-implementations/stove0/targets/media-archive/support/src/a\_stove0\_media\_archive\_lib/\_\_init\_\_.py](../../../../../../some-implementations/stove0/targets/media-archive/support/src/a_stove0_media_archive_lib/__init__.py)

### Machine authority

- `/external_contract/python/a_stove0_media_archive_lib.MediaArchiveProjectionPayload.canonical_members`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: a277c507a98e138afba13ffd41d9d2b18c15fdd4f950f16059c0db26b06f2348 -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self) -> 'Self'\""
  },
  "distribution": "a-stove0-media-archive-lib",
  "module": "a_stove0_media_archive_lib",
  "name": "canonical_members",
  "owner": "a_stove0_media_archive_lib.MediaArchiveProjectionPayload",
  "unit": "member"
}
```

</details>
