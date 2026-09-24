# a_stove0_media_archive_lib.MediaProjectedValue.bind_source

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:a-stove0-media-archive-lib:a-stove0-media-archive-lib-mediaprojected-34bbbbf411:980d2e5e4f -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [a-stove0-media-archive-lib](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-49468e372b"></a>
- <a id="s-b66aea89e2"></a>`distribution`: `a-stove0-media-archive-lib`
- <a id="s-c7a6c97156"></a>`module`: `a_stove0_media_archive_lib`
- <a id="s-dbae0de187"></a>`name`: `bind_source`
- <a id="s-5626ece562"></a>`owner`: `a_stove0_media_archive_lib.MediaProjectedValue`
- <a id="s-efd6701e8d"></a>`unit`: `member`

### Declared structure

- <a id="s-798ce18179"></a>`kind`: `"method"`
- <a id="s-25ef726081"></a>`signature`: `"\"(self) -> 'Self'\""`

## Maintained corroboration

### Related interface records

- [MediaProjectedValue](a-stove0-media-archive-lib-mediaprojectedvalue.md)

## Governing policies

- <a id="pa-6bde1e45f6"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:a-stove0-media-archive-lib:a_stove0_media_archive_lib](../../../evidence/sources/authorities.md#src-6929294187) — [some-implementations/stove0/targets/media-archive/support/src/a\_stove0\_media\_archive\_lib/\_\_init\_\_.py](../../../../../../some-implementations/stove0/targets/media-archive/support/src/a_stove0_media_archive_lib/__init__.py)

### Machine authority

- `/external_contract/python/a_stove0_media_archive_lib.MediaProjectedValue.bind_source`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 00e58888baa55e4573e11349348cbe61cb6a64b877d58de1578ded626f7bb53c -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self) -> 'Self'\""
  },
  "distribution": "a-stove0-media-archive-lib",
  "module": "a_stove0_media_archive_lib",
  "name": "bind_source",
  "owner": "a_stove0_media_archive_lib.MediaProjectedValue",
  "unit": "member"
}
```

</details>
