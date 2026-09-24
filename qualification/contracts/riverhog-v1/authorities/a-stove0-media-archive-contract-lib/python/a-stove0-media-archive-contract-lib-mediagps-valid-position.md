# a_stove0_media_archive_contract_lib.MediaGps.valid_position

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:a-stove0-media-archive-contract-lib:a-stove0-media-archive-contract-lib-media-d7257e7bef:808ab08ab4 -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [a-stove0-media-archive-contract-lib](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-9488d6c1d6"></a>
- <a id="s-aae15eadd6"></a>`distribution`: `a-stove0-media-archive-contract-lib`
- <a id="s-4f70fade03"></a>`module`: `a_stove0_media_archive_contract_lib`
- <a id="s-504be2bcbe"></a>`name`: `valid_position`
- <a id="s-74a5f78b00"></a>`owner`: `a_stove0_media_archive_contract_lib.MediaGps`
- <a id="s-a634c9531a"></a>`unit`: `member`

### Declared structure

- <a id="s-ca338daea0"></a>`kind`: `"method"`
- <a id="s-effa18768b"></a>`signature`: `"\"(self) -> 'Self'\""`

## Maintained corroboration

### Related interface records

- [MediaGps](a-stove0-media-archive-contract-lib-mediagps.md)

## Governing policies

- <a id="pa-5106784e9d"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:a-stove0-media-archive-contract-lib:a_stove0_media_archive_contract_lib](../../../evidence/sources/authorities.md#src-3f8c4376e0) — [some-implementations/stove0/targets/media-archive/contracts/src/a\_stove0\_media\_archive\_contract\_lib/\_\_init\_\_.py](../../../../../../some-implementations/stove0/targets/media-archive/contracts/src/a_stove0_media_archive_contract_lib/__init__.py)

### Machine authority

- `/external_contract/python/a_stove0_media_archive_contract_lib.MediaGps.valid_position`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: 5051f5d57b0d35000ce421b175dc3e27291cb289a8708fafd601ef23ee3cef9b -->

```json
{
  "contract": {
    "kind": "method",
    "signature": "\"(self) -> 'Self'\""
  },
  "distribution": "a-stove0-media-archive-contract-lib",
  "module": "a_stove0_media_archive_contract_lib",
  "name": "valid_position",
  "owner": "a_stove0_media_archive_contract_lib.MediaGps",
  "unit": "member"
}
```

</details>
