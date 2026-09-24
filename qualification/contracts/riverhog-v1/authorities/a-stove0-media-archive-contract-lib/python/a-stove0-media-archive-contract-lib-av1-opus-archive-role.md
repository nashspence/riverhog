# a_stove0_media_archive_contract_lib.AV1_OPUS_ARCHIVE_ROLE

[Atlas](../../../index.md) · [Authority](../index.md) · [Interface](index.md) · [Policies](../../../policies/index.md)

<!-- contract-element: python:a-stove0-media-archive-contract-lib:a-stove0-media-archive-contract-lib-av1-o-3022935d96:5a817bdf6b -->

Exact externally visible contract owned by this contract element.

| Audit field | Value |
|---|---|
| Authority | [a-stove0-media-archive-contract-lib](../index.md) |
| Interface | [Python](index.md) |

## External contract

<a id="s-b1efdef42c"></a>
- <a id="s-d0524cb0e9"></a>`distribution`: `a-stove0-media-archive-contract-lib`
- <a id="s-fee1fce9f4"></a>`module`: `a_stove0_media_archive_contract_lib`
- <a id="s-e6d8fe75c5"></a>`name`: `AV1_OPUS_ARCHIVE_ROLE`
- <a id="s-554a616aba"></a>`unit`: `export`

### Declared structure

- <a id="s-fa16c7154f"></a>`kind`: `"constant"`
- <a id="s-199bc6bbbd"></a>`value`: `"stove0.media.av1-opus-archive/v1"`

## Governing policies

- <a id="pa-7d532cd5ea"></a>[compatibility/python-api/v1](../../release/compatibility-guarantees/compatibility-python-api.md#p-e574772ba5)

## Evidence

### Qualification

- [make dist-smoke](../../../evidence/sources/commands.md#q-0ba2578a3e)
- [make build](../../../evidence/sources/commands.md#q-d1121e35fa)

### Executable sources

- [generator:contract-projection](../../../evidence/sources/authorities.md#src-47381a6c4f) — [scripts/contract\_freeze.py::contract\_projection](../../../../../../scripts/contract_freeze.py)
- [python:a-stove0-media-archive-contract-lib:a_stove0_media_archive_contract_lib](../../../evidence/sources/authorities.md#src-3f8c4376e0) — [some-implementations/stove0/targets/media-archive/contracts/src/a\_stove0\_media\_archive\_contract\_lib/\_\_init\_\_.py](../../../../../../some-implementations/stove0/targets/media-archive/contracts/src/a_stove0_media_archive_contract_lib/__init__.py)

### Machine authority

- `/external_contract/python/a_stove0_media_archive_contract_lib.AV1_OPUS_ARCHIVE_ROLE`

### Exact owned JSON

<details>
<summary>Expand exact machine-owned values</summary>

The following JSON is the complete value owned at each machine-authority pointer. No contractual fields are summarized away.

<!-- exact-contract-value: c4d43f89fedb64e142976914c99a0a678336f0dd031ed11ea7fa38089d01d306 -->

```json
{
  "contract": {
    "kind": "constant",
    "value": "stove0.media.av1-opus-archive/v1"
  },
  "distribution": "a-stove0-media-archive-contract-lib",
  "module": "a_stove0_media_archive_contract_lib",
  "name": "AV1_OPUS_ARCHIVE_ROLE",
  "unit": "export"
}
```

</details>
